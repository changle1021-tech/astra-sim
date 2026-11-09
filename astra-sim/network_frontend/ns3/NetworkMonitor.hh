#pragma once

#include "ns3/channel.h"
#include "ns3/node-container.h"
#include "ns3/qbb-net-device.h"
#include "ns3/rdma-driver.h"
#include "ns3/simulator.h"
#include <json/json.hpp>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace AstraSim {

// Observe existing events rather than scheduling ticks that advance an idle
// serving simulation. Packet bytes are charged at transmission start and at
// successful MAC reception, including packet headers and control packets.
class Ns3NetworkMonitor {
  public:
    Ns3NetworkMonitor(const std::string& configuration, const ns3::NodeContainer& nodes) {
        std::ifstream input(configuration);
        if (!input) throw std::runtime_error("Cannot read network monitor configuration");
        std::map<std::string, std::string> settings;
        std::string line;
        while (std::getline(input, line)) {
            std::istringstream row(line);
            std::string key, value;
            if (row >> key >> value) settings[key] = value;
        }
        if (settings["ENABLE_NETWORK_MONITOR"] != "1") return;
        interval_ = PositiveInterval(settings, "BW_MON_INTERVAL_NS");
        qp_interval_ = PositiveInterval(settings, "QP_MON_INTERVAL_NS");
        summary_path_ = settings.at("NETWORK_MONITOR_SUMMARY_FILE");
        Open(bandwidth_, settings.at("BW_MON_FILE"));
        Open(rates_, settings.at("RATE_MON_FILE"));
        bandwidth_ << "time_ns,node_id,port_id,peer_node_id,link_bandwidth_bps,interval_ns,tx_bytes,rx_bytes,tx_gbps,rx_gbps\n";
        rates_ << "time_ns,src_node_id,dst_node_id,src_port,dst_port,size_bytes,rate_bps\n";
        bandwidth_ << std::setprecision(12);
        for (uint32_t i = 0; i < nodes.GetN(); ++i) {
            auto node = nodes.Get(i);
            for (uint32_t j = 0; j < node->GetNDevices(); ++j) {
                auto device = ns3::DynamicCast<ns3::QbbNetDevice>(node->GetDevice(j));
                if (!device) continue;
                auto channel = device->GetChannel();
                uint32_t peer = node->GetId();
                for (std::size_t k = 0; k < channel->GetNDevices(); ++k) {
                    auto other = channel->GetDevice(k);
                    if (other != device) peer = other->GetNode()->GetId();
                }
                const auto index = ports_.size();
                ports_.push_back({node->GetId(), device->GetIfIndex(), peer,
                                  device->GetDataRate().GetBitRate(), 0, 0, 0, 0});
                if (!device->TraceConnectWithoutContext("PhyTxBegin", ns3::MakeBoundCallback(&CountTx, this, index)) ||
                    !device->TraceConnectWithoutContext("MacRx", ns3::MakeBoundCallback(&CountRx, this, index)) ||
                    !device->TraceConnectWithoutContext("RdmaQpDequeue", ns3::MakeCallback(&Ns3NetworkMonitor::OnQpPacket, this))) {
                    throw std::runtime_error("Cannot attach ns-3 network monitor traces");
                }
            }
            auto driver = node->GetObject<ns3::RdmaDriver>();
            if (driver && !driver->TraceConnectWithoutContext("QpComplete", ns3::MakeCallback(&Ns3NetworkMonitor::OnQpComplete, this))) {
                throw std::runtime_error("Cannot attach ns-3 QP completion monitor");
            }
        }
        enabled_ = true;
        WritePorts(0, 0);
        next_port_ = interval_;
        next_qp_ = qp_interval_;
    }

    void Sample() {
        if (!enabled_) return;
        const auto now = Now();
        AdvancePorts(now);
        if (now >= next_qp_) {
            for (auto& item : active_rates_) {
                const auto rate = item.first->m_rate.GetBitRate();
                if (rate != item.second) {
                    WriteRate(item.first, rate);
                    item.second = rate;
                }
            }
            next_qp_ = now + qp_interval_;
        }
    }

    void Finish() {
        if (!enabled_) return;
        Sample();
        const auto now = Now();
        if (now > last_port_) WritePorts(now, now - last_port_);
        bandwidth_.flush();
        rates_.flush();
        uint64_t total_tx = 0, total_rx = 0;
        for (const auto& port : ports_) { total_tx += port.tx; total_rx += port.rx; }
        nlohmann::json summary = {
            {"time_end_ns", now}, {"port_count", ports_.size()},
            {"bandwidth_interval_ns", interval_}, {"qp_interval_ns", qp_interval_},
            {"bandwidth_rows", bandwidth_rows_}, {"qp_rate_rows", rate_rows_},
            {"qp_started", qp_started_}, {"qp_completed", qp_completed_},
            {"qp_active_at_end", active_rates_.size()},
            {"port_tx_bytes", total_tx}, {"port_rx_bytes", total_rx},
            {"byte_accounting", "packet bytes at PHY transmission start and successful MAC reception; includes headers and control packets"}
        };
        std::ofstream output;
        Open(output, summary_path_);
        output << summary.dump(2) << '\n';
        output.flush();
    }

  private:
    struct Port {
        uint32_t node, interface, peer;
        uint64_t link_bps, tx, rx, last_tx, last_rx;
    };
    bool enabled_ = false;
    uint64_t interval_ = 0, qp_interval_ = 0, next_port_ = 0, next_qp_ = 0, last_port_ = 0;
    uint64_t bandwidth_rows_ = 0, rate_rows_ = 0, qp_started_ = 0, qp_completed_ = 0;
    std::string summary_path_;
    std::ofstream bandwidth_, rates_;
    std::vector<Port> ports_;
    std::map<ns3::Ptr<ns3::RdmaQueuePair>, uint64_t> active_rates_;

    static uint64_t Now() { return ns3::Simulator::Now().GetNanoSeconds(); }
    static uint64_t PositiveInterval(const std::map<std::string, std::string>& settings, const std::string& key) {
        const auto& text = settings.at(key);
        if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
            throw std::invalid_argument(key + " must be a positive integer in ns");
        const auto value = std::stoull(text);
        if (!value || value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) / 2)
            throw std::invalid_argument(key + " is outside the supported range");
        return value;
    }
    static void Open(std::ofstream& stream, const std::string& path) {
        stream.exceptions(std::ios::badbit | std::ios::failbit);
        stream.open(path);
    }
    static void CountTx(Ns3NetworkMonitor* monitor, std::size_t index, ns3::Ptr<const ns3::Packet> packet) {
        monitor->AdvancePorts(Now());
        monitor->ports_[index].tx += packet->GetSize();
    }
    static void CountRx(Ns3NetworkMonitor* monitor, std::size_t index, ns3::Ptr<const ns3::Packet> packet) {
        monitor->AdvancePorts(Now());
        monitor->ports_[index].rx += packet->GetSize();
    }
    void AdvancePorts(uint64_t now) {
        while (next_port_ && now >= next_port_) {
            WritePorts(next_port_, interval_);
            next_port_ += interval_;
        }
    }
    void WritePorts(uint64_t time, uint64_t duration) {
        for (auto& port : ports_) {
            const auto tx = port.tx - port.last_tx, rx = port.rx - port.last_rx;
            bandwidth_ << time << ',' << port.node << ',' << port.interface << ',' << port.peer << ','
                       << port.link_bps << ',' << duration << ',' << tx << ',' << rx << ','
                       << (duration ? tx * 8.0 / duration : 0.0) << ','
                       << (duration ? rx * 8.0 / duration : 0.0) << '\n';
            port.last_tx = port.tx;
            port.last_rx = port.rx;
            ++bandwidth_rows_;
        }
        last_port_ = time;
    }
    void WriteRate(ns3::Ptr<ns3::RdmaQueuePair> qp, uint64_t rate) {
        rates_ << Now() << ',' << qp->m_src << ',' << qp->m_dest << ',' << qp->sport << ','
               << qp->dport << ',' << qp->m_size << ',' << rate << '\n';
        ++rate_rows_;
    }
    void OnQpPacket(ns3::Ptr<const ns3::Packet>, ns3::Ptr<ns3::RdmaQueuePair> qp) {
        if (active_rates_.count(qp)) return;
        const auto rate = qp->m_rate.GetBitRate();
        active_rates_[qp] = rate;
        ++qp_started_;
        WriteRate(qp, rate);
    }
    void OnQpComplete(ns3::Ptr<ns3::RdmaQueuePair> qp) {
        auto found = active_rates_.find(qp);
        if (found == active_rates_.end()) return;
        if (found->second != qp->m_rate.GetBitRate()) WriteRate(qp, qp->m_rate.GetBitRate());
        // Zero is an observed completion marker, not a congestion-control rate.
        WriteRate(qp, 0);
        active_rates_.erase(found);
        ++qp_completed_;
    }
};

} // namespace AstraSim
