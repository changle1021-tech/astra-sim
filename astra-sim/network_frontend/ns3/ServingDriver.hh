#pragma once

#include "astra-sim/common/Logging.hh"
#include "astra-sim/system/Sys.hh"
#include "ns3/core-module.h"
#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

namespace AstraSim {

// The same frontend contract as the analytical serving driver. End ranks
// report before controllers; a controller assigns its TP/PP follower ranks.
inline void run_ns3_serving(std::vector<Sys*>& systems,
                            const std::vector<int>& starts,
                            const std::vector<int>& ends,
                            long long host_dispatch_latency_ns = 0) {
    const int count = systems.size();
    std::vector<std::vector<Sys*>> managed(starts.size());
    for (std::size_t i = 0; i < starts.size(); ++i) {
        const int upper = i + 1 < starts.size() ? starts[i + 1] : count;
        for (int rank = starts[i] + 1; rank < upper; ++rank) {
            if (std::find(ends.begin(), ends.end(), rank) == ends.end()) {
                managed[i].push_back(systems[rank]);
            }
        }
    }
    std::vector<std::vector<Sys*>> completion_groups(ends.size());
    for (std::size_t i = 0; i < ends.size(); ++i) {
        const int rank = ends[i];
        const auto next_start = std::upper_bound(starts.begin(), starts.end(), rank);
        if (next_start == starts.begin()) {
            throw std::invalid_argument("Serving completion rank has no matching controller");
        }
        const int first = *(next_start - 1);
        const int upper = next_start == starts.end() ? count : *next_start;
        if (rank < first || rank >= upper) {
            throw std::invalid_argument("Serving completion rank is outside its instance");
        }
        for (int peer = first; peer < upper; ++peer) {
            completion_groups[i].push_back(systems[peer]);
        }
    }
    // A managed rank in a later iteration has completed the target even if
    // it is still busy. Preserve PP lookahead while waiting for slower peers.
    auto completed_iteration = [](const std::vector<Sys*>& group, uint32_t target) {
        return std::all_of(group.begin(), group.end(), [target](const Sys* system) {
            const auto* workload = system->workload;
            return workload->iteration > target ||
                   (workload->iteration == target && workload->is_finished);
        });
    };
    std::vector<long long> passed(count, -1), deadlines(count, 0), reported(count, -1);
    long long generation = 0;
    int quiet_rounds = 0;
    bool exiting = false;
    struct DispatchWindow { long long ready; int expected; int assigned; };
    std::map<std::string, DispatchWindow> dispatch_windows;
    auto clear = [&]() { std::fill(passed.begin(), passed.end(), -1); };
    auto ask = [&](int rank, const std::vector<Sys*>& followers) {
        auto* workload = systems[rank]->workload;
        const auto now = ns3::Simulator::Now().GetNanoSeconds();
        const bool eligible = passed[rank] < 0 || passed[rank] != generation ||
                              (deadlines[rank] > 0 && now >= deadlines[rank]);
        if (workload->is_sleep || !workload->is_finished || !eligible) return false;
        if (static_cast<long long>(workload->iteration) != reported[rank]) {
            reported[rank] = workload->iteration;
            ++generation;
        }
        workload->report();
        LoggerFactory::get_logger("workload")->info("Waiting");
        std::string reply;
        if (!std::getline(std::cin, reply)) {
            throw std::runtime_error("Serving frontend closed its input pipe");
        }
        if (reply == "pass" || reply.rfind("pass ", 0) == 0) {
            const auto deadline = reply.size() > 5 ? std::stoll(reply.substr(5)) : 0;
            if (deadline < 0) ++generation;
            passed[rank] = generation;
            deadlines[rank] = std::max(0LL, deadline);
            if (deadline > now) {
                // RDMA maintenance events must not skip a known arrival.
                ns3::Simulator::Schedule(ns3::NanoSeconds(deadline - now), [] {});
            }
        } else {
            ++generation;
            passed[rank] = -1;
            if (reply == "exit") exiting = true;
            else if (reply == "done") workload->is_sleep = true;
            else if (host_dispatch_latency_ns == 0) workload->add_workload(reply, followers);
            else {
                auto it = dispatch_windows.find(reply);
                if (it == dispatch_windows.end()) {
                    if (host_dispatch_latency_ns > std::numeric_limits<long long>::max() - now) {
                        throw std::overflow_error("Host dispatch exceeds ns-3 clock range");
                    }
                    int expected = 0;
                    for (int peer = 0; peer < count; ++peer) {
                        if (std::ifstream(reply + "." + std::to_string(peer) + ".et").good()) ++expected;
                    }
                    if (expected == 0) throw std::runtime_error("No rank graphs for host dispatch");
                    it = dispatch_windows.emplace(reply, DispatchWindow{
                        now + host_dispatch_latency_ns, expected, 0}).first;
                }
                const auto delay = std::max(0LL, it->second.ready - now);
                it->second.assigned += 1 + followers.size();
                if (it->second.assigned > it->second.expected) {
                    throw std::runtime_error("Duplicate rank assignment during host dispatch");
                }
                if (it->second.assigned == it->second.expected) dispatch_windows.erase(it);
                if (delay == 0) workload->add_workload(reply, followers);
                else {
                    // Shared rank files describe one forward: charge its host
                    // handoff once, even when the completion rank is polled later.
                    workload->is_sleep = true;
                    workload->is_finished = false;
                    ns3::Simulator::Schedule(ns3::NanoSeconds(delay), [workload, reply, followers] {
                        workload->is_sleep = false;
                        workload->is_finished = true;
                        workload->add_workload(reply, followers);
                    });
                }
            }
        }
        return true;
    };
    ns3::Simulator::PreRun();
    while (!exiting) {
        if (ns3::Simulator::IsFinished()) {
            ns3::Simulator::Schedule(ns3::NanoSeconds(1000000), [] {});
            clear();
        }
        ns3::Simulator::RunOneEvent();
        bool asked = false;
        for (std::size_t i = 0; i < ends.size(); ++i) {
            const int rank = ends[i];
            const auto* workload = systems[rank]->workload;
            if (!workload->is_sleep && workload->is_finished &&
                completed_iteration(completion_groups[i], workload->iteration)) {
                asked = ask(rank, {}) || asked;
            }
            if (exiting) break;
        }
        if (exiting) break;
        for (std::size_t i = 0; i < starts.size(); ++i) {
            asked = ask(starts[i], managed[i]) || asked;
            if (exiting) break;
        }
        if (asked) quiet_rounds = 0;
        else if (++quiet_rounds >= 1000) { quiet_rounds = 0; clear(); }
    }
}

inline std::vector<int> parse_serving_ranks(const std::string& text, int count) {
    std::vector<int> result;
    std::size_t start = 0;
    while (start < text.size()) {
        const auto end = text.find(',', start);
        const auto rank = std::stoi(text.substr(start, end - start));
        if (rank < 0 || rank >= count ||
            std::find(result.begin(), result.end(), rank) != result.end()) {
            throw std::invalid_argument("Invalid or duplicated serving NPU rank");
        }
        result.push_back(rank);
        if (end == std::string::npos) break;
        start = end + 1;
    }
    if (!std::is_sorted(result.begin(), result.end())) {
        throw std::invalid_argument("Serving controller ranks must be sorted");
    }
    return result;
}

}  // namespace AstraSim
