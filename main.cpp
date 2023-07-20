#include "feed.h"
#include "worker_queue.h"

#include <cassert>
#include <iostream>
#include <vector>

struct WorkerStats {
    std::uint64_t received = 0;
    std::uint64_t last_sequence = 0;
};

struct FanoutStats {
    std::uint64_t published = 0;
    std::uint64_t dropped = 0;
};

void publish_tick(const Tick& tick, std::vector<WorkerQueue>& queues,
                  FanoutStats& stats) {
    ++stats.published;

    for (auto& queue : queues) {
        if (!queue.try_push(tick)) {
            ++stats.dropped;
        }
    }
}

void drain_worker(WorkerQueue& queue, WorkerStats& stats) {
    while (auto tick = queue.try_pop()) {
        ++stats.received;
        stats.last_sequence = tick->sequence;
    }
}

void test_feed() {
    Feed feed;

    const Tick first = feed.next();
    assert(first.instrument_id == 1);
    assert(first.bid == 100.99);
    assert(first.ask == 101.01);
    assert(first.sequence == 1);

    const Tick second = feed.next();
    assert(second.instrument_id == 2);
    assert(second.sequence == 2);

    std::cout << "feed ok\n";
}

void test_basic_fanout() {
    constexpr int kWorkers = 3;
    constexpr int kTicks = 10;

    Feed feed;
    FanoutStats fanout_stats;
    std::vector<WorkerQueue> queues;
    std::vector<WorkerStats> workers(kWorkers);

    for (int i = 0; i < kWorkers; ++i) {
        queues.emplace_back(kTicks);
    }

    for (int i = 0; i < kTicks; ++i) {
        publish_tick(feed.next(), queues, fanout_stats);
    }

    for (int i = 0; i < kWorkers; ++i) {
        drain_worker(queues[i], workers[i]);
    }

    assert(fanout_stats.published == kTicks);
    assert(fanout_stats.dropped == 0);

    for (const auto& worker : workers) {
        assert(worker.received == kTicks);
        assert(worker.last_sequence == kTicks);
    }

    std::cout << "basic fanout ok\n";
}

int main() {
    test_feed();
    test_basic_fanout();
}
