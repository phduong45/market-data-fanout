#include "feed.h"

#include <cassert>
#include <iostream>
#include <vector>

struct WorkerStats {
    std::uint64_t received = 0;
    std::uint64_t last_sequence = 0;
};

void deliver_tick(const Tick& tick, std::vector<WorkerStats>& workers) {
    for (auto& worker : workers) {
        ++worker.received;
        worker.last_sequence = tick.sequence;
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
    std::vector<WorkerStats> workers(kWorkers);

    for (int i = 0; i < kTicks; ++i) {
        deliver_tick(feed.next(), workers);
    }

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
