#include "feed.h"
#include "spsc_queue.h"

#include <algorithm>
#include <cassert>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

struct WorkerStats {
    std::uint64_t received = 0;
    std::uint64_t last_sequence = 0;
    std::uint64_t total_latency_ns = 0;
    std::uint64_t max_latency_ns = 0;
};

struct FanoutStats {
    std::uint64_t published = 0;
    std::uint64_t dropped = 0;
};

std::uint64_t now_ns() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

template <std::size_t Capacity>
using WorkerQueue = spsc::SpscQueue<Tick, Capacity>;

template <std::size_t Capacity>
using WorkerQueues = std::vector<std::unique_ptr<WorkerQueue<Capacity>>>;

template <std::size_t Capacity>
WorkerQueues<Capacity> make_worker_queues(int workers) {
    WorkerQueues<Capacity> queues;
    queues.reserve(workers);

    for (int i = 0; i < workers; ++i) {
        queues.push_back(std::make_unique<WorkerQueue<Capacity>>());
    }

    return queues;
}

template <std::size_t Capacity>
void publish_tick(const Tick& tick, WorkerQueues<Capacity>& queues,
                  FanoutStats& stats) {
    ++stats.published;

    for (auto& queue : queues) {
        if (!queue->try_push(tick)) {
            ++stats.dropped;
        }
    }
}

template <std::size_t Capacity>
void drain_worker(WorkerQueue<Capacity>& queue, WorkerStats& stats) {
    while (auto tick = queue.try_pop()) {
        const std::uint64_t latency = now_ns() - tick->timestamp_ns;

        ++stats.received;
        stats.last_sequence = tick->sequence;
        stats.total_latency_ns += latency;
        stats.max_latency_ns = std::max(stats.max_latency_ns, latency);
    }
}

double average_latency_ns(const WorkerStats& stats) {
    if (stats.received == 0) {
        return 0.0;
    }

    return static_cast<double>(stats.total_latency_ns) /
           static_cast<double>(stats.received);
}

void test_feed() {
    Feed feed;

    const Tick first = feed.next();
    assert(first.instrument_id == 1);
    assert(first.bid == 100.99);
    assert(first.ask == 101.01);
    assert(first.sequence == 1);
    assert(first.timestamp_ns > 0);

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
    auto queues = make_worker_queues<16>(kWorkers);
    std::vector<WorkerStats> workers(kWorkers);

    for (int i = 0; i < kTicks; ++i) {
        publish_tick(feed.next(), queues, fanout_stats);
    }

    for (int i = 0; i < kWorkers; ++i) {
        drain_worker(*queues[i], workers[i]);
    }

    assert(fanout_stats.published == kTicks);
    assert(fanout_stats.dropped == 0);

    for (const auto& worker : workers) {
        assert(worker.received == kTicks);
        assert(worker.last_sequence == kTicks);
        assert(worker.max_latency_ns > 0);
        assert(average_latency_ns(worker) > 0.0);
    }

    std::cout << "basic fanout ok\n";
}

void test_drop_when_worker_queue_is_full() {
    constexpr int kWorkers = 2;
    constexpr int kTicks = 5;
    constexpr int kQueueCapacity = 2;

    Feed feed;
    FanoutStats fanout_stats;
    auto queues = make_worker_queues<kQueueCapacity>(kWorkers);
    std::vector<WorkerStats> workers(kWorkers);

    for (int i = 0; i < kTicks; ++i) {
        publish_tick(feed.next(), queues, fanout_stats);
    }

    for (int i = 0; i < kWorkers; ++i) {
        drain_worker(*queues[i], workers[i]);
    }

    assert(fanout_stats.published == kTicks);
    assert(fanout_stats.dropped == (kTicks - kQueueCapacity) * kWorkers);

    for (const auto& worker : workers) {
        assert(worker.received == kQueueCapacity);
        assert(worker.last_sequence == kQueueCapacity);
        assert(worker.max_latency_ns > 0);
    }

    std::cout << "drop policy ok\n";
}

void test_threaded_fanout() {
    constexpr int kWorkers = 3;
    constexpr int kTicks = 1000;

    Feed feed;
    FanoutStats fanout_stats;
    auto queues = make_worker_queues<1024>(kWorkers);
    std::vector<WorkerStats> workers(kWorkers);
    std::atomic<bool> done = false;

    std::vector<std::thread> worker_threads;
    worker_threads.reserve(kWorkers);

    for (int i = 0; i < kWorkers; ++i) {
        worker_threads.emplace_back([&, i] {
            while (!done.load(std::memory_order_acquire)) {
                drain_worker(*queues[i], workers[i]);
            }

            drain_worker(*queues[i], workers[i]);
        });
    }

    std::thread feed_thread([&] {
        for (int i = 0; i < kTicks; ++i) {
            publish_tick(feed.next(), queues, fanout_stats);
        }

        done.store(true, std::memory_order_release);
    });

    feed_thread.join();

    for (auto& worker_thread : worker_threads) {
        worker_thread.join();
    }

    assert(fanout_stats.published == kTicks);
    assert(fanout_stats.dropped == 0);

    for (const auto& worker : workers) {
        assert(worker.received == kTicks);
        assert(worker.last_sequence == kTicks);
        assert(worker.max_latency_ns > 0);
        assert(average_latency_ns(worker) > 0.0);
    }

    std::cout << "threaded fanout ok\n";
}

int main() {
    test_feed();
    test_basic_fanout();
    test_drop_when_worker_queue_is_full();
    test_threaded_fanout();
}
