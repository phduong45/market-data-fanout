#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <new>
#include <optional>
#include <utility>

namespace spsc {
inline constexpr std::size_t kCacheLineSize = 64;

// Exactly one producer and one consumer.
template <typename T, std::size_t Capacity>
class SpscQueue {
    static constexpr bool is_power_of_two(std::size_t value) {
        return value > 0 && (value & (value - 1)) == 0;
    }

    static_assert(is_power_of_two(Capacity),
                  "SpscQueue capacity must be a power of two");

  public:
    SpscQueue() = default;

    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    SpscQueue(SpscQueue&&) = delete;
    SpscQueue& operator=(SpscQueue&&) = delete;

    ~SpscQueue() {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t tail = tail_.load(std::memory_order_relaxed);

        while (tail != head) {
            slot(tail)->~T();
            ++tail;
        }
    }

    static constexpr std::size_t capacity() {
        return Capacity;
    }

    template <typename... Args>
    bool try_emplace(Args&&... args) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t tail = tail_.load(std::memory_order_acquire);

        if (head - tail == Capacity) {
            return false;
        }

        ::new (static_cast<void*>(slot(head))) T(std::forward<Args>(args)...);
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    bool try_push(T value) {
        return try_emplace(std::move(value));
    }

    std::optional<T> try_pop() {
        const std::size_t head = head_.load(std::memory_order_acquire);
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        if (head == tail) {
            return std::nullopt;
        }

        T* ptr = slot(tail);
        T value = std::move(*ptr);
        ptr->~T();

        tail_.store(tail + 1, std::memory_order_release);
        return value;
    }

  private:
    std::size_t index(std::size_t counter) const {
        return counter & (Capacity - 1);
    }

    T* slot(std::size_t counter) {
        auto offset = index(counter) * sizeof(T);
        return std::launder(reinterpret_cast<T*>(storage_.data() + offset));
    }

    alignas(T) std::array<std::byte, sizeof(T) * Capacity> storage_{};
    // Producer owns head_, consumer owns tail_.
    alignas(kCacheLineSize) std::atomic<std::size_t> head_{0};
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_{0};
};
} // namespace spsc
