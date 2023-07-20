#pragma once

#include "tick.h"

#include <cstddef>
#include <deque>
#include <optional>

class WorkerQueue {
  public:
    explicit WorkerQueue(std::size_t capacity) : capacity_(capacity) {
    }

    bool try_push(Tick tick) {
        if (queue_.size() == capacity_) {
            return false;
        }

        queue_.push_back(tick);
        return true;
    }

    std::optional<Tick> try_pop() {
        if (queue_.empty()) {
            return std::nullopt;
        }

        Tick tick = queue_.front();
        queue_.pop_front();
        return tick;
    }

  private:
    std::size_t capacity_ = 0;
    std::deque<Tick> queue_;
};
