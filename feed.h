#pragma once

#include "tick.h"

#include <chrono>
#include <cstdint>

class Feed {
  public:
    Tick next() {
        ++sequence_;

        const int instrument_id = static_cast<int>((sequence_ - 1) % 3) + 1;
        const double mid = 100.0 + static_cast<double>(instrument_id);

        return Tick{
            .instrument_id = instrument_id,
            .bid = mid - 0.01,
            .ask = mid + 0.01,
            .sequence = sequence_,
            .timestamp_ns = now_ns(),
        };
    }

  private:
    static std::uint64_t now_ns() {
        const auto now = std::chrono::steady_clock::now().time_since_epoch();
        return static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
    }

    std::uint64_t sequence_ = 0;
};
