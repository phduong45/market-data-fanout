#pragma once

#include "tick.h"

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
            .timestamp_ns = sequence_ * 1000,
        };
    }

  private:
    std::uint64_t sequence_ = 0;
};
