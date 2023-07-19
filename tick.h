#pragma once

#include <cstdint>

struct Tick {
    int instrument_id = 0;
    double bid = 0.0;
    double ask = 0.0;
    std::uint64_t sequence = 0;
    std::uint64_t timestamp_ns = 0;
};
