#pragma once

class BitVector {
    std::vector<bool> a;

public:
    void ensure(uint64_t n); // make the vector at least n bits

    bool get(uint64_t idx);
    void set(uint64_t idx, bool value); // assign a single bit
};