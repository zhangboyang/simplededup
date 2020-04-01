#include "config.h"

#include "BitVector.h"

void BitVector::ensure(uint64_t n)
{
    uint64_t new_size = a.size();
    if (new_size == 0) new_size = 1;
    while (new_size < n) new_size *= 2;
    if (a.size() != new_size) a.resize(new_size);
}

bool BitVector::get(uint64_t idx)
{
    return a[idx];
}
void BitVector::set(uint64_t idx, bool value)
{
    a[idx] = value;
}
void BitVector::set(uint64_t idx_begin, uint64_t idx_end, bool value)
{
    while (idx_begin < idx_end) {
        set(idx_begin++, value);
    }
}