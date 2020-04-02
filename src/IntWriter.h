#pragma once

#include <string>

class IntWriter {
    FILE *fp;
public:
    IntWriter(const std::string &file_name);
    ~IntWriter();
    IntWriter(const IntWriter &) = delete;
    IntWriter& operator= (const IntWriter &) = delete;

    void rewind();
    void flush();
    uint64_t tell();
    void writeByte(uint8_t value);
    void writeInt(uint64_t value);
    void writeZippedInt(uint64_t value);
};
