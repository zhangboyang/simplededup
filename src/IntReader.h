#pragma once

#include <string>

class IntReader {
    FILE *fp;
public:
    IntReader(const std::string &file_name);
    ~IntReader();
    IntReader(const IntReader &) = delete;
    IntReader& operator= (const IntReader &) = delete;

    void rewind();
    void flush();
    bool eofOccured();
    uint8_t readByte();
    uint64_t readInt();
    uint64_t readZippedInt();
};
