#include "config.h"

#include "IntWriter.h"
#include <cstdio>
#include <cassert>

IntWriter::IntWriter(const std::string &file_name)
{
    fp = fopen(file_name.c_str(), "wb");
    assert(fp);
}
IntWriter::~IntWriter()
{
    fclose(fp);
}

void IntWriter::rewind()
{
    ::rewind(fp);
}
void IntWriter::flush()
{
    fflush(fp);
}

void IntWriter::writeByte(uint8_t value)
{
    fputc(value, fp);
}

void IntWriter::writeInt(uint64_t value)
{
    fwrite(&value, sizeof(value), 1, fp);
}

void IntWriter::writeZippedInt(uint64_t value)
{
    if ((value & 0x7fULL) == value) {
        writeByte(value);
    } else if ((value & 0x3fffULL) == value) {
        writeByte((value >> 8) | 0x80);
        writeByte(value);
    } else if ((value & 0x1fffffULL) == value) {
        writeByte((value >> 16) | 0xc0);
        writeByte(value >> 8);
        writeByte(value);
    } else if ((value & 0xfffffffULL) == value) {
        writeByte((value >> 24) | 0xe0);
        writeByte(value >> 16);
        writeByte(value >> 8);
        writeByte(value);
    } else if ((value & 0x7ffffffffULL) == value) {
        writeByte((value >> 32) | 0xf0);
        writeByte(value >> 24);
        writeByte(value >> 16);
        writeByte(value >> 8);
        writeByte(value);
    } else if ((value & 0x3ffffffffffULL) == value) {
        writeByte((value >> 40) | 0xf8);
        writeByte(value >> 32);
        writeByte(value >> 24);
        writeByte(value >> 16);
        writeByte(value >> 8);
        writeByte(value);
    } else if ((value & 0x1ffffffffffffULL) == value) {
        writeByte((value >> 48) | 0xfc);
        writeByte(value >> 40);
        writeByte(value >> 32);
        writeByte(value >> 24);
        writeByte(value >> 16);
        writeByte(value >> 8);
        writeByte(value);
    } else if ((value & 0xffffffffffffffULL) == value) {
        writeByte(0xfe);
        writeByte(value >> 48);
        writeByte(value >> 40);
        writeByte(value >> 32);
        writeByte(value >> 24);
        writeByte(value >> 16);
        writeByte(value >> 8);
        writeByte(value);
    } else {
        writeByte(0xff);
        writeInt(value);
    }
}