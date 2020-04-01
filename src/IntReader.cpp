#include "config.h"

#include "IntReader.h"
#include <cassert>

IntReader::IntReader(const std::string &file_name)
{
    fp = fopen(file_name.c_str(), "rb");
    assert(fp);
}
IntReader::~IntReader()
{
    fclose(fp);
}

void IntReader::rewind()
{
    ::rewind(fp);
}
void IntReader::flush()
{
    fflush(fp);
}
bool IntReader::eofOccured()
{
    return feof(fp);
}
uint8_t IntReader::readByte()
{
    return fgetc(fp);
}
uint64_t IntReader::readInt()
{
    uint64_t value;
    fread(&value, sizeof(value), 1, fp);
    return value;
}
uint64_t IntReader::readZippedInt()
{
    uint64_t value;
    uint64_t firstbyte = readByte();
    if (firstbyte < 0x80) {
        value = firstbyte;
    } else if (firstbyte < 0xc0) {
        value = (firstbyte & 0x3f) << 8; 
        value |= ((uint64_t) readByte());
    } else if (firstbyte < 0xe0) {
        value = (firstbyte & 0x1f) << 16; 
        value |= ((uint64_t) readByte()) << 8;
        value |= ((uint64_t) readByte());
    } else if (firstbyte < 0xf0) {
        value = (firstbyte & 0x0f) << 24; 
        value |= ((uint64_t) readByte()) << 16;
        value |= ((uint64_t) readByte()) << 8;
        value |= ((uint64_t) readByte());
    } else if (firstbyte < 0xf8) {
        value = (firstbyte & 0x07) << 32; 
        value |= ((uint64_t) readByte()) << 24;
        value |= ((uint64_t) readByte()) << 16;
        value |= ((uint64_t) readByte()) << 8;
        value |= ((uint64_t) readByte());
    } else if (firstbyte < 0xfc) {
        value = (firstbyte & 0x03) << 40; 
        value |= ((uint64_t) readByte()) << 32;
        value |= ((uint64_t) readByte()) << 24;
        value |= ((uint64_t) readByte()) << 16;
        value |= ((uint64_t) readByte()) << 8;
        value |= ((uint64_t) readByte());
    } else if (firstbyte < 0xfe) {
        value = (firstbyte & 0x01) << 48; 
        value |= ((uint64_t) readByte()) << 40;
        value |= ((uint64_t) readByte()) << 32;
        value |= ((uint64_t) readByte()) << 24;
        value |= ((uint64_t) readByte()) << 16;
        value |= ((uint64_t) readByte()) << 8;
        value |= ((uint64_t) readByte());
    } else if (firstbyte < 0xff) {
        value = ((uint64_t) readByte()) << 48;
        value |= ((uint64_t) readByte()) << 40;
        value |= ((uint64_t) readByte()) << 32;
        value |= ((uint64_t) readByte()) << 24;
        value |= ((uint64_t) readByte()) << 16;
        value |= ((uint64_t) readByte()) << 8;
        value |= ((uint64_t) readByte());
    } else {
        value = readInt();
    }
    return value;
}
