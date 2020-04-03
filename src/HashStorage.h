#pragma once

#include <cstdio>
#include <cstdint>
#include <cinttypes>
#include <functional>
#include <vector>
#include <string>
#include <memory>

#include "IntWriter.h"
#include "IntReader.h"

struct HashRecord {
    union {
        uint64_t hash_value;
        uint64_t group_leader;
    };
    uint64_t physical_id;
    uint64_t logical_id;

    void dump() const
    {
        printf("%016" PRIX64 " %016" PRIX64 " %016" PRIX64 "\n", hash_value, physical_id, logical_id);
    }
};

class HashStorage {
    std::vector<HashRecord> record_buffer;

    int n_stor = 0;
    std::vector<std::string> stor_name;
    std::vector<std::unique_ptr<IntWriter>> stor_writer;
    std::vector<std::unique_ptr<IntReader>> stor_reader;
    std::vector<uint64_t> stor_used_bytes;


    std::string makeFileName(int stor_id);

    
    void writeRecord(std::unique_ptr<IntWriter> &writer, const HashRecord &record);
    bool readRecord(std::unique_ptr<IntReader> &reader, HashRecord &record);

    void sortBuffer();
    void flushWriteBuffer();

    void iterateSortedRecordInternal(bool file_sorted, std::function<void()> begin_callback, std::function<void(int, HashRecord &)> record_callback);

public:
    
    uint64_t file_cap = 1 * 1048576 / sizeof(HashRecord); // max records in a single file
    std::function<bool(const HashRecord &, const HashRecord &)> comparator;

    ~HashStorage();
    
    void emitRecord(const HashRecord &new_record);
    void finishEmitRecord();

    void iterateSortedRecord(bool file_sorted, std::function<void(const HashRecord &)> iter_callback);

    void iterateSortedRecordAndModifyHashInplace(bool file_sorted, std::function<void(HashRecord &)> iter_callback, std::function<void()> flush_callback);
    std::function<void(const HashRecord &new_record)> writeRecordInplace;
};
