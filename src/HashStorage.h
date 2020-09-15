#pragma once

#include "IntWriter.h"
#include "IntReader.h"

struct HashRecord {
    union {
        uint64_t hash_value;
        uint64_t group_id;
    };
    uint64_t logical_id;

    void dump() const
    {
        LOG("%016" PRIX64 " %016" PRIX64 "\n", hash_value, logical_id);
    }
};

class HashStorage {
    uint64_t buffer_cap; // max records in a single file

    std::vector<HashRecord> record_buffer;

    int n_stor = 0;
    std::vector<std::string> stor_name;
    std::vector<std::unique_ptr<IntWriter>> stor_writer;
    std::vector<std::unique_ptr<IntReader>> stor_reader;
    std::vector<uint64_t> stor_used_bytes;


    std::string makeFileName(int stor_id);

    
    void writeRecord(std::unique_ptr<IntWriter> &writer, const HashRecord &record);
    bool readRecord(std::unique_ptr<IntReader> &reader, HashRecord &record);

    void reserveBuffer();
    void discardBuffer();
    void sortBuffer();
    void flushWriteBuffer();

    void iterateSortedRecordInternal(bool file_sorted, std::function<void()> begin_callback, std::function<void(int, HashRecord &)> record_callback);

public:
    ~HashStorage();
    
    uint64_t sort_mem = 600;
    std::string stor_path = "hashstorage";
    
    std::function<bool(const HashRecord &, const HashRecord &)> comparator;

    
    void beginEmitRecord();
    void emitRecord(const HashRecord &new_record);
    void finishEmitRecord();

    void iterateSortedRecord(bool file_sorted, std::function<void(const HashRecord &)> iter_callback);

    void iterateSortedRecordAndModifyHashInplace(bool file_sorted, std::function<void(HashRecord &)> iter_callback, std::function<void()> flush_callback);
    std::function<void(const HashRecord &new_record)> writeRecordInplace;
};
