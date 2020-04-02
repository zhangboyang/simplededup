#include "config.h"

#include <cstdio>
#include <algorithm>
#include <queue>

#include "HashStorage.h"

void HashStorage::emitRecord(const HashRecord &new_record)
{
    record_buffer.push_back(new_record);
    if (record_buffer.size() >= file_cap) {
        flushWriteBuffer();
    }
}
std::string HashStorage::makeFileName(int stor_id)
{
    char buf[512];
    sprintf(buf, "hashstorage.%04d", stor_id);
    return std::string(buf);
}
void HashStorage::writeRecord(std::unique_ptr<IntWriter> &writer, const HashRecord &record)
{
    writer->writeInt(record.hash_value);
    writer->writeZippedInt(record.physical_id);
    writer->writeZippedInt(record.logical_id);
}
bool HashStorage::readRecord(std::unique_ptr<IntReader> &reader, HashRecord &record)
{
    record.hash_value = reader->readInt();
    record.physical_id = reader->readZippedInt();
    record.logical_id = reader->readZippedInt();
    return !reader->eofOccured();
}
void HashStorage::sortBuffer()
{
    std::stable_sort(record_buffer.begin(), record_buffer.end(), comparator);
}
void HashStorage::flushWriteBuffer()
{
    int stor_id = n_stor++;
    std::string file_name = makeFileName(stor_id);
    stor_name.push_back(file_name);
    stor_writer.push_back(std::make_unique<IntWriter>(file_name));
    stor_reader.push_back(std::make_unique<IntReader>(file_name));
    
    sortBuffer();
    auto &writer = stor_writer.back();
    printf("  writing records to '%s' ...\n", file_name.c_str());
    for (auto &r: record_buffer) {
        writeRecord(writer, r);
    }
    record_buffer.clear();
}
void HashStorage::finishEmitRecord()
{
    flushWriteBuffer();
    uint64_t space_used = 0;
    for (auto &w: stor_writer) {
        w->flush();
        space_used += w->tell();
    }
    printf("  hash storage used %.3fGB of disk space.\n", space_used / 1073741824.0);
}
void HashStorage::iterateSortedRecordInternal(bool file_sorted, std::function<void()> begin_callback, std::function<void(int, HashRecord &)> record_callback)
{
    if (!file_sorted) {
        for (int stor_id = 0; stor_id < n_stor; stor_id++) {
            auto &file_name = stor_name[stor_id];
            printf("  sorting records in '%s' ...\n", file_name.c_str());
            HashRecord record;
            auto &reader = stor_reader[stor_id];
            record_buffer.clear();
            reader->rewind();
            while (readRecord(reader, record)) {
                record_buffer.push_back(record);
            }
            sortBuffer();
            auto &writer = stor_writer[stor_id];
            writer->rewind();
            for (auto &r: record_buffer) {
                writeRecord(writer, r);
            }
            writer->flush();
        }
    }

    printf("  performing %d-way merge-sort ...\n", n_stor);
    begin_callback();
    std::vector<HashRecord> head(n_stor);
    auto pqcomp = [&](int lhs, int rhs) { return !comparator(head[lhs], head[rhs]); };
    std::priority_queue<int, std::vector<int>, decltype(pqcomp)> pq(pqcomp);
    for (int stor_id = 0; stor_id < n_stor; stor_id++) {
        stor_reader[stor_id]->rewind();
        if (readRecord(stor_reader[stor_id], head[stor_id])) {
            pq.push(stor_id);
        }
    }
    while (!pq.empty()) {
        int stor_id = pq.top(); pq.pop();
        record_callback(stor_id, head[stor_id]);
        if (readRecord(stor_reader[stor_id], head[stor_id])) {
            pq.push(stor_id);
        }
    }
}

void HashStorage::iterateSortedRecord(bool file_sorted, std::function<void(const HashRecord &)> callback)
{
    iterateSortedRecordInternal(file_sorted, [](){}, [&](int, HashRecord &r) { callback(r); });
}

void HashStorage::iterateSortedRecordAndModifyHashInplace(bool file_sorted, std::function<void(HashRecord &)> callback)
{
    iterateSortedRecordInternal(file_sorted,
        [&]() {
            for (auto &w: stor_writer) {
                w->rewind();
            }
        },
        [&](int stor_id, HashRecord &record) {
            callback(record);
            writeRecord(stor_writer[stor_id], record);
        }
    );
    for (auto &w: stor_writer) {
        w->flush();
    }
}