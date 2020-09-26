#include "config.h"

#include "HashStorage.h"

HashStorage::~HashStorage()
{
    for (int i = 0; i < n_stor; i++) {
        remove(makeFileName(i).c_str());
    }
}
void HashStorage::beginEmitRecord()
{
    buffer_cap = sort_mem * 1048576 / sizeof(HashRecord);
    reserveBuffer();
}
void HashStorage::emitRecord(const HashRecord &new_record)
{
    record_buffer.push_back(new_record);
    if (record_buffer.size() >= buffer_cap) {
        flushWriteBuffer();
    }
}
std::string HashStorage::makeFileName(int stor_id)
{
    char buf[512];
    sprintf(buf, ".%04d", stor_id);
    return stor_path + std::string(buf);
}
void HashStorage::writeRecord(std::unique_ptr<IntWriter> &writer, const HashRecord &record)
{
    writer->writeInt(record.hash_value);
    writer->writeZippedInt(record.logical_id);
}
bool HashStorage::readRecord(std::unique_ptr<IntReader> &reader, HashRecord &record)
{
    record.hash_value = reader->readInt();
    record.logical_id = reader->readZippedInt();
    return !reader->eofOccured();
}
void HashStorage::discardBuffer()
{
    // clear and free memory
    std::vector<HashRecord>().swap(record_buffer);
}
void HashStorage::reserveBuffer()
{
    record_buffer.reserve(buffer_cap);
}
void HashStorage::sortBuffer()
{
    std::sort(record_buffer.begin(), record_buffer.end(), comparator);
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
    LOG("  writing records to '%s' ...\n", file_name.c_str());
    for (auto &r: record_buffer) {
        writeRecord(writer, r);
    }
    record_buffer.clear();
}
void HashStorage::finishEmitRecord()
{
    flushWriteBuffer();
    discardBuffer();
    uint64_t space_used = 0;
    for (auto &w: stor_writer) {
        w->flush();
        uint64_t written_bytes = w->tell();
        stor_used_bytes.push_back(written_bytes);
        space_used += written_bytes;
    }
    LOG("  hash storage used %s of disk space.\n", HB(space_used));
}
void HashStorage::iterateSortedRecordInternal(bool file_sorted, std::function<void()> begin_callback, std::function<void(int, HashRecord &)> record_callback)
{
    if (!file_sorted) {
        reserveBuffer();
        for (int stor_id = 0; stor_id < n_stor; stor_id++) {
            auto &file_name = stor_name[stor_id];
            LOG("  sorting records in '%s' ...\n", file_name.c_str());
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
        discardBuffer();
    }

    LOG("  performing %d-way merge-sort ...\n", n_stor);
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

void HashStorage::iterateSortedRecord(bool file_sorted, std::function<void(const HashRecord &)> iter_callback)
{
    iterateSortedRecordInternal(file_sorted, [](){}, [&](int stor_id, HashRecord &record) {
        iter_callback(record);
    });
}

void HashStorage::iterateSortedRecordAndModifyHashInplace(bool file_sorted, std::function<void(HashRecord &)> iter_callback, std::function<void()> flush_callback)
{
    // only hash_value can be changed, because record size can't change
    // the write sequence can not be changed
    std::deque<int> q;
    writeRecordInplace = [&](const HashRecord &record) {
        writeRecord(stor_writer[q.front()], record);
        q.pop_front();
    };
    iterateSortedRecordInternal(file_sorted,
        [&]() {
            for (auto &w: stor_writer) {
                w->rewind();
            }
        },
        [&](int stor_id, HashRecord &record) {
            q.push_back(stor_id);
            iter_callback(record);
        }
    );
    flush_callback();
    auto it = stor_used_bytes.begin();
    for (auto &w: stor_writer) {
        w->flush();
        VERIFY(w->tell() == *it++);
    }
    writeRecordInplace = nullptr;
}