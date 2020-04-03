#include "config.h"

#include <tuple>
#include <algorithm>

#include "xxhash.h"

#include "DedupInstance.h"
#include "HashStorage.h"
#include "KernelInterface.h"


DedupInstance::~DedupInstance()
{
    for (auto &f: file_list) {
        KernelInterface::closeFD(f.fd);
    }
}

void DedupInstance::addFile(const std::string &file_name)
{
    file_list.push_back(FileItem(file_name));
}

int DedupInstance::getFD(std::vector<FileItem>::iterator f)
{
    if (f->opened_it.has_value()) {
        // file is in cache
        auto it = std::any_cast<std::list<std::vector<FileItem>::iterator>::iterator>(f->opened_it);
        opened_file.splice(opened_file.begin(), opened_file, it);
        return f->fd;

    } else {
        // not in cache
        f->fd = KernelInterface::openFD(f->file_name);
        opened_file.push_front(f);
        f->opened_it = opened_file.begin();

        // shrink cache
        while (opened_file.size() > ref_limit) {
            auto it = opened_file.back();
            KernelInterface::closeFD(it->fd);
            it->fd = -1;
            it->opened_it.reset();
            opened_file.pop_back();
        }
        return f->fd;
    }
}

void DedupInstance::hashFiles()
{
    // hash each block of each file (skip already deduped blocks)

    hash_storage.comparator = [](const auto &lhs, const auto &rhs) {
        return std::tie(lhs.physical_id, lhs.hash_value) < std::tie(rhs.physical_id, rhs.hash_value);
    };

    for (auto &f: file_list) {
        KernelInterface::getFileBlocks(f.file_name, block_size, [&](uint64_t file_size) {
            f.size = file_size;
            f.logical_id_base = n_logical_id;
            n_logical_id += f.size / block_size;
        }, [&](uint64_t physical_off, uint64_t logical_off, uint64_t data_size, auto read_data) {
            if (data_size == block_size) {
                HashRecord hash_record;
                char *buffer;

                hash_record.hash_value = -1;
                hash_record.physical_id = physical_off / block_size;
                hash_record.logical_id = f.logical_id_base + logical_off / block_size;
                
                // don't hash a block twice
                physical_hashed.ensure(hash_record.physical_id + 1);
                if (!physical_hashed.get(hash_record.physical_id) && (buffer = read_data())) {
                    hash_record.hash_value = XXH64(buffer, block_size, 0);
                    physical_hashed.set(hash_record.physical_id, true);
                }
                
                hash_storage.emitRecord(hash_record);
            } else {
                // ignore unaligned end part of file
                ignored_blocks++;
            }
        });
    }
    hash_storage.finishEmitRecord();
    logical_deduped.ensure(n_logical_id);

    // fill hashes that not filled earlier
    HashRecord last_record;
    last_record.hash_value = -1;
    last_record.physical_id = -1;
    last_record.logical_id = -1;
    hash_storage.iterateSortedRecordAndModifyHashInplace(true, [&](auto &record) {
        if (record.physical_id != last_record.physical_id) {
            last_record = record;
        }
        if (record.hash_value == -1) {
            record.hash_value = last_record.hash_value;
        }
        hash_storage.writeRecordInplace(record);
    }, [](){});
}

void DedupInstance::groupBlocks()
{
    // group blocks respecting to ref_limit
    // this is the bin packing problem, use first fit algorithm

    uint64_t base_hash = 0;
    
    hash_storage.comparator = [](const auto &lhs, const auto &rhs) {
        return std::tie(lhs.hash_value, lhs.physical_id, lhs.logical_id) < std::tie(rhs.hash_value, rhs.physical_id, rhs.logical_id);
    };

    std::vector<HashRecord> bin_buffer;
    std::vector<HashRecord> read_buffer;
    uint64_t overref_id = -1, overref_cnt = 0;
    
    auto write_record_as_ignore = [&](HashRecord &record) {
        record.group_leader = -1;
        hash_storage.writeRecordInplace(record);
        //record.dump();
    };
    auto flush_bin_buffer = [&]() {
        if (!bin_buffer.empty()) {
            if (bin_buffer.size() == 1) {
                lonely_blocks++;
            } else if (bin_buffer.size() < ref_limit) {
                popular_blocks++;
            } else if (bin_buffer.size() == ref_limit) {
                hotspot_blocks++;
            }
            auto leader = std::min_element(bin_buffer.begin(), bin_buffer.end(), [](auto &lhs, auto &rhs){ return lhs.logical_id < rhs.logical_id; });
            for (auto &r: bin_buffer) {
                r.group_leader = leader->physical_id;
                hash_storage.writeRecordInplace(r);
                //r.dump();
            }
            bin_buffer.clear();
        }
    };
    auto flush_read_buffer = [&]() {
        if (bin_buffer.size() + read_buffer.size() <= ref_limit) {
            bin_buffer.insert(bin_buffer.end(), read_buffer.begin(), read_buffer.end());
            read_buffer.clear();
        } else {
            flush_bin_buffer();
            bin_buffer = std::move(read_buffer);
            read_buffer.clear();
        }
    };
    auto show_overref = [&]() {
        if (overref_id != -1) {
            overref_blocks++;
            printf("warning: block %016" PRIX64 " already has %" PRIu64 " references\n", overref_id, overref_cnt);
        }
    };

    hash_storage.iterateSortedRecordAndModifyHashInplace(false, [&](auto &record) {
        //record.dump();
        if (overref_id != -1) {
            if (overref_id != record.physical_id) {
                show_overref();
                overref_id = -1;
            } else {
                overref_cnt++;
                write_record_as_ignore(record);
                return;
            }
        }
        if (read_buffer.empty() || record.hash_value != base_hash) {
            flush_read_buffer();
            flush_bin_buffer();
            // new hash occurred, set as base
            base_hash = record.hash_value;
        }
        if (!read_buffer.empty() && read_buffer.front().physical_id != record.physical_id) {
            flush_read_buffer();
        }
        read_buffer.push_back(record);
        if (read_buffer.size() > ref_limit) {
            overref_id = record.physical_id;
            overref_cnt = read_buffer.size();
            flush_bin_buffer();
            for (auto &r: read_buffer) {
                write_record_as_ignore(r);
            }
            read_buffer.clear();
        }
    }, [&]() {
        show_overref();
        flush_read_buffer();
        flush_bin_buffer();
    });
}

void DedupInstance::submitRanges()
{
    // sort hash records and dedup

    uint64_t base_physical_id = -1;
    uint64_t base_off = 0;
    std::vector<FileItem>::iterator base_f;
    std::vector<std::pair<std::vector<FileItem>::iterator/*dest_f*/, uint64_t/*dest_off*/>> dedup_targets;

    hash_storage.comparator = [](const auto &lhs, const auto &rhs) {
        return std::tie(lhs.group_leader, lhs.logical_id) < std::tie(rhs.group_leader, rhs.logical_id);
    };

    auto flush_targets = [&](){
        if (!dedup_targets.empty()) {
            int base_fd = getFD(base_f);
            if (base_fd >= 0) {
                // copy targets to buffer
                std::vector<std::tuple<int, uint64_t, uint64_t>> dedup_buffer;
                for (auto &[dest_f, dest_off]: dedup_targets) {
                    int dest_fd = getFD(dest_f);
                    if (dest_fd >= 0) {
                        dedup_buffer.push_back(std::make_tuple(dest_fd, dest_off, 0));
                    }
                }

                // submit range
                uint64_t range_length = block_size;
                KernelInterface::dedupRange(base_fd, base_off, range_length, dedup_buffer);

                // check results
                auto target_it = dedup_targets.begin();
                for (auto &[dest_fd, dest_offset, result]: dedup_buffer) {
                    auto &[dest_f, dest_off] = *target_it++;
                    if (result != -1) {
                        total_dedup += result;
                    } else {
                        printf("warning: unable to dedup '%s' offset %016" PRIX64 " with base '%s' offset %016" PRIX64 " length %016" PRIX64 ".\n", dest_f->file_name.c_str(), dest_off, base_f->file_name.c_str(), base_off, range_length);
                    }
                }
            }
            dedup_targets.clear();
        }
    };

    hash_storage.iterateSortedRecord(false, [&](const HashRecord &record) {
        //record.dump();
        if (record.group_leader != -1) {

            FileItem t; t.logical_id_base = record.logical_id;
            auto f = --std::upper_bound(file_list.begin(), file_list.end(), t, [](const auto &lhs, const auto &rhs){ return lhs.logical_id_base < rhs.logical_id_base; });
            uint64_t off = (record.logical_id - f->logical_id_base) * block_size;

            if (record.group_leader == record.physical_id) {
                flush_targets();
                // switch to new base
                base_physical_id = record.physical_id;
                base_off = off;
                base_f = f;
            } else {
                dedup_targets.push_back(std::make_pair(f, off));
            }
        }
    });
    flush_targets();
}

void DedupInstance::doDedup()
{
    printf("step 1: hash files ...\n");
    hashFiles();
    printf("\n");

    printf("step 2: group blocks ...\n");
    groupBlocks();
    printf("\n");

    printf("step 3: submit ranges to kernel ...\n");
    submitRanges();
    printf("\n");

    printf("finished!\n");
    printf("\n");
    printf("total lonely blocks: %" PRIu64 "\n", lonely_blocks);
    printf("total popular blocks: %" PRIu64 "\n", popular_blocks);
    printf("total hotspot blocks: %" PRIu64 "\n", hotspot_blocks);
    printf("total overref blocks: %" PRIu64 "\n", overref_blocks);
    printf("total ignored blocks: %" PRIu64 "\n", ignored_blocks);
    
    printf("kernel reported %.3fGB of data deduplicated.\n", total_dedup / 1073741824.0);
    printf("\n");
}
