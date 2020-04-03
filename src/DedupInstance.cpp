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
        kern.closeFD(f.fd);
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
        f->fd = kern.openFD(f->file_name);
        opened_file.push_front(f);
        f->opened_it = opened_file.begin();

        // shrink cache
        while (opened_file.size() > ref_limit) {
            auto it = opened_file.back();
            kern.closeFD(it->fd);
            it->fd = -1;
            it->opened_it.reset();
            opened_file.pop_back();
        }
        return f->fd;
    }
}
// hash each block of each file
void DedupInstance::hashFiles()
{
    hash_storage.comparator = [](const auto &lhs, const auto &rhs) {
        return std::tie(lhs.physical_id, lhs.hash_value) < std::tie(rhs.physical_id, rhs.hash_value);
    };

    for (auto &f: file_list) {
        kern.getFileBlocks(f.file_name, block_size, [&](uint64_t file_size) {
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
    });
}

void DedupInstance::calcTargets()
{
    hash_storage.comparator = [](const auto &lhs, const auto &rhs) {
        return std::tie(lhs.hash_value, lhs.physical_id, lhs.logical_id) < std::tie(rhs.hash_value, rhs.physical_id, rhs.logical_id);
    };
    
    HashRecord base_record;
    base_record.hash_value = -1;
    base_record.physical_id = -1;
    base_record.logical_id = -1;
    uint64_t ref_count = 0;
    
    auto process_last_base = [&]() {
        if (ref_count > ref_limit) {
            printf("warning: block %016" PRIX64 " already has %" PRIu64 " references\n", base_record.physical_id, ref_count);
        }
        if (ref_count == 1) lonely_blocks++;
        if (ref_count > 1) popular_blocks++;
    };

    hash_storage.iterateSortedRecordAndModifyHashInplace(false, [&](HashRecord &record) {
        if (record.hash_value != base_record.hash_value) {
    change_base:
            process_last_base();
            // new hash occurred, set as base
            base_record = record;
            ref_count = 1;
            record.target_physical_id = record.physical_id;
        } else {
            if (record.physical_id == base_record.physical_id) {
                // this block already deduped, skip it
                ref_count++;
                record.target_physical_id = -1;
            } else {
                // this block can be deduped
                if (ref_count >= ref_limit) {
                    goto change_base;
                }
                ref_count++;
                record.target_physical_id = base_record.physical_id;
            }
        }
    });
    process_last_base();
}

void DedupInstance::submitRanges()
{
    // sort hash records and dedup
    hash_storage.comparator = [](const auto &lhs, const auto &rhs) {
        return std::tie(lhs.target_physical_id, lhs.physical_id, lhs.logical_id) < std::tie(rhs.target_physical_id, rhs.physical_id, rhs.logical_id);
    };

    uint64_t base_physical_id = -1;
    uint64_t base_offset = 0;
    std::vector<FileItem>::iterator base_f;
    std::vector<std::pair<std::vector<FileItem>::iterator, uint64_t>> dedup_targets;

    auto process_last_base = [&](){
        if (!dedup_targets.empty()) {
            int base_fd = getFD(base_f);
            if (base_fd >= 0) {
                std::vector<std::tuple<int, uint64_t, uint64_t>> dedup_buffer;
                for (auto &target: dedup_targets) {
                    int dest_fd = getFD(target.first);
                    if (dest_fd >= 0) {
                        dedup_buffer.push_back(std::make_tuple(dest_fd, target.second, 0));
                    }
                }
                kern.dedupRange(base_fd, base_offset, block_size, dedup_buffer);
                for (auto &[dest_fd, dest_offset, result]: dedup_buffer) {
                    if (result != -1) {
                        total_dedup += result;
                    }
                }
            }
            dedup_targets.clear();
        }
    };

    hash_storage.iterateSortedRecord(false, [&](const HashRecord &record) {
        //record.dump();
        if (record.target_physical_id != -1) {

            FileItem t; t.logical_id_base = record.logical_id;
            auto f = --std::upper_bound(file_list.begin(), file_list.end(), t, [](const auto &lhs, const auto &rhs){ return lhs.logical_id_base < rhs.logical_id_base; });
            uint64_t off = (record.logical_id - f->logical_id_base) * block_size;

            if (record.target_physical_id == record.physical_id) {
                process_last_base();
                // switch to new base
                base_physical_id = record.physical_id;
                base_offset = off;
                base_f = f;
            } else {
                dedup_targets.push_back(std::make_pair(f, off));
            }
        }
    });

    process_last_base();
}

void DedupInstance::doDedup()
{
    printf("step 1: hash files ...\n");
    hashFiles();
    printf("\n");

    printf("step 2: calculate dedup targets ...\n");
    calcTargets();
    printf("\n");

    printf("step 3: submit ranges to kernel ...\n");
    submitRanges();
    printf("\n");

    printf("finished!\n");
    printf("total ignored blocks: %" PRIu64 "\n", ignored_blocks);
    printf("total lonely blocks: %" PRIu64 "\n", lonely_blocks);
    printf("total popular blocks: %" PRIu64 "\n", popular_blocks);
    printf("kernel reported %.3fGB of data deduplicated.\n", total_dedup / 1073741824.0);
    printf("\n");
}
