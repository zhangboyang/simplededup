#include "config.h"

#include "xxhash.h"

#include "DedupInstance.h"
#include "HashStorage.h"
#include "KernelInterface.h"


DedupInstance::~DedupInstance()
{
    for (auto &f: file_list) {
        KernelInterface::closeFD(f.fd);
    }
    KernelInterface::closeFD(tmp_fd);
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

std::vector<DedupInstance::FileItem>::iterator DedupInstance::getFileItemByLogicalID(uint64_t logical_id)
{
    FileItem t; t.logical_id_base = logical_id;
    return --std::upper_bound(file_list.begin(), file_list.end(), t, [](const auto &lhs, const auto &rhs){ return lhs.logical_id_base < rhs.logical_id_base; });
}

void DedupInstance::hashFiles()
{
    // hash each block of each file (skip already deduped blocks)

    hash_storage.comparator = [](const auto &lhs, const auto &rhs) {
        return std::tie(lhs.hash_value, lhs.logical_id) < std::tie(rhs.hash_value, rhs.logical_id);
    };

    hash_storage.beginEmitRecord();

    for (auto &f: file_list) {
        bool success = KernelInterface::getFileBlocks(f.file_name, block_size, [&](uint64_t file_size) {
            f.size = file_size;
            f.logical_id_base = n_logical_id;
            n_logical_id += f.size / block_size;
        }, [&](uint64_t physical_off, uint64_t logical_off, uint64_t data_size, auto read_data) {
            if (data_size == block_size) {
                HashRecord hash_record;
                char *buffer;

                hash_record.logical_id = f.logical_id_base + logical_off / block_size;
                
                if ((buffer = read_data())) {
                    hash_record.hash_value = XXH64(buffer, block_size, 0);
                    hash_storage.emitRecord(hash_record);
                } else {
                    // ignore error block
                    ignored_blocks++;
                }
            } else {
                // ignore unaligned end part of file
                ignored_blocks++;
            }
        });
        if (!success) {
            f.size = 0;
            f.logical_id_base = n_logical_id;
        }
    }
    hash_storage.finishEmitRecord();

    // group blocks respecting to ref_limit
    uint64_t group_id = -1;
    uint64_t group_hash;
    uint64_t group_ref = 0;
    hash_storage.iterateSortedRecordAndModifyHashInplace(true, [&](auto &record) {
        hashed_blocks++;
        if (group_id == -1 || group_ref >= ref_limit || record.hash_value != group_hash) {
            if (group_ref > 0) {
                (group_ref > 1 ? shared_blocks : unique_blocks)++;
            }
            group_id = record.logical_id;
            group_ref = 0;
            group_hash = record.hash_value;
        }
        group_ref++;
        record.group_id = group_id;
        hash_storage.writeRecordInplace(record);
    }, [](){});
    if (group_ref > 0) {
        (group_ref > 1 ? shared_blocks : unique_blocks)++;
    }
}

uint64_t DedupInstance::submitRanges(int mode)
{
    // sort hash records and dedup
    
    uint64_t counter = 0;

    std::vector<uint64_t> group;

    hash_storage.comparator = [](const auto &lhs, const auto &rhs) {
        return std::tie(lhs.group_id, lhs.logical_id) < std::tie(rhs.group_id, rhs.logical_id);
    };

    auto dump_group = [&]() {
        LOG("=== BEGIN OF GROUP DUMP ===\n");
        for (auto logical_id: group) {
            auto f = getFileItemByLogicalID(logical_id);
            uint64_t off = (logical_id - f->logical_id_base) * block_size;
            LOG("%016" PRIX64 ": off %016" PRIX64 " file '%s'\n", logical_id, off, f->file_name.c_str());
        }
        LOG("=== END OF GROUP DUMP ===\n");
    };

    auto flush_targets = [&](){
        if (mode == 0) {
            if (group.size() <= 1) return;
        }
        if (mode == 1) {
            if (group.size() != 1) return;
        }
        
        AllocTempRange();
        bool copy_success = false;
        
        // fill range buffer
        std::vector<std::tuple<int, uint64_t, uint64_t>> dedup_buffer;
        for (auto logical_id: group) {
            auto dest_f = getFileItemByLogicalID(logical_id);
            uint64_t dest_off = (logical_id - dest_f->logical_id_base) * block_size;
            
            int dest_fd = getFD(dest_f);
            if (dest_fd >= 0) {
                if (!copy_success) {
                    copy_success = KernelInterface::copyRange(tmp_fd, tmp_off, dest_fd, dest_off, block_size);
                }
                dedup_buffer.push_back(std::make_tuple(dest_fd, dest_off, 0));
            }
        }

        if (!copy_success) {
            LOG("warning: unable to copy group data\n");
            dump_group();
            return;
        }

        // submit range
        KernelInterface::dedupRange(tmp_fd, tmp_off, block_size, dedup_buffer);

        // check results
        auto it = group.begin();
        for (auto &[dest_fd, dest_offset, result]: dedup_buffer) {
            auto logical_id = *it++;
            if (result != -1) {
                counter += result;
            } else {
                LOG("warning: unable to dedup %016" PRIX64 "\n", logical_id);
                dump_group();
            }
        }

        if (mode == 0) {
            counter -= block_size;
        }
    };

    uint64_t group_id = -1;
    hash_storage.iterateSortedRecord(false, [&](const HashRecord &record) {
        if (record.group_id != group_id) {
            flush_targets();
            group_id = record.group_id;
            group.clear();
        }
        group.push_back(record.logical_id);
    });
    flush_targets();

    return counter;
}

void DedupInstance::AllocTempRange()
{
    tmp_off += block_size;
    if (tmp_fd < 0 || tmp_off >= chunk_limit) {
        KernelInterface::closeFD(tmp_fd);
        tmp_fd = KernelInterface::openFD(chunk_file, O_RDWR | O_CREAT | O_TRUNC);
        if (tmp_fd < 0) {
            LOG("unable to create chunk storage.\n");
        }
        tmp_off = 0;
    }
    VERIFY(tmp_fd > 0);
}
void DedupInstance::doDedup()
{
    LOG("step 1: hash files & group blocks ...\n");
    hashFiles();
    LOG("\n");

    uint64_t dedup_blocks = hashed_blocks - shared_blocks - unique_blocks;
    LOG("dedup plan:\n");
    LOG("  ignored blocks: %" PRIu64 " (%.3fGB)\n", ignored_blocks, ignored_blocks * block_size / 1073741824.0);
    LOG("  hased blocks: %" PRIu64 " (%.3fGB)\n", hashed_blocks, hashed_blocks * block_size / 1073741824.0);
    LOG("  shared blocks: %" PRIu64 " (%.3fGB)\n", shared_blocks, shared_blocks * block_size / 1073741824.0);
    LOG("  unique blocks: %" PRIu64 " (%.3fGB)\n", unique_blocks, unique_blocks * block_size / 1073741824.0);
    LOG("  dedup blocks: %" PRIu64 " (%.3fGB)\n", dedup_blocks, dedup_blocks * block_size / 1073741824.0);
    LOG("\n");
    if (dedup_blocks == 0) {
        LOG("nothing to deduplicate.\n");
        LOG("\n");
        return;
    }

    LOG("step 2: submit duplicate ranges to kernel ...\n");
    uint64_t dedup_bytes = submitRanges(0);
    LOG("successfully deduplicated %.3fGB of data.\n", dedup_bytes / 1073741824.0);
    LOG("\n");

    LOG("step 3: copy unique blocks ...\n");
    uint64_t copy_bytes = submitRanges(1);
    LOG("successfully copied %.3fGB of data.\n", copy_bytes / 1073741824.0);
    LOG("\n");

    remove(chunk_file.c_str());
    LOG("finished!\n");
}
