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

    resetProgress();

    auto physical_set = std::make_unique<BitVector>();
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

                uint64_t physical_id = physical_off / block_size;
                physical_set->ensure(physical_id + 1);
                if (!physical_set->get(physical_id)) {
                    physical_blocks++;
                    physical_set->set(physical_id, true);
                }
                
                hash_record.logical_id = f.logical_id_base + logical_off / block_size;
                
                if ((buffer = read_data())) {
                    hashed_blocks++;
                    hash_record.hash_value = XXH64(buffer, block_size, 0);
                    hash_storage.emitRecord(hash_record);
                    if (shouldPrintProgress()) {
                        LOG("  progress: now hashed %.2fGiB of data\n", hashed_blocks * block_size / 1073741824.0);
                    }
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
    physical_set.reset();

    // group blocks respecting to ref_limit
    uint64_t group_id = -1;
    uint64_t group_hash;
    uint64_t group_ref = 0;
    hash_storage.iterateSortedRecordAndModifyHashInplace(true, [&](auto &record) {
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

void DedupInstance::iterateGroups(std::function<void(std::vector<uint64_t/*logical_id*/> &group)> group_callback)
{
    std::vector<uint64_t> group;

    hash_storage.comparator = [](const auto &lhs, const auto &rhs) {
        return std::tie(lhs.group_id, lhs.logical_id) < std::tie(rhs.group_id, rhs.logical_id);
    };

    uint64_t group_id = -1;
    hash_storage.iterateSortedRecord(false, [&](const HashRecord &record) {
        if (record.group_id != group_id) {
            group_callback(group);
            group_id = record.group_id;
            group.clear();
        }
        group.push_back(record.logical_id);
    });
    group_callback(group);
}

void DedupInstance::submitDuplicate()
{
    auto dump_group = [&](std::vector<uint64_t> &group) {
        LOG("=== BEGIN OF GROUP DUMP ===\n");
        for (auto logical_id: group) {
            auto f = getFileItemByLogicalID(logical_id);
            uint64_t off = (logical_id - f->logical_id_base) * block_size;
            LOG("%016" PRIX64 ": off %016" PRIX64 " file '%s'\n", logical_id, off, f->file_name.c_str());
        }
        LOG("=== END OF GROUP DUMP ===\n");
    };

    uint64_t redirect_bytes = 0;
    uint64_t processed = 0;
    resetProgress();
    
    iterateGroups([&](std::vector<uint64_t> &group){
        if (group.size() < 2) return;

        if (shouldPrintProgress()) {
            LOG("  progress: %3.0f%% (redirected %.2fGiB of data)\n", 100.0 * processed / shared_blocks, redirect_bytes / 1073741824.0);
        }
        processed++;
        
        allocChunkBlock();
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
            dump_group(group);
            return;
        }
        
        // submit range
        KernelInterface::dedupRange(tmp_fd, tmp_off, block_size, dedup_buffer);

        // check results
        auto it = group.begin();
        for (auto &[dest_fd, dest_offset, result]: dedup_buffer) {
            auto logical_id = *it++;
            if (result != -1) {
                redirect_bytes += result;
            } else {
                LOG("warning: unable to dedup %016" PRIX64 "\n", logical_id);
                dump_group(group);
            }
        }
    });

    LOG("successfully redirected %.3fGiB of data.\n", redirect_bytes / 1073741824.0);
    
}

void DedupInstance::relocateUnique()
{
    uint64_t relocate_bytes = 0;
    uint64_t processed = 0;
    resetProgress();

    uint64_t logical_id_base = -1;
    std::string dest_fn;
    int dest_fd = -1;

    uint64_t range_offset, range_length = 0;
    auto flush_range = [&]() {
        if (dest_fd == -1 || range_length == 0) return;
        std::vector<std::tuple<int, uint64_t, uint64_t>> dedup_buffer;
        dedup_buffer.push_back(std::make_tuple(dest_fd, range_offset, 0));
        KernelInterface::dedupRange(tmp_fd, 0, range_length, dedup_buffer);
        uint64_t result;
        std::tie(std::ignore, std::ignore, result) = dedup_buffer[0];
        if (result != -1) {
            relocate_bytes += result;
        } else {
            LOG("warning: unable to relocate file '%s' offset %016" PRIX64 " length %016" PRIX64 "\n", dest_fn.c_str(), range_offset, range_length);
        }
    };

    iterateGroups([&](std::vector<uint64_t> &group){
        if (group.size() != 1) return;

        if (shouldPrintProgress()) {
            LOG("  progress: %3.0f%% (relocated %.2fGiB of data)\n", 100.0 * processed / unique_blocks, relocate_bytes / 1073741824.0);
        }
        processed++;

        uint64_t logical_id = group[0];

        auto dest_f = getFileItemByLogicalID(logical_id);
        uint64_t dest_off = (logical_id - dest_f->logical_id_base) * block_size;

        if (logical_id_base != dest_f->logical_id_base || dest_off != range_offset + range_length || range_length >= chunk_limit) {
            flush_range();
            dest_fn = dest_f->file_name;
            logical_id_base = dest_f->logical_id_base;
            dest_fd = getFD(dest_f);
            range_offset = dest_off;
            range_length = 0;
            truncateChunkStore();
        }

        KernelInterface::copyRange(tmp_fd, range_length, dest_fd, dest_off, block_size);
        range_length += block_size;
    });
    flush_range();
    LOG("successfully relocated %.3fGiB of data.\n", relocate_bytes / 1073741824.0);
}

void DedupInstance::truncateChunkStore()
{
    KernelInterface::closeFD(tmp_fd);
    tmp_fd = KernelInterface::openFD(chunk_file, O_RDWR | O_CREAT | O_TRUNC);
    if (tmp_fd < 0) {
        LOG("unable to create chunk storage.\n");
    }
    VERIFY(tmp_fd >= 0);
}
void DedupInstance::allocChunkBlock()
{
    tmp_off += block_size;
    if (tmp_fd < 0 || tmp_off >= chunk_limit) {
        truncateChunkStore();
        tmp_off = 0;
    }
    VERIFY(tmp_fd >= 0);
}

void DedupInstance::resetProgress()
{
    next_progress = time(NULL) + 60;
}
bool DedupInstance::shouldPrintProgress()
{
    if (time(NULL) >= next_progress) {
        next_progress += 60;
        return true;
    }
    return false;
}

void DedupInstance::doDedup()
{
    LOG("step 1: hash files & group blocks ...\n");
    hashFiles();
    LOG("\n");

    LOG("statistics:\n");
    LOG("  physical blocks: %" PRIu64 " (%.3fGiB)\n", physical_blocks, physical_blocks * block_size / 1073741824.0);
    LOG("  ignored blocks: %" PRIu64 " (%.3fGiB)\n", ignored_blocks, ignored_blocks * block_size / 1073741824.0);
    LOG("  hased blocks: %" PRIu64 " (%.3fGiB)\n", hashed_blocks, hashed_blocks * block_size / 1073741824.0);
    LOG("  shared blocks: %" PRIu64 " (%.3fGiB)\n", shared_blocks, shared_blocks * block_size / 1073741824.0);
    LOG("  unique blocks: %" PRIu64 " (%.3fGiB)\n", unique_blocks, unique_blocks * block_size / 1073741824.0);
    LOG("\n");

    uint64_t before_dedup = physical_blocks;
    uint64_t after_dedup = shared_blocks + unique_blocks;
    uint64_t delta = before_dedup - after_dedup;
    LOG("dedup plan:\n");
    LOG("  before dedup: %" PRIu64 " (%.3fGiB)\n", before_dedup, before_dedup * block_size / 1073741824.0);
    LOG("  after dedup: %" PRIu64 " (%.3fGiB)\n", after_dedup, after_dedup * block_size / 1073741824.0);
    LOG("  delta: %" PRIu64 " (%.3fGiB)\n", delta, delta * block_size / 1073741824.0);
    LOG("\n");
    if (delta == 0) {
        LOG("nothing to deduplicate!\n");
        return;
    }

    LOG("step 2: submit duplicate ranges to kernel ...\n");
    submitDuplicate();
    LOG("\n");

    LOG("step 3: relocate unique blocks ...\n");
    relocateUnique();
    LOG("\n");

    remove(chunk_file.c_str());
    LOG("finished!\n");
}
