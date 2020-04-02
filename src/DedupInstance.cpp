#include "config.h"

#include <cinttypes>
#include <tuple>
#include <algorithm>

#include "xxhash.h"

#include "DedupInstance.h"
#include "HashStorage.h"
#include "KernelInterface.h"

void DedupInstance::addFile(const std::string &file_name)
{
    file_list.push_back(FileItem(file_name));
}

// hash each block of each file
void DedupInstance::hashFiles()
{
    for (auto &f: file_list) {
        bool success = kern.getFileBlocks(f.file_name, block_size, [&](uint64_t file_size) {
            f.size = file_size;
            f.logical_id_base = n_logical_id;
            n_logical_id += f.size / block_size;
        }, [&](uint64_t physical_off, uint64_t logical_off, auto read_data) {

            HashStorage::HashRecord hash_record;
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
        });

        f.ignored = !success;
    }
    hash_storage.finishEmitRecord();
    logical_deduped.ensure(n_logical_id);
}

uint64_t DedupInstance::submitRanges()
{
    // sort hash records and dedup
    uint64_t base_hash = -1;
    int base_fd = -1;
    uint64_t base_offset = 0;

    uint64_t total_dedup = 0;

    hash_storage.comparator = [](const auto &lhs, const auto &rhs) {
        return lhs.hash_value < rhs.hash_value;
    };
    hash_storage.iterateSortedRecord(false, [&](const auto &record) {
        FileItem t; t.logical_id_base = record.logical_id;
        FileItem &f = *--std::upper_bound(file_list.begin(), file_list.end(), t, [](const auto &lhs, const auto &rhs){ return lhs.logical_id_base < rhs.logical_id_base; });
        uint64_t off = (record.logical_id - f.logical_id_base) * block_size;

        //printf("%016llx %016llx %016llx  %s %llx\n", record.hash_value, record.physical_id, record.logical_id, f.file_name.c_str(), off);

        if (base_fd == -1 || record.hash_value != base_hash) {
            base_hash = record.hash_value;
            base_offset = off;
            kern.switchFD(base_fd, f.file_name);
        } else if (base_fd >= 0) {
            int fd = kern.getFD(f.file_name);
            if (fd >= 0) {
                std::vector<std::tuple<int, uint64_t, uint64_t>> targets {{fd, off, 0}};
                total_dedup += kern.dedupRange(base_fd, base_offset, block_size, targets);
            }
            kern.releaseFD(fd);
        }
    });
    kern.releaseFD(base_fd);

    return total_dedup;
}

void DedupInstance::doDedup()
{
    // sort file names
    std::sort(file_list.begin(), file_list.end());

    // assign id to each file
    for (uint64_t i = 0; i < file_list.size(); i++) {
        file_list[i].id = i;
    }

    printf("step 1: hash files ...\n");
    hash_storage.comparator = [](const auto &lhs, const auto &rhs) {
        return std::tie(lhs.physical_id, lhs.hash_value) < std::tie(rhs.physical_id, rhs.hash_value);
    };
    hashFiles();
    printf("\n");

    printf("step X: submit duplicate ranges to kernel ...\n");
    uint64_t total_dedup = submitRanges();
    printf("\n");

    printf("finished!\n");
    printf("kernel reported %.3fGB of data deduplicated.\n", total_dedup / 1073741824.0);
    printf("\n");
}