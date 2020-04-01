#include "config.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <linux/fs.h>
#include <linux/fiemap.h>

#include <cstring>
#include <cstdlib>
#include <algorithm>
#include <tuple>

#include "xxhash.h"

#include "DedupInstance.h"
#include "HashStorage.h"


void DedupInstance::addFile(const std::string &file_name)
{
    file_list.push_back(FileItem(file_name));
}

// read a file and hash each block
void DedupInstance::hashFile(FileItem &f)
{
    auto file_name = f.file_name.c_str();
    struct stat sb;
    struct fiemap mapprobe;
    struct fiemap *mapdata = NULL;
    char *buffer = (char *) malloc(block_size);
    int fd = -1;
    int r;
    bool success = false;
    size_t array_bytes;

    if (lstat(file_name, &sb) == -1) {
        printf("error: can't lstat '%s', file ignored. (%s)\n", file_name, strerror(errno));
        goto fail;
    }

    if ((sb.st_mode & S_IFMT) != S_IFREG) {
        printf("error: '%s' is not a regular file, file ignored.\n", file_name);
        goto fail;
    }

    fd = open(file_name, O_RDONLY); // ignore race cond between lstat() and open()
    if (fd == -1) {
        printf("error: can't open '%s', file ignored. (%s)\n", file_name, strerror(errno));
        goto fail;
    }

    f.size = sb.st_size;

    memset(&mapprobe, 0, sizeof(mapprobe));
    mapprobe.fm_start = 0;
    mapprobe.fm_length = f.size;
    mapprobe.fm_flags = FIEMAP_FLAG_SYNC;
    mapprobe.fm_extent_count = 0;

    r = ioctl(fd, FS_IOC_FIEMAP, &mapprobe);
    if (r < 0) {
        printf("error: '%s' fiemap failed, file ignored. (%s)\n", file_name, strerror(errno));
        goto fail;
    }

    array_bytes = sizeof(struct fiemap_extent) * mapprobe.fm_mapped_extents; // ignore integer overflow
    mapdata = (struct fiemap *) malloc(sizeof(struct fiemap) + array_bytes);
    if (!mapdata) {
        printf("error: can't alloc memory for fiemap '%s', file ignored. (%s)\n", file_name, strerror(errno));
        goto fail;
    }
    memset(mapdata, 0, sizeof(struct fiemap) + array_bytes);
    mapdata->fm_start = 0;
    mapdata->fm_length = f.size;
    mapdata->fm_flags = FIEMAP_FLAG_SYNC;
    mapdata->fm_extent_count = mapprobe.fm_mapped_extents;

    r = ioctl(fd, FS_IOC_FIEMAP, mapdata);
    if (r < 0) {
        printf("error: '%s' fiemap failed, file ignored. (%s)\n", file_name, strerror(errno));
        goto fail;
    }

    f.logical_id_base = n_logical_id;
    n_logical_id += f.size / block_size;

    for (uint64_t i = 0; i < mapdata->fm_mapped_extents; i++) {
        auto e = &mapdata->fm_extents[i];
        //printf("%x %llx %llx %llx\n", e->fe_flags, e->fe_logical, e->fe_physical, e->fe_length);
        if (e->fe_flags & FIEMAP_EXTENT_NOT_ALIGNED) continue;
        if (e->fe_logical % block_size != 0 || e->fe_physical % block_size != 0 || e->fe_length % block_size != 0) {
            printf("warning: '%s' extents not aligned, extents ignored.\n", file_name);
            continue;
        }
        for (uint64_t off = 0; off < e->fe_length; off += block_size) {
            if (f.size - (e->fe_logical + off) < block_size) {
                // ignore last unaligned part of file
                continue;
            }

            HashStorage::HashRecord hash_record;

            hash_record.physical_id = (e->fe_physical + off) / block_size;
            hash_record.logical_id = f.logical_id_base + (e->fe_logical + off) / block_size;

            physical_hashed.ensure(hash_record.physical_id + 1);
            if (physical_hashed.get(hash_record.physical_id)) {

                // this physical block has been hashed, don't hash it twice
                hash_record.hash_value = -1;

            } else {

                if (lseek(fd, e->fe_logical + off, SEEK_SET) == -1) {
                    printf("warning: '%s' lseek failed, block ignored. (%s)\n", file_name, strerror(errno));
                    continue;
                }
                if (read(fd, buffer, block_size) != block_size) {
                    printf("warning: '%s' read failed, block ignored. (%s)\n", file_name, strerror(errno));
                    continue;
                }

                hash_record.hash_value = XXH64(buffer, block_size, 0);
                physical_hashed.set(hash_record.physical_id, true);
            }
            
            hash_storage.emitRecord(hash_record);
        }
    }


    success = true;
fail:
    if (!success) f.ignore = true;
    if (fd != -1) close(fd);
    if (mapdata) free(mapdata);
    if (buffer) free(buffer);
}

void DedupInstance::doDedup()
{
    // sort file names
    std::sort(file_list.begin(), file_list.end());

    hash_storage.comparator = [](const auto &lhs, const auto &rhs) {
        return std::tie(lhs.physical_id, lhs.hash_value) < std::tie(rhs.physical_id, rhs.hash_value);
    };
    // assign id to each file
    for (uint64_t i = 0; i < file_list.size(); i++) {
        file_list[i].id = i;
    }

    // hash each file
    for (auto &f: file_list) {
        hashFile(f);
        printf("%llx\n", f.logical_id_base);
    }
    hash_storage.finishEmitRecord();
    logical_deduped.ensure(n_logical_id);



    // sort hash records and dedup
    hash_storage.comparator = [](const auto &lhs, const auto &rhs) { return lhs.hash_value < rhs.hash_value; };
    uint64_t base_hash = -1;
    int base_fd = -1;
    uint64_t base_offset = 0;
    size_t dedup_info_size = sizeof(struct file_dedupe_range) + sizeof(struct file_dedupe_range_info);
    struct file_dedupe_range *dedup_info = (struct file_dedupe_range *) malloc(dedup_info_size);

    hash_storage.iterateSortedRecord(false, [&](const auto &record) {
        FileItem t; t.logical_id_base = record.logical_id;
        FileItem &f = *--std::upper_bound(file_list.begin(), file_list.end(), t, [](const auto &lhs, const auto &rhs){ return lhs.logical_id_base < rhs.logical_id_base; });
        uint64_t off = (record.logical_id - f.logical_id_base) * block_size;

        printf("%016llx %016llx %016llx  %s %llx\n", record.hash_value, record.physical_id, record.logical_id, f.file_name.c_str(), off);

        if (base_fd == -1 || record.hash_value != base_hash) {
            base_hash = record.hash_value;
            base_offset = off;
            if (base_fd >= 0) { close(base_fd); base_fd = -1; }
            base_fd = open(f.file_name.c_str(), O_RDONLY);
        } else if (base_fd >= 0) {
            int fd = open(f.file_name.c_str(), O_WRONLY);
            if (fd >= 0) {
                memset(dedup_info, 0, dedup_info_size);
                dedup_info->src_offset = base_offset;
                dedup_info->src_length = block_size;
                dedup_info->dest_count = 1;
                dedup_info->info[0].dest_fd = fd;
                dedup_info->info[0].dest_offset = off;
                int r = ioctl(base_fd, FIDEDUPERANGE, dedup_info);
                printf("%d %llx %llx\n", r, dedup_info->info[0].status, dedup_info->info[0].bytes_deduped);
                if (r == -1) {
                    printf("%s\n", strerror(errno));
                }
                close(fd);
            }
        }
    });
    if (base_fd >= 0) close(base_fd);
    free(dedup_info);
}