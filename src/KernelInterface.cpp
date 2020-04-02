#include "config.h"

#include <cstring>
#include <cstdlib>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <linux/fs.h>
#include <linux/fiemap.h>

#include "KernelInterface.h"


KernelInterface::KernelInterface()
{
    dedup_info_size = sizeof(struct file_dedupe_range) + sizeof(struct file_dedupe_range_info);
    dedup_info = (struct file_dedupe_range *) malloc(dedup_info_size);
}
KernelInterface::~KernelInterface()
{
    free(dedup_info);
}

const char *KernelInterface::getError(int e)
{
    return strerror(e); // not thread-safe
}

bool KernelInterface::getFileBlocks(const std::string &file_name, int block_size, std::function<void(uint64_t file_size)> info_callback, std::function<void(uint64_t physical_off, uint64_t logical_off, std::function<char *()> read_data)> iter_callback)
{
    auto file_str = file_name.c_str();
    struct stat sb;
    struct fiemap mapprobe;
    struct fiemap *mapdata = NULL;
    char *buffer = (char *) malloc(block_size);
    int fd = -1;
    int r;
    bool success = false;
    size_t array_bytes;

    if (lstat(file_str, &sb) == -1) {
        printf("error: can't lstat '%s', file ignored. (%s)\n", file_str, getError(errno));
        goto fail;
    }

    if ((sb.st_mode & S_IFMT) != S_IFREG) {
        printf("error: '%s' is not a regular file, file ignored.\n", file_str);
        goto fail;
    }

    fd = open(file_str, O_RDONLY); // ignore race cond between lstat() and open()
    if (fd == -1) {
        printf("error: can't open '%s', file ignored. (%s)\n", file_str, getError(errno));
        goto fail;
    }

    memset(&mapprobe, 0, sizeof(mapprobe));
    mapprobe.fm_start = 0;
    mapprobe.fm_length = sb.st_size;
    mapprobe.fm_flags = FIEMAP_FLAG_SYNC;
    mapprobe.fm_extent_count = 0;

    r = ioctl(fd, FS_IOC_FIEMAP, &mapprobe);
    if (r < 0) {
        printf("error: '%s' fiemap failed, file ignored. (%s)\n", file_str, getError(errno));
        goto fail;
    }

    array_bytes = sizeof(struct fiemap_extent) * mapprobe.fm_mapped_extents; // ignore integer overflow
    mapdata = (struct fiemap *) malloc(sizeof(struct fiemap) + array_bytes);
    if (!mapdata) {
        printf("error: can't alloc memory for fiemap '%s', file ignored. (%s)\n", file_str, getError(errno));
        goto fail;
    }
    memset(mapdata, 0, sizeof(struct fiemap) + array_bytes);
    mapdata->fm_start = 0;
    mapdata->fm_length = sb.st_size;
    mapdata->fm_flags = FIEMAP_FLAG_SYNC;
    mapdata->fm_extent_count = mapprobe.fm_mapped_extents;

    r = ioctl(fd, FS_IOC_FIEMAP, mapdata);
    if (r < 0) {
        printf("error: '%s' fiemap failed, file ignored. (%s)\n", file_str, getError(errno));
        goto fail;
    }

    info_callback(sb.st_size);

    for (uint64_t i = 0; i < mapdata->fm_mapped_extents; i++) {
        auto e = &mapdata->fm_extents[i];
        //printf("%x %llx %llx %llx\n", e->fe_flags, e->fe_logical, e->fe_physical, e->fe_length);
        if (e->fe_flags & FIEMAP_EXTENT_NOT_ALIGNED) continue;
        if (e->fe_logical % block_size != 0 || e->fe_physical % block_size != 0 || e->fe_length % block_size != 0) {
            printf("warning: '%s' extents not aligned, extents ignored.\n", file_str);
            continue;
        }
        for (uint64_t off = 0; off < e->fe_length; off += block_size) {
            if (sb.st_size - (e->fe_logical + off) < block_size) {
                // ignore last unaligned part of file
                continue;
            }

            iter_callback(e->fe_physical + off, e->fe_logical + off, [&]()-> char *{
                if (lseek(fd, e->fe_logical + off, SEEK_SET) == -1) {
                    printf("warning: '%s' lseek failed, block ignored. (%s)\n", file_str, getError(errno));
                    return nullptr;
                }
                if (read(fd, buffer, block_size) != block_size) {
                    printf("warning: '%s' read failed, block ignored. (%s)\n", file_str, getError(errno));
                    return nullptr;
                }
                return buffer;
            });
        }
    }


    success = true;
fail:
    if (fd != -1) close(fd);
    if (mapdata) free(mapdata);
    if (buffer) free(buffer);
    return success;
}

uint64_t KernelInterface::dedupRange(int src_fd, uint64_t src_offset, uint64_t range_length, std::vector<std::tuple<int/*dest_fd*/, uint64_t/*dest_offset*/, uint64_t/*out_result*/>> &targets)
{
    uint64_t total_dedup = 0;

    for (auto &dedup_item: targets) {
        int dest_fd; uint64_t dest_offset;
        uint64_t out_result = -1;
        std::tie(dest_fd, dest_offset, std::ignore) = dedup_item;

        memset(dedup_info, 0, dedup_info_size);
        dedup_info->src_offset = src_offset;
        dedup_info->src_length = range_length;
        dedup_info->dest_count = 1;
        dedup_info->info[0].dest_fd = dest_fd;
        dedup_info->info[0].dest_offset = dest_offset;

        int r = ioctl(src_fd, FIDEDUPERANGE, dedup_info);
        if (r == -1) {
            printf("error: ioctl FIDEDUPERANGE failed. (%s)\n", getError(errno));
        } else {
            if (dedup_info->info[0].status == FILE_DEDUPE_RANGE_SAME) {
                total_dedup += (out_result = dedup_info->info[0].bytes_deduped);
            }
        }

        dedup_item = std::make_tuple(dest_fd, dest_offset, out_result);
    }

    return total_dedup;
}

int KernelInterface::getFD(const std::string &file_name)
{
    return open(file_name.c_str(), O_RDWR);
}
void KernelInterface::releaseFD(int &fd)
{
    if (fd >= 0) close(fd);
    fd = -1;
}
void KernelInterface::switchFD(int &fd, const std::string &file_name)
{
    releaseFD(fd);
    fd = getFD(file_name);
}