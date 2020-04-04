#include "config.h"

#include <cstring>
#include <cstdlib>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <unistd.h>
#include <linux/fs.h>
#include <linux/fiemap.h>

#include "KernelInterface.h"


const char *KernelInterface::getError(int e)
{
    return strerror(e); // not thread-safe
}

void KernelInterface::getFileBlocks(const std::string &file_name, int block_size, std::function<void(uint64_t file_size)> info_callback, std::function<void(uint64_t physical_off, uint64_t logical_off, uint64_t data_size, std::function<char *()> read_data)> iter_callback)
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
            uint64_t data_size = std::min((uint64_t)(sb.st_size - (e->fe_logical + off)), (uint64_t) block_size);
            
            iter_callback(e->fe_physical + off, e->fe_logical + off, data_size, [&]()-> char *{
                if (lseek(fd, e->fe_logical + off, SEEK_SET) == -1) {
                    printf("warning: '%s' lseek failed, block ignored. (%s)\n", file_str, getError(errno));
                    return nullptr;
                }
                if (read(fd, buffer, data_size) != data_size) {
                    printf("warning: '%s' read failed, block ignored. (%s)\n", file_str, getError(errno));
                    return nullptr;
                }
                return buffer;
            });
        }
    }


fail:
    if (fd != -1) close(fd);
    if (mapdata) free(mapdata);
    if (buffer) free(buffer);
}

void KernelInterface::dedupRange(int src_fd, uint64_t src_offset, uint64_t range_length, std::vector<std::tuple<int/*dest_fd*/, uint64_t/*dest_offset*/, uint64_t/*out_result*/>> &targets)
{
    char buffer[4096]; /* page size */
    static_assert(sizeof(struct file_dedupe_range) + sizeof(struct file_dedupe_range_info) <= sizeof(buffer));
    const size_t max_batch_size = (sizeof(buffer) - sizeof(struct file_dedupe_range)) / sizeof(struct file_dedupe_range_info);
    const size_t dedup_info_size = sizeof(struct file_dedupe_range) + sizeof(struct file_dedupe_range_info) * max_batch_size;
    struct file_dedupe_range *dedup_info = (struct file_dedupe_range *) buffer;
    
    for (size_t i = 0; i < targets.size(); i += max_batch_size) {
        memset(dedup_info, 0, dedup_info_size);
        dedup_info->src_offset = src_offset;
        dedup_info->src_length = range_length;

        size_t batch_size = std::min(targets.size() - i, max_batch_size);
        dedup_info->dest_count = batch_size;

        for (size_t j = i; j < i + batch_size; j++) {
            auto &[dest_fd, dest_offset, out_result] = targets[j];
            out_result = -1;
            dedup_info->info[j - i].dest_fd = dest_fd;
            dedup_info->info[j - i].dest_offset = dest_offset;
        }

        int r = ioctl(src_fd, FIDEDUPERANGE, dedup_info);
        if (r == -1) {
            printf("error: ioctl FIDEDUPERANGE failed. (%s)\n", getError(errno));
        } else {
            for (size_t j = i; j < i + batch_size; j++) {
                auto &[dest_fd, dest_offset, out_result] = targets[j];
                if (dedup_info->info[j - i].status == FILE_DEDUPE_RANGE_SAME) {
                    out_result = dedup_info->info[j - i].bytes_deduped;
                }
            }
        }
    }
}

void KernelInterface::setMaxFD(int n)
{
    struct rlimit rlim;
    
    if (getrlimit(RLIMIT_NOFILE, &rlim) == -1) {
        printf("error: can't set max file descriptor to %d, getrlimit() failed. (%s)\n", n, getError(errno));
        return;
    }
    
    rlim.rlim_cur = n;
    
    if (setrlimit(RLIMIT_NOFILE, &rlim) == -1) {
        printf("error: can't set max file descriptor to %d, setrlimit() failed. (%s)\n", n, getError(errno));
        return;
    }

    printf("max file descriptor set to %d/%d (soft/hard).\n", (int) rlim.rlim_cur, (int) rlim.rlim_max);
    printf("\n");
}

int KernelInterface::openFD(const std::string &file_name)
{
    auto file_str = file_name.c_str();
    int fd = open(file_str, O_RDWR);
    if (fd == -1) {
        printf("error: can't open '%s'. (%s)\n", file_str, getError(errno));
    }
    return fd;
}
void KernelInterface::closeFD(int fd)
{
    if (fd >= 0) {
        close(fd);
    }
}