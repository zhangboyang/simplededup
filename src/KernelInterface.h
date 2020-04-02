#pragma once

#include <string>
#include <functional>
#include <map>
#include <list>
#include <vector>

class KernelInterface {
    size_t dedup_info_size;
    struct file_dedupe_range *dedup_info;
    
    const char *getError(int e);

public:
    int max_fd = 0;

    KernelInterface();
    ~KernelInterface();
    
    void getFileBlocks(const std::string &file_name, int block_size, std::function<void(uint64_t file_size)> info_callback, std::function<void(uint64_t physical_off, uint64_t logical_off, std::function<char *()> read_data)> iter_callback);
    void dedupRange(int src_fd, uint64_t src_offset, uint64_t range_length, std::vector<std::tuple<int/*dest_fd*/, uint64_t/*dest_offset*/, uint64_t/*out_result*/>> &targets);

    int openFD(const std::string &file_name);
    void closeFD(int fd);
};
