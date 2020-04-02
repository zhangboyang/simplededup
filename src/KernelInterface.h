#pragma once

#include <string>
#include <functional>
#include <map>
#include <vector>

class KernelInterface {
    size_t dedup_info_size;
    struct file_dedupe_range *dedup_info;
    //class X;
    //std::map<std::string, std::pair<int, X>> fd_queue;
    const char *getError(int e);

public:
    KernelInterface();
    ~KernelInterface();
    
    bool getFileBlocks(const std::string &file_name, int block_size, std::function<void(uint64_t file_size)> info_callback, std::function<void(uint64_t physical_off, uint64_t logical_off, std::function<char *()> read_data)> iter_callback);
    uint64_t dedupRange(int src_fd, uint64_t src_offset, uint64_t range_length, std::vector<std::tuple<int/*dest_fd*/, uint64_t/*dest_offset*/, uint64_t/*out_result*/>> &targets);

    int getFD(const std::string &file_name);
    void releaseFD(int &fd);
    void switchFD(int &fd, const std::string &file_name);
};
