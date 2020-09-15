#pragma once

#include <fcntl.h>

class KernelInterface {
public:

    static const char *getError(int e);
    
    static bool getFileBlocks(const std::string &file_name, int block_size, std::function<void(uint64_t file_size)> info_callback, std::function<void(uint64_t physical_off, uint64_t logical_off, uint64_t data_size, std::function<char *()> read_data)> iter_callback);

    static bool copyRange(int dst_fd, uint64_t dst_off, int src_fd, uint64_t src_off, uint64_t length);

    static void dummyRead(int fd, uint64_t offset, uint64_t length);
    static void dedupRange(int src_fd, uint64_t src_offset, uint64_t range_length, std::vector<std::tuple<int/*dest_fd*/, uint64_t/*dest_offset*/, uint64_t/*out_result*/>> &targets);

    static void setMaxFD(int n);
    static int openFD(const std::string &file_name, int flags = O_RDWR);
    static void closeFD(int fd);

};
