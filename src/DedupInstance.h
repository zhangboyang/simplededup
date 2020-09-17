#pragma once

#include "BitVector.h"
#include "HashStorage.h"
#include "KernelInterface.h"

class DedupInstance {
    struct FileItem {
        std::string file_name;
        uint64_t size = 0;
        uint64_t logical_id_base = 0;

        int fd = -1;

        std::any opened_it; // opened_file::iterator

        FileItem() {}
        FileItem(const std::string &_file_name) : file_name(_file_name) {}
        friend bool operator < (const DedupInstance::FileItem &lhs, const DedupInstance::FileItem &rhs)
        {
            return lhs.file_name < rhs.file_name;
        }
    };
    
    std::vector<FileItem> file_list;

    uint64_t n_logical_id = 0;

    uint64_t physical_blocks = 0;
    uint64_t ignored_blocks = 0;
    uint64_t hashed_blocks = 0;
    uint64_t shared_blocks = 0;
    uint64_t unique_blocks = 0;

    std::list<std::vector<FileItem>::iterator> opened_file;

    int getFD(std::vector<FileItem>::iterator f);
    std::vector<FileItem>::iterator getFileItemByLogicalID(uint64_t logical_id);

    void hashFiles();
    void iterateGroups(std::function<void(std::vector<uint64_t/*logical_id*/> &group)> group_callback);
    void submitDuplicate();
    void relocateUnique();

    void truncateChunkStore();
    void allocChunkBlock();

    void resetProgress();
    bool shouldPrintProgress();

public:
    ~DedupInstance();

    HashStorage hash_storage;

    std::string chunk_file = "chunkstorage.tmp";
    uint64_t block_size = 4096; // fs block size
    uint64_t ref_limit = 500; // max reference to a single block

    int tmp_fd = -1;
    uint64_t tmp_off = 0;
    uint64_t chunk_limit = 128 * 1048576;

    time_t next_progress;

    void addFile(const std::string &file_name);
    void doDedup();
};