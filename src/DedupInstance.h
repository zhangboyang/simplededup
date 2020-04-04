#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <any>

#include "HashStorage.h"
#include "BitVector.h"
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
    HashStorage hash_storage;

    BitVector physical_hashed;

    uint64_t n_logical_id = 0;
    BitVector logical_deduped;

    uint64_t lonely_blocks = 0;
    uint64_t popular_blocks = 0;
    uint64_t hotspot_blocks = 0;
    uint64_t overref_blocks = 0;
    uint64_t ignored_blocks = 0;
    uint64_t dupe_blocks = 0;
    
    uint64_t reref_bytes = 0;

    std::list<std::vector<FileItem>::iterator> opened_file;

    int getFD(std::vector<FileItem>::iterator f);

    void hashFiles();
    void groupBlocks();
    void calcTargets();
    void submitRanges();

public:
    ~DedupInstance();

    uint64_t block_size = 4096; // fs block size
    uint64_t ref_limit = 500; // max reference to a single block

    void addFile(const std::string &file_name);
    void doDedup();
};