#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "HashStorage.h"
#include "BitVector.h"
#include "KernelInterface.h"

class DedupInstance {
    struct FileItem {
        uint64_t id = 0;
        std::string file_name;
        bool ignored = false;
        uint64_t size = 0;
        uint64_t logical_id_base = 0;

        FileItem() {}
        FileItem(const std::string &_file_name) : file_name(_file_name) {}
        friend bool operator < (const DedupInstance::FileItem &lhs, const DedupInstance::FileItem &rhs)
        {
            return lhs.file_name < rhs.file_name;
        }
    };

    KernelInterface kern;

    std::vector<FileItem> file_list;
    HashStorage hash_storage;

    BitVector physical_hashed;

    uint64_t n_logical_id = 0;
    BitVector logical_deduped;

    void hashFiles();
    void calcTargets();
    uint64_t submitRanges();

public:
    uint64_t block_size = 4096; // fs block size

    void addFile(const std::string &file_name);
    void doDedup();
};