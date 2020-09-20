# simplededup

Yet another block-level btrfs deduplication tool. Simplededup is an offline deduplicator, it will read every block of given files and eliminate duplicate blocks. It uses FIEMAP and FIDEDUPERANGE to submit same blocks to kernel, so it may also compatible with other filesystems.

## Advantages

* **Accept file lists**: You can select which files to dedupe.
* **Optimized for HDDs**: Simplededup will try best to reduce random disk seeks.
* **Real dedupe operation offloaded to kernel**: Bugs in simplededup are unlikely to hurt your files.
* **Works with large data**: Temporary data is saved to disk instead of RAM.
* May compatible with other filesystems.

## Disadvantages

* **No incremental dedupe support**: Simplededup will read & write whole data in each run.
* **Huge amount of writes to disk**: Simplededup will relocate all your files, so may not be suitable for SSDs.
* **Not integrated with btrfs**: Simplededup is not aware of advance features of btrfs such as snapshots.
* **Dedupe granularity can't be set**: Dedupe granularity must equals to filesystem blocksize.

## Requirements

* A filesystem with FIEMAP and FIDEDUPERANGE support. (Only btrfs is tested yet)
* All your files can be read in reasonable time. (e.g. You don't have a 1TB file reflinked 1000 times)
* **RAM**: block_bitmap (32MB per TB) + sort_buffer (default 600MB); actual usage may higher due to C++ memory allocation policy.
* **Disk**: 4.4GB per TB for temporary hash storage, and free space for relocating existing data (the more the better).

## Gotchas

* Metadata usage will **increase** after deduplication.
* Not all freed space is really freed.

## Build & Installation

1. Build [xxHash](https://github.com/Cyan4973/xxHash) first

```sh
cd /your/build/dir
git clone https://github.com/Cyan4973/xxHash.git
cd xxHash
mkdir build
cd build
cmake ../cmake_unofficial -DBUILD_SHARED_LIBS=OFF
make
```

2. Build simplededup

```sh
cd /your/build/dir
git clone https://github.com/zhangboyang/simplededup.git
cd simplededup
cd src
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
# the compiled binary should be 'simplededup'
```

## Usage

* Simply pipe a NUL-delimited file list to simplededup, and simplededup will dedupe them.

```sh
cd /path/to/dedup
find /path/to/dedup -type f -print0 | ./simplededup
```

* Options can be altered by command line, use `--help` to get details.

```sh
./simplededup --help
```

## Algorithm

The algorithm is very simple. First, hash all data blocks and use external merge-sort to sort all hashes. Then, group blocks which have same hash. For each set of same blocks, copy to a temp file and use FIDEDUPERANGE to deduplicate them. For each unique block, also copy to a temp file and use FIDEDUPERANGE to relocate them. The data is copied because of [this problem](https://lore.kernel.org/linux-btrfs/66ea94f5-ba6b-da68-7d6b-c422b66f058d@gmail.com/).

## Other similar tools

* [duperemove](https://github.com/markfasheh/duperemove)
* [BEES](https://github.com/Zygo/bees)
