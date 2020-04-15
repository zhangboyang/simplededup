# simplededup

Yet another block-level btrfs deduplication tool. Simplededup is an offline deduplicator, it will read every block of given files and eliminate duplicate blocks. It uses FIEMAP and FIDEDUPERANGE to submit same blocks to kernel, so it may also compatible with other filesystems.

## Advantages

* **Accept file lists**: You can select which files to dedupe.
* **Optimized for HDDs**: Simplededup will try best to reduce random disk seeks.
* **Real dedupe operation offloaded to kernel**: Bugs in simplededup won't hurt your files.
* **Works with large data**: Temporary data is saved to disk instead of RAM.
* May compatible with other filesystems.

## Disadvantages

* **No incremental dedupe support**: Simplededup will read whole data in each run.
* **Not integrated with btrfs**: Simplededup is not aware of advance features of btrfs such as snapshots.
* **Dedupe granularity can't be set yet**: Dedupe granularity equals to filesystem blocksize, which can't be changed yet.

## Requirements

* A filesystem with FIEMAP and FIDEDUPERANGE support. (Only btrfs is tested yet)
* Your files can be read in reasonable time. (e.g. You don't have a 1TB file reflinked 1000 times)
* **RAM**: block_bitmap (32MB per TB) + sort_buffer (default 600MB); actual usage may higher due to C++ memory allocation policy.
* **Disk**: 4GB~7GB per TB (typically 5GB per TB).

## Gotchas

* Metadata usage may **increase** after deduplication.
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
find /path/to/dedup -type f -print0 | ./simplededup
```

* Options can be altered by command line, use `--help` to get details.

```sh
./simplededup --help
```

## Algorithm

TODO

## Other similar tools

* [duperemove](https://github.com/markfasheh/duperemove)
* [BEES](https://github.com/Zygo/bees)
