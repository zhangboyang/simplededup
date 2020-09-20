#include "config.h"

#include <unistd.h>
#include <getopt.h>

#include "DedupInstance.h"

void _verify(bool cond, const char *file, int line, const char *func, const char *expr)
{
    if (!cond) {
        int errsv = errno;
        LOG("ASSERTION FAILED: %s]\n", expr);
        LOG("file: %s\n", file);
        LOG("line: %d\n", line);
        LOG("function: %s\n", func);
        LOG("errno: %s\n", strerror(errsv)); // not thread-safe
        LOG("\n");
        fflush(stdout);
        abort();
    }
}

std::string _logtime()
{
    time_t result = time(nullptr);
    std::string t(ctime(&result)); // not thread-safe
    t.pop_back();
    return t;
}

static bool str2u64(uint64_t &dst, const char *str)
{
    char *p;
    errno = 0;
    uint64_t value = strtoull(str, &p, 10);
    if (p == str || *p || errno) return false;
    dst = value;
    return true;
}

static std::string build_help(int argc, char *argv[], DedupInstance &d)
{
    std::string hlp;
    char buf[4096];
    /* begin */ sprintf(buf, "Yet another file system deduplication tool.\n");
    hlp += buf; sprintf(buf, "\n");
    hlp += buf; sprintf(buf, "Usage:\n");
    hlp += buf; sprintf(buf, "\n");
    hlp += buf; sprintf(buf, "  find /path/to/dedup -type f -print0 | %s [OPTIONS]\n", argv[0]);
    hlp += buf; sprintf(buf, "\n");
    hlp += buf; sprintf(buf, "Options:\n");
    hlp += buf; sprintf(buf, "\n");
    hlp += buf; sprintf(buf, "  -s, --hash-file        Temporary hash storage path\n"
                             "                           [default: %s.XXXX]\n", d.hash_storage.stor_path.c_str());
    hlp += buf; sprintf(buf, "  -c, --chunk-file       Temporary chunk storage path\n"
                             "                           [default: %s]\n", d.chunk_file.c_str());
    hlp += buf; sprintf(buf, "  -t, --temp-size        Temporary chunk storage size in bytes\n"
                             "                           [default: %" PRIu64 "]\n", d.chunk_limit);
    hlp += buf; sprintf(buf, "  -m, --sort-mem         Sort buffer size in MiB\n"
                             "                           [default: %" PRIu64 "]  (hint: set this to about 1/3 of RAM size)\n", d.hash_storage.sort_mem);
    hlp += buf; sprintf(buf, "  -r, --ref-limit        Max references to a single block\n"
                             "                           [default: %" PRIu64 "]\n", d.ref_limit);
    hlp += buf; sprintf(buf, "  -b, --block-size       File system block size in bytes\n"
                             "                           [default: %" PRIu64 "]\n", d.block_size);
    hlp += buf; sprintf(buf, "  -u, --relocate-unique  Relocate unique data blocks to free more space\n"
                             "                           [default: %" PRIu64 "]  (hint: set to 0 to disable relocate)\n", d.relocate_enable);
    hlp += buf; sprintf(buf, "\n");
    hlp += buf; /* end */
    return hlp;
}

int main(int argc, char *argv[])
{
    printf("\n");
    printf("simplededup v%d.%d\n", SIMPLEDEDUP_VERSION_MAJOR, SIMPLEDEDUP_VERSION_MINOR);
    printf("https://github.com/zhangboyang/simplededup\n");
    printf("\n");

    DedupInstance d;

    std::string hlp = build_help(argc, argv, d);

    while (1) {
        static struct option long_options[] = {
            {"hash-file", required_argument, 0, 's'},
            {"chunk-file", required_argument, 0, 'c'},
            {"temp-size", required_argument, 0, 't'},
            {"sort-mem", required_argument, 0, 'm'},
            {"ref-limit", required_argument, 0, 'r'},
            {"block-size", required_argument, 0, 'b'},
            {"relocate-unique", required_argument, 0, 'u'},
            {"help", no_argument, 0, 'h'},
            { /* end of options */ }
        };
        int c = getopt_long(argc, argv, "s:c:t:m:r:b:u:h", long_options, NULL);
        if (c == -1) break;
        char *p;
        switch (c) {

        case 's':
            d.hash_storage.stor_path = std::string(optarg);
            break;
        case 'c':
            d.chunk_file = std::string(optarg);
            break;
        case 't':
            if (!str2u64(d.chunk_limit, optarg)) goto bad_number;
            break;
        case 'm':
            if (!str2u64(d.hash_storage.sort_mem, optarg)) goto bad_number;
            break;
        case 'r':
            if (!str2u64(d.ref_limit, optarg)) goto bad_number;
            break;
        case 'b':
            if (!str2u64(d.block_size, optarg)) goto bad_number;
            break;
        case 'u':
            if (!str2u64(d.relocate_enable, optarg)) goto bad_number;
            break;
        bad_number:
            printf("error: bad number '%s'.\n", optarg);
            goto show_help;

        default:
            printf("\n");
            goto show_help;

        show_help:
        case 'h':
            printf("%s", hlp.c_str());
            return 1;
        }
    }
    
    if (isatty(0)) {
        printf("please pipe a NUL-delimited file list to me.\n");
        printf("use '--help' to get usage information.\n");
        printf("\n");
        return 1;
    }

    // read file names
    //   use 'find . -type f -print0' to create a file list
    int ch;
    std::string filename;
    while ((ch = getchar()) != EOF) {
        if (ch) {
            filename.push_back(ch);
        } else {
            d.addFile(filename);
            filename.clear();
        }
    }

    // set max opened file descriptors
    KernelInterface::setMaxFD(d.ref_limit + 2500);
    LOG("\n");
    
    // do dedup
    d.doDedup();

    printf("\n");
    return 0;
}
