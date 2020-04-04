#include "config.h"
#include <cstdio>
#include <cstring>

#include "DedupInstance.h"

void _verify(bool cond, const char *file, int line, const char *func, const char *expr)
{
    if (!cond) {
        int errsv = errno;
        printf("[assertion failed: %s]\n", expr);
        printf("file: %s\n", file);
        printf("line: %d\n", line);
        printf("function: %s\n", func);
        printf("errno: %s\n", strerror(errsv));
        printf("\n");
        fflush(stdout);
        abort();
    }
}

int main()
{
    printf("simplededup v%d.%d\n\n", SIMPLEDEDUP_VERSION_MAJOR, SIMPLEDEDUP_VERSION_MINOR);

    DedupInstance d;
    
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
    printf("\n");
    
    // do dedup
    d.doDedup();

    return 0;
}
