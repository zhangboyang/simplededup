#include "config.h"
#include <cstdio>

#include "DedupInstance.h"

int main()
{
    printf("simplededup v%d.%d\n", SIMPLEDEDUP_VERSION_MAJOR, SIMPLEDEDUP_VERSION_MINOR);

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

    // do dedup
    d.doDedup();

    return 0;
}
