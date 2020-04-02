#include <cstdio>
#include <cstring>
#include <cstdint>
#include <cstdlib>

union DataBlock {
    int64_t id;
    char raw[4096];
};

const int64_t N = 100000;

int main()
{
    FILE *fp;
    DataBlock data;
    
    system("rm -f testdata.*");
    
    memset(&data, 0, sizeof(data));
    
    // 0, 1, 2 ... N
    fp = fopen("testdata.1", "wb");
    for (int64_t i = 0; i < N; i++) {
        data.id = i;
        fwrite(&data, sizeof(data), 1, fp);
    }
    fclose(fp);
    
    // N, N - 1, N - 2 ... 0
    fp = fopen("testdata.2", "wb");
    for (int64_t i = N - 1; i >= 0; i--) {
        data.id = i;
        fwrite(&data, sizeof(data), 1, fp);
    }
    fclose(fp);
    
    // a hole in file
    fp = fopen("testdata.3", "wb");
    for (int64_t i = 0; i < N; i++) {
        fseek(fp, i * sizeof(data), SEEK_SET);
        if (i < N / 3 || i >= N * 2 / 3) {
            data.id = i;
            fwrite(&data, sizeof(data), 1, fp);
        }
    }
    fclose(fp);
    
    // 0, 1, 2 ... N
    fp = fopen("testdata.4", "wb");
    for (int64_t i = 0; i < N; i++) {
        data.id = i;
        fwrite(&data, sizeof(data), 1, fp);
    }
    fputs("somedata", fp);
    fclose(fp);
    
    // reflink
    system("cp --reflink=always testdata.1 testdata.5");
    
    // reflink and modify
    system("cp --reflink=always testdata.1 testdata.6");
    fp = fopen("testdata.6", "r+b");
    for (int64_t i = 0; i < N; i += 1000) {
        data.id = i * 10;
        fwrite(&data, sizeof(data), 1, fp);
    }
    fclose(fp);
    
    system("sync testdata.*");
    
    system("sha1sum testdata.*");
    
    system("btrfs fi du testdata.*");
    
    return 0;
}
