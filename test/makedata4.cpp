#include <cstdio>
#include <cstring>
#include <cstdint>
#include <cstdlib>

union DataBlock {
    int64_t id;
    char raw[4096];
};

const int64_t N = 1024;

int main()
{
    FILE *fp;
    DataBlock data;
    
    system("rm -f testdata.*");
    
    memset(&data, 0, sizeof(data));
    /*fp = fopen("/dev/urandom", "rb");
    fread(&data, sizeof(data), 1, fp);
    fclose(fp);*/
    
    // 0, 1, 2 ... N
    fp = fopen("testdata.1", "wb");
    for (int64_t i = 0; i < N; i++) {
        data.id = i;
        fwrite(&data, sizeof(data), 1, fp);
    }
    fclose(fp);
    
    // empty file
    fp = fopen("testdata.2", "wb");
    fclose(fp);
    
    // 0, 1, 2 ... N
    fp = fopen("testdata.3", "wb");
    for (int64_t i = 0; i < N; i++) {
        data.id = i;
        fwrite(&data, sizeof(data), 1, fp);
    }
    fclose(fp);
    
    system("sync testdata.*");
    
    system("sha1sum testdata.*");
    
    system("btrfs fi du --raw testdata.*");
    
    return 0;
}
