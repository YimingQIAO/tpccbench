#include <cstdio>
#include <chrono>
#include <string.h>

#include "disk_storage.h"

struct Person {
    int a;
    int b;
    char c[256];

    int32_t size() const {
        return sizeof(a) + sizeof(b) + strlen(c);
    }
};

int main(int argc, const char *argv[]) {
    Person p;
    snprintf(p.c, 256, "hello world");

    int num = 100000;
    int fd = DirectIOFile("test.txt");
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num; ++i) {
        p.a = i;
        p.b = i * 3;
        SeqDiskTupleWrite(fd, &p);
    }
    close(fd);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    printf("%d ms\n", int(duration / 1e3));

    Person p2;
    fd = DirectIOFile("test.txt");
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num; ++i) {
        DiskTupleRead(fd, &p2, i);
    }
    close(fd);
    end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    printf("%d ms\n", int(duration / 1e3));

    fd = DirectIOFile("test.txt");
    printf("Size: %ld byte\n", DiskTableSize<Person>(fd));
    remove("test.txt");
    return 0;
}