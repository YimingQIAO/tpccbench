#include <chrono>
#include <iostream>
#include <sys/fcntl.h>

int main(int argc, const char *argv[]) {
    int fd = open("test.txt", O_RDWR | O_CREAT);
    fcntl(fd, F_NOCACHE, 1);
    FILE *file = fdopen(fd, "r+");
    for (int i = 0; i < 100000; ++i) {
        int random = rand() % 100;
        fprintf(file, "Hello World %d\t", random);
    }
    fseek(file, 0, SEEK_SET);
    char *data = new char[512];
    int size = 0;
    auto beg = std::chrono::high_resolution_clock::now();
    while (fread(data, 1, 512, file) > 0) {
        size += 512;
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "File Size: " << size << " bytes" << std::endl;
    uint64_t time = std::chrono::duration_cast<std::chrono::nanoseconds>(end - beg).count();
    double throughput = size / 1e6 / (time / 1e9);
    std::cout << "Throughput: " << throughput << " MB/s" << std::endl;

    return 0;
}
