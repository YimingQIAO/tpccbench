#ifndef TPCC_DISK_STORAGE_H
#define TPCC_DISK_STORAGE_H

#include <string>
#include <utility>
#include <fcntl.h>
#include <libc.h>

__attribute__((__used__))
static inline int DirectIOFile(const std::string &table_name) {
#if __APPLE__
    int fd = open(table_name.c_str(), O_RDWR | O_CREAT, 0666);
    fcntl(fd, F_NOCACHE, 1);
#elif __linux__
    int fd = open(table_name.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0666);
#endif
    if (fd < 0) throw std::runtime_error("open file error in DirectIOFile");
    return fd;
}

template<typename T>
static inline void DiskTupleWrite(int fd, T *data, int64_t pos) {
    int ret = pwrite(fd, data, sizeof(T), pos * sizeof(T));
    if (ret < 0) throw std::runtime_error("write error in DiskTupleWrite");
}

template<typename T>
static inline void DiskTupleWrite(int fd, T *data) {
    int ret = write(fd, data, sizeof(T));
    if (ret < 0) throw std::runtime_error("write error in DiskTupleWrite");
}

template<typename T>
static inline int DiskTupleRead(int fd, T *data, int64_t pos) {
    int ret = pread(fd, data, sizeof(T), pos * sizeof(T));
    if (ret < 0) throw std::runtime_error("read error in DiskTupleRead");
    return ret;
}

template<typename T>
static inline int64_t DiskTableSize(int fd) {
    // get #tuple
    struct stat st_buf;
    fstat(fd, &st_buf);
    int32_t tuple_size = st_buf.st_size / sizeof(T);

    // get size of each tuple
    int64_t ret = 0;
    T tuple;
    for (int i = 0; i < tuple_size; ++i) {
        int s = DiskTupleRead(fd, &tuple, i);
        if (s != sizeof(T)) throw std::runtime_error("read error in DiskTableSize");
        ret += tuple.size();
    }

    return ret;
}

template<typename T>
static inline int64_t DiskTableSize(const std::string &file_name) {
    int fd = DirectIOFile(file_name);
    return DiskTableSize<T>(fd);
}

template<class T>
struct Tuple {
    bool in_memory_;
    T data_;            // in memory
    int64_t pos_;       // on disk

    Tuple() : in_memory_(false), data_(), pos_(-1) {}
};

#endif //TPCC_DISK_STORAGE_H
