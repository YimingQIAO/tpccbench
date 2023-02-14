#ifndef TPCC_DISK_STORAGE_H
#define TPCC_DISK_STORAGE_H

#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <stdexcept>

#define BLOCKSIZE 4096
#define DIRECT_IO_BUFFER_SIZE 4096

namespace {
    void *direct_io_buffer = aligned_alloc(BLOCKSIZE, DIRECT_IO_BUFFER_SIZE);
}

__attribute__((__used__)) static inline int
DirectIOFile(const std::string &table_name) {
#if __APPLE__
    int fd = open(table_name.c_str(), O_RDWR | O_CREAT, 0666);
    fcntl(fd, F_NOCACHE, 1);
#elif __linux__
    int fd = open(table_name.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0666);
#endif
    if (fd < 0)
        throw std::runtime_error("open file error in DirectIOFile");
    return fd;
}

template<typename T>
static inline void DiskTupleWrite(int fd, T *data, int64_t pos) {
    if (sizeof(T) > DIRECT_IO_BUFFER_SIZE)
        throw std::runtime_error("direct io buffer size is less than sizeof(T) in DiskTupleWrite");

    memcpy(direct_io_buffer, data, sizeof(T));
    int32_t num_page = sizeof(T) / BLOCKSIZE + 1;
    int ret = pwrite(fd, direct_io_buffer, num_page * BLOCKSIZE, pos * num_page * BLOCKSIZE);
    if (ret < 0) throw std::runtime_error("write error in DiskTupleWrite");
}

template<typename T>
static inline void DiskTupleWrite(int fd, T *data) {
    if (sizeof(T) > DIRECT_IO_BUFFER_SIZE)
        throw std::runtime_error("direct io buffer size is less than sizeof(T) in DiskTupleWrite");

    memcpy(direct_io_buffer, data, sizeof(T));
    int32_t num_page = sizeof(T) / BLOCKSIZE + 1;
    int64_t ret = write(fd, direct_io_buffer, num_page * BLOCKSIZE);
    if (ret < 0) throw std::runtime_error("write error in DiskTupleWrite");
}

template<typename T>
static inline int64_t DiskTupleRead(int fd, T *data, int64_t pos) {
    int32_t num_page = sizeof(T) / BLOCKSIZE + 1;
    int64_t ret = pread(fd, direct_io_buffer, num_page * BLOCKSIZE, pos * num_page * BLOCKSIZE);
    if (ret < 0)
        throw std::runtime_error("read error in DiskTupleWrite");
    else {
        memcpy(data, direct_io_buffer, sizeof(T));
        return sizeof(T);
    }
}

template<typename T>
static inline int64_t DiskTableSize(int fd) {
    // get #tuple
    struct stat st_buf;
    if (fstat(fd, &st_buf) == -1) {
        switch (errno) {
            case EBADF:
                throw std::runtime_error("fd is not a valid file descriptor");
            case EFAULT:
                throw std::runtime_error(
                        "std buffer is outside of your accessible address space");
            default:
                throw std::runtime_error("fstat error in DiskTableSize");
        }
    } else {
        int32_t tuple_size = st_buf.st_size / sizeof(T);

        // get size of each tuple
        int64_t disk_size = 0;
        T tuple;
        for (int i = 0; i < tuple_size; ++i) {
            int s = DiskTupleRead(fd, &tuple, i);
            if (s != sizeof(T))
                throw std::runtime_error("read error in DiskTableSize");
            disk_size += tuple.size();
        }

        return disk_size;
    }
}

template<typename T>
static inline int64_t DiskTableSize(const std::string &file_name) {
    int fd = DirectIOFile(file_name);
    return DiskTableSize<T>(fd);
}

template<class T>
struct Tuple {
    bool in_memory_;
    T data_;      // in memory
    int64_t pos_; // on disk

    Tuple() : in_memory_(false), data_(), pos_(-1) {}
};

#endif // TPCC_DISK_STORAGE_H
