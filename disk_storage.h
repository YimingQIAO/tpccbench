#ifndef TPCC_DISK_STORAGE_H
#define TPCC_DISK_STORAGE_H

#include <string>
#include <utility>
#include <fcntl.h>
#include <libc.h>


class DiskTable {
public:
    std::string table_name_;
    int file_;

    explicit DiskTable(std::string table_name) : table_name_(std::move(table_name)) {
        file_ = open(table_name_.c_str(), O_RDWR | O_CREAT);
    }

    ~DiskTable() {
        close(file_);
    }

    void TupleWrite(const char *data, size_t size) const;

    void TupleRead(char *data, size_t size) const;

    void seek(int offset) const;

private:
};


#endif //TPCC_DISK_STORAGE_H
