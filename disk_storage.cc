#include "disk_storage.h"

void DiskTable::TupleWrite(const char *data, size_t size) const {
    write(file_, data, size);
}

void DiskTable::TupleRead(char *data, size_t size) const {
    read(file_, data, size);
}

void DiskTable::seek(int offset) const {
    lseek(file_, offset, SEEK_SET);
}
