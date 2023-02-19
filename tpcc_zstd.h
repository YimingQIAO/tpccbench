#ifndef TPCC_TPCC_ZSTD_H
#define TPCC_TPCC_ZSTD_H

#include "zstd.h"
#include "common.h"
#include "zdict.h"

#include <string>

namespace {
    ZSTD_CCtx *const cctx = ZSTD_createCCtx();
    ZSTD_DCtx *const dctx = ZSTD_createDCtx();
}

template<typename T>
static std::string ZstdCompress(ZSTD_CDict_s *cdict, T *src) {
    size_t code_capacity = ZSTD_compressBound(sizeof(T));
    std::string code_buffer(code_capacity, '\0');
    size_t code_size = ZSTD_compress_usingCDict(cctx, code_buffer.data(), code_capacity,
                                                src, sizeof(T), cdict);
    return code_buffer.substr(0, code_size);
}

template<typename T>
static void ZstdDecompress(ZSTD_DDict_s *ddict, T *data, std::string &src) {
    int ret = ZSTD_decompress_usingDDict(dctx, data, sizeof(T), src.data(), src.size(), ddict);
    if (ret < 0) {
        CHECK_ZSTD(ret);
    }
}

#endif //TPCC_TPCC_ZSTD_H
