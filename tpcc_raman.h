#ifndef TPCC_TPCC_RAMAN_H
#define TPCC_TPCC_RAMAN_H

#include "code_tree.h"
#include "frequency_table.h"
#include "bit_stream.h"
#include "canonical_code.h"
#include "coder.h"

struct RamanCompressor {
    std::vector<raman::FreqStat> stats_;
    std::vector<raman::CodeTree> forest_;

    explicit RamanCompressor(size_t num_fields) : stats_(num_fields) {}

    inline uint32_t Size() {
        uint32_t ret = 0;
        for (auto &stat: stats_) ret += stat.BimapSize();
        for (auto &tree: forest_) ret += tree.Size();
        return ret;
    }
};

static RamanCompressor RamanLearning(std::vector<std::vector<std::string>> &samples) {
    if (samples.empty()) throw std::runtime_error("Empty sample set in RamanLearning");
    size_t num_fields = samples[0].size();

    // stats
    RamanCompressor compressor(num_fields);
    for (auto &sample: samples) {
        for (size_t i = 0; i < num_fields; ++i) compressor.stats_[i].Increment(sample[i]);
    }

    // code tree
    for (size_t i = 0; i < num_fields; ++i) {
        raman::CodeTree tree = compressor.stats_[i].BuildCodeTree();
        raman::CanonicalCode code(tree, compressor.stats_[i].getSymbolLimit());
        compressor.forest_.push_back(code.toCodeTree());
    }
    return compressor;
}

template<typename T>
static BitStream RamanCompress(RamanCompressor *compressor, const T *sample) {
    BitStream bits;
    std::vector<std::string> raman_sample = sample->toRamanFormat();
    for (size_t i = 0; i < raman_sample.size(); ++i) {
        size_t sym = compressor->stats_[i].Str2Idx(raman_sample[i]);
        bits.WriteBits(compressor->forest_[i].GetCode(sym));
    }
    return bits;
}

template<typename T>
static void RamanDecompress(RamanCompressor *compressor, T *sample, BitStream &bits) {
    size_t sym;
    uint32_t pos = 0;
    size_t num_fields = compressor->stats_.size();
    std::vector<std::string> raman_sample(num_fields);

    for (size_t i = 0; i < num_fields; ++i) {
        raman::Decode(sym, &compressor->forest_[i], bits, pos);
        raman_sample[i] = compressor->stats_[i].Idx2Str(sym);
    }
    sample->fromRamanFormat(raman_sample);
}

template<typename T>
class RamanTupleBuffer {
public:
    const size_t kBufferSize = 1024 * 16;

    RamanTupleBuffer() : buffer_(kBufferSize), n_tuple(0), keys_(kBufferSize) {}

    inline uint32_t Size() {
        uint32_t raman_dict_size = 0;
        for (auto &compressor: compressors_) raman_dict_size += compressor.Size();
        return raman_dict_size;
    }

    inline void Append(const T &sample, int64_t key) {
        buffer_[n_tuple] = sample;
        keys_[n_tuple] = key;
        n_tuple++;
    }

    inline bool IsFull() {
        return n_tuple == kBufferSize;
    }

    uint64_t BlockCompress(std::vector<BitStream> &db, std::vector<int64_t> *keys, int32_t *dict_id) {
        // block learning
        std::vector<std::vector<std::string>> samples;
        for (auto &sample: buffer_) samples.push_back(sample.toRamanFormat());
        compressors_.push_back(RamanLearning(samples));
        RamanCompressor &compressor = compressors_.back();

        // block compress
        for (auto &sample: buffer_) db.push_back(RamanCompress(&compressor, &sample));

        // get keys and dict id
        if (keys) *keys = keys_;
        if (dict_id) *dict_id = compressors_.size() - 1;

        // clear block
        Clear();

        return compressor.Size();
    }

    inline T *find(int64_t key) {
        for (int32_t i = 0; i < n_tuple; ++i) {
            if (keys_[i] == key) return &buffer_[i];
        }
        return nullptr;
    }

    inline RamanCompressor *GetCompressor(int32_t dict_id) {
        if (dict_id >= compressors_.size())
            throw std::runtime_error("Invalid dict id in GetCompressor");
        return &compressors_[dict_id];
    }

private:
    int32_t n_tuple;
    std::vector<T> buffer_;
    std::vector<int64_t> keys_;
    std::vector<RamanCompressor> compressors_;

    inline void Clear() {
        n_tuple = 0;
    }
};

#endif //TPCC_TPCC_RAMAN_H
