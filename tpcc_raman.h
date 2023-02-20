#ifndef TPCC_TPCC_RAMAN_H
#define TPCC_TPCC_RAMAN_H

#include "code_tree.h"
#include "frequency_table.h"
#include "bit_stream.h"
#include "canonical_code.h"
#include "coder.h"

struct RamanCompressor {
    std::vector<raman::FreqStat> stats_;
    std::vector<std::unique_ptr<raman::CodeTree>> forest_;

    explicit RamanCompressor(size_t num_fields) : stats_(num_fields), forest_(num_fields) {}
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
        tree = code.toCodeTree();
        compressor.forest_[i] = std::make_unique<raman::CodeTree>(std::move(tree));
    }
    return compressor;
}

template<typename T>
static BitStream RamanCompress(RamanCompressor *compressor, T *sample) {
    BitStream bits;
    std::vector<std::string> raman_sample = sample->toRamanFormat();
    for (size_t i = 0; i < raman_sample.size(); ++i) {
        size_t sym = compressor->stats_[i].Str2Idx(raman_sample[i]);
        bits.WriteBits(compressor->forest_[i]->GetCode(sym));
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
        pos = raman::Decode(sym, compressor->forest_[i].get(), bits, pos);
        raman_sample[i] = compressor->stats_[i].Idx2Str(sym);
    }
    sample->fromRamanFormat(raman_sample);
}

#endif //TPCC_TPCC_RAMAN_H
