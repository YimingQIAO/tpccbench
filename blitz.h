//
// Created by Qiao Yiming on 2023/1/11.
//

#ifndef TPCC_BLITZ_H
#define TPCC_BLITZ_H

#include <random>

#include "base.h"
#include "model.h"
#include "tpccdb.h"
#include "model_learner.h"
#include "compression.h"
#include "decompression.h"

#define kEnum 0
#define kInteger 1
#define kDouble 2
#define kString 3

/**
 * This class is used to interpret categorical attributes.
 */
class SimpleCategoricalInterpreter : public db_compress::AttrInterpreter {
private:
    int cap_;

public:
    explicit SimpleCategoricalInterpreter(int cap) : cap_(cap) {}

    bool EnumInterpretable() const override { return true; }

    int EnumCap() const override { return cap_; }

    size_t EnumInterpret(const db_compress::AttrValue &attr) const override { return attr.Int(); }
};

struct AttrConfig {
    int type;
    int capacity;
    double tolerance;
};

class BlitzTable {
public:
    db_compress::CompressionConfig compressionConfig();

    db_compress::Schema schema();

    uint32_t rowsNum() {
        return table_.size();
    }

    db_compress::AttrVector &getTuple(uint32_t idx) {
        assert(idx < table_.size());
        return table_[idx];
    }

protected:
    std::vector<db_compress::AttrVector> table_;
    std::vector<AttrConfig> config_;
};

class OrderLineTable : public BlitzTable {
public:
    OrderLineTable() {
        config_ = {
                {kInteger, 0,               0.5},
                {kEnum,    10,              0},
                {kEnum,    5,               0},
                {kEnum,    15,              0},
                {kEnum,    Item::NUM_ITEMS, 0},
                {kEnum,    5,               0},
                {kEnum,    10,              0},
                {kDouble,  0,               0.0025},
                {kString,  0,               0},
                {kString,  0,               0}
        };

        RegisterAttrInterpreter();
    }

    bool pushTuple(OrderLine *order_line);

private:
    void RegisterAttrInterpreter() {
        for (int i = 0; i < config_.size(); ++i) {
            AttrConfig &ac = config_[i];
            if (ac.type == kEnum)
                db_compress::RegisterAttrInterpreter(i,
                                                     new SimpleCategoricalInterpreter(ac.capacity));
            else
                db_compress::RegisterAttrInterpreter(i,
                                                     new db_compress::AttrInterpreter());
        }

        // Register attributed model and interpreter
        RegisterAttrModel(0, new db_compress::TableCategoricalCreator());
        RegisterAttrModel(1, new db_compress::TableNumericalIntCreator());
        RegisterAttrModel(2, new db_compress::TableNumericalRealCreator());
        RegisterAttrModel(3, new db_compress::StringModelCreator());
    }
};

void BlitzLearning(BlitzTable &table, db_compress::RelationCompressor &compressor);

#endif //TPCC_BLITZ_H
