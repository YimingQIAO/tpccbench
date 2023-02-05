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
    virtual db_compress::CompressionConfig compressionConfig();

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

    void RegisterAttrInterpreter() {
        for (int i = 0; i < config_.size(); ++i) {
            AttrConfig &ac = config_[i];
            if (ac.type == kEnum)
                db_compress::RegisterAttrInterpreter(i, new SimpleCategoricalInterpreter(ac.capacity));
            else
                db_compress::RegisterAttrInterpreter(i, new db_compress::AttrInterpreter());
        }

        // Register attributed model and interpreter
        RegisterAttrModel(0, new db_compress::TableCategoricalCreator());
        RegisterAttrModel(1, new db_compress::TableNumericalIntCreator());
        RegisterAttrModel(2, new db_compress::TableNumericalRealCreator());
        RegisterAttrModel(3, new db_compress::StringModelCreator());
    }
};

class OrderLineBlitz : public BlitzTable {
public:
    OrderLineBlitz() {
        config_ = {
                {kEnum,    Item::NUM_ITEMS, 0},
                {kDouble,  0,               0.0025},
                {kEnum,    15,              0},
                {kEnum,    5,               0},
                {kEnum,    100,             0},
                {kString,  0,               0},
                {kString,  0,               0},
                {kInteger, 0,               0.5},
                {kEnum,    10,              0},
                {kEnum,    5,               0},
        };
        RegisterAttrInterpreter();
    }

    bool pushTuple(OrderLine *order_line);
};

class StockBlitz : public BlitzTable {
public:
    StockBlitz() {
        config_ = {
                {kInteger, 0,   0.5},
                {kEnum,    5,   0},
                {kEnum,    100, 0},
                {kEnum,    1,   0},
                {kEnum,    1,   0},
                {kEnum,    1,   0},
                {kString,  0,   0},
                {kString,  0,   0},
                {kString,  0,   0},
                {kString,  0,   0},
                {kString,  0,   0},
                {kString,  0,   0},
                {kString,  0,   0},
                {kString,  0,   0},
                {kString,  0,   0},
                {kString,  0,   0},
                {kString,  0,   0}
        };
        RegisterAttrInterpreter();
    }

    db_compress::CompressionConfig compressionConfig() override {
        db_compress::CompressionConfig config;
        for (AttrConfig &ac: config_) config.allowed_err_.push_back(ac.tolerance);
        config.skip_model_learning_ = true;
        return config;
    }

    bool pushTuple(Stock *stock);
};

void BlitzLearning(BlitzTable &table, db_compress::RelationCompressor &compressor);

void orderlineToAttrVector(const OrderLine &order_line, db_compress::AttrVector &tuple);

OrderLine attrVectorToOrderLine(db_compress::AttrVector &attrVector);

void stockToAttrVector(const Stock &stock, db_compress::AttrVector &tuple);

Stock attrVectorToStock(db_compress::AttrVector &attrVector);

#endif //TPCC_BLITZ_H
