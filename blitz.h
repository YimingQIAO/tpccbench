//
// Created by Qiao Yiming on 2023/1/11.
//

#ifndef TPCC_BLITZ_H
#define TPCC_BLITZ_H

#include <random>
#include <sstream>

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

// -------------------------- Blitz Table ------------------------------------

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
    static const int kNumAttrs = 10;

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
    static const int kNumAttrs = 17;

    StockBlitz() {
        config_ = {
                {kInteger, 0,    0.5},
                {kEnum,    5,    0},
                {kEnum,    100,  0},
                {kInteger, 0,    0.5},
                {kEnum,    2000, 0},
                {kEnum,    100,  0},
                {kString,  0,    0},
                {kString,  0,    0},
                {kString,  0,    0},
                {kString,  0,    0},
                {kString,  0,    0},
                {kString,  0,    0},
                {kString,  0,    0},
                {kString,  0,    0},
                {kString,  0,    0},
                {kString,  0,    0},
                {kString,  0,    0}
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

class CustomerBlitz : public BlitzTable {
public:
    static const int kNumAttrs = 21;

    CustomerBlitz() {
        config_ = {
                {kEnum,   Customer::NUM_PER_DISTRICT,  0},
                {kEnum,   District::NUM_PER_WAREHOUSE, 0},
                {kEnum,   5,                           0},
                {kDouble, 0,                           0.0025},
                {kDouble, 0,                           0.000025},
                {kEnum,   1,                           0},
                {kDouble, 0,                           0.0025},
                {kDouble, 0,                           0.0025},
                {kEnum,   1,                           0},
                {kEnum,   2,                           0},
                {kString, 0,                           0},
                {kString, 0,                           0},
                {kEnum,   1,                           0},
                {kString, 0,                           0},
                {kString, 0,                           0},
                {kString, 0,                           0},
                {kEnum,   50,                          0},
                {kString, 0,                           0},
                {kString, 0,                           0},
                {kString, 0,                           0},
                {kString, 0,                           0}
        };
        RegisterAttrInterpreter();
    }

    db_compress::CompressionConfig compressionConfig() override {
        db_compress::CompressionConfig config;
        for (AttrConfig &ac: config_) config.allowed_err_.push_back(ac.tolerance);
        config.skip_model_learning_ = true;
        return config;
    }

    bool pushTuple(Customer *customer);
};

void BlitzLearning(BlitzTable &table, db_compress::RelationCompressor &compressor);

void orderlineToAttrVector(const OrderLine &order_line, db_compress::AttrVector &tuple);

OrderLine attrVectorToOrderLine(db_compress::AttrVector &attrVector);

void stockToAttrVector(const Stock &stock, db_compress::AttrVector &tuple);

Stock attrVectorToStock(db_compress::AttrVector &attrVector);

void customerToAttrVector(const Customer &customer, db_compress::AttrVector &tuple);

Customer attrVectorToCustomer(db_compress::AttrVector &attrVector);

// --------------------------- Enum Handle ----------------------------------
int EnumStrToId(const std::string &str, int32_t attr, const std::string &table_name);

std::string &EnumIdToStr(int32_t id, int32_t attr, const std::string &table_name);

#endif //TPCC_BLITZ_H
