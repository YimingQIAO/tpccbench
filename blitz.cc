//
// Created by Qiao Yiming on 2023/1/11.
//

#include "blitz.h"

db_compress::CompressionConfig BlitzTable::compressionConfig() {
    db_compress::CompressionConfig config;
    for (AttrConfig &ac: config_) config.allowed_err_.push_back(ac.tolerance);
    config.skip_model_learning_ = true;
    return config;
}

db_compress::Schema BlitzTable::schema() {
    std::vector<int> attr_type;
    attr_type.reserve(config_.size());
    for (AttrConfig &ac: config_) attr_type.push_back(ac.type);
    return db_compress::Schema(attr_type);
}

bool OrderLineTable::pushTuple(OrderLine *order_line) {
    if (order_line == nullptr) return false;

    db_compress::AttrVector tuple(10);
    tuple.attr_[0].value_ = order_line->ol_o_id;
    tuple.attr_[1].value_ = order_line->ol_d_id;
    tuple.attr_[2].value_ = order_line->ol_w_id;
    tuple.attr_[3].value_ = order_line->ol_number;
    tuple.attr_[4].value_ = std::to_string(order_line->ol_i_id);
    tuple.attr_[5].value_ = order_line->ol_supply_w_id;
    tuple.attr_[6].value_ = order_line->ol_quantity;
    tuple.attr_[7].value_ = order_line->ol_amount;
    tuple.attr_[8].value_ = std::string(order_line->ol_delivery_d,
                                        order_line->ol_delivery_d + DATETIME_SIZE + 1);
    tuple.attr_[9].value_ = std::string(order_line->ol_dist_info,
                                        order_line->ol_dist_info + Stock::DIST + 1);
    table_.push_back(tuple);
    return true;
}

void BlitzLearning(BlitzTable &table, db_compress::RelationCompressor &compressor) {
    // random number
    std::random_device random_device;
    std::mt19937 mt19937(0);
    std::uniform_int_distribution<uint32_t> dist(0, table.rowsNum() - 1);
    bool tuning = false;

    // Learning Iterations
    while (true) {
        int tuple_cnt = 0;
        int tuple_random_cnt = 0;
        int tuple_idx;

        while (tuple_cnt < table.rowsNum()) {
            if (tuple_random_cnt < kNumEstSample) {
                tuple_idx = static_cast<int>(dist(mt19937));
                tuple_random_cnt++;
            } else {
                tuple_idx = tuple_cnt;
                tuple_cnt++;
            }

            db_compress::AttrVector &tuple = table.getTuple(tuple_idx);
            { compressor.LearnTuple(tuple); }
            if (tuple_cnt >= kNonFullPassStopPoint && !compressor.RequireFullPass()) {
                break;
            }
        }
        compressor.EndOfLearningAndWriteModel();

        if (!tuning && compressor.RequireFullPass()) {
            tuning = true;
        }

        if (!compressor.RequireMoreIterationsForLearning()) {
            break;
        }
    }
}
