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

bool OrderLineBlitz::pushTuple(OrderLine *order_line) {
    if (order_line == nullptr) return false;
    db_compress::AttrVector tuple(10);
    orderlineToAttrVector(*order_line, tuple);
    table_.push_back(tuple);
    return true;
}

bool StockBlitz::pushTuple(Stock *stock) {
    if (stock == nullptr) return false;
    db_compress::AttrVector tuple(6 + District::NUM_PER_WAREHOUSE + 1);
    stockToAttrVector(*stock, tuple);
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

void orderlineToAttrVector(const OrderLine &order_line, db_compress::AttrVector &tuple) {
    tuple.attr_[0].value_ = order_line.ol_i_id;
    tuple.attr_[1].value_ = order_line.ol_amount;
    tuple.attr_[2].value_ = order_line.ol_number;
    tuple.attr_[3].value_ = order_line.ol_supply_w_id;
    tuple.attr_[4].value_ = order_line.ol_quantity;
    tuple.attr_[5].value_ = order_line.ol_delivery_d;
    tuple.attr_[6].value_ = order_line.ol_dist_info;
    tuple.attr_[7].value_ = order_line.ol_o_id;
    tuple.attr_[8].value_ = order_line.ol_d_id;
    tuple.attr_[9].value_ = order_line.ol_w_id;
}

OrderLine attrVectorToOrderLine(db_compress::AttrVector &attrVector) {
    OrderLine order_line;
    order_line.ol_o_id = std::get<int>(attrVector.attr_[7].value_);
    order_line.ol_d_id = std::get<int>(attrVector.attr_[8].value_);
    order_line.ol_w_id = std::get<int>(attrVector.attr_[9].value_);
    order_line.ol_number = std::get<int>(attrVector.attr_[2].value_);
    order_line.ol_i_id = std::get<int>(attrVector.attr_[0].value_);
    order_line.ol_supply_w_id = std::get<int>(attrVector.attr_[3].value_);
    order_line.ol_quantity = std::get<int>(attrVector.attr_[4].value_);
    order_line.ol_amount = std::get<double>(attrVector.attr_[1].value_);
    std::string delivery_d = std::get<std::string>(attrVector.attr_[5].value_);
    std::string dist_info = std::get<std::string>(attrVector.attr_[6].value_);
    memcpy(order_line.ol_delivery_d, delivery_d.c_str(), DATETIME_SIZE + 1);
    memcpy(order_line.ol_dist_info, dist_info.c_str(), Stock::DIST + 1);
    return order_line;
}

void stockToAttrVector(const Stock &stock, db_compress::AttrVector &tuple) {
    tuple.attr_[0].value_ = stock.s_i_id;
    tuple.attr_[1].value_ = stock.s_w_id;
    tuple.attr_[2].value_ = stock.s_quantity;
    tuple.attr_[3].value_ = stock.s_ytd;
    tuple.attr_[4].value_ = stock.s_order_cnt;
    tuple.attr_[5].value_ = stock.s_remote_cnt;
    tuple.attr_[6].value_ = stock.s_data;
    for (int32_t k = 0; k < District::NUM_PER_WAREHOUSE; k++)
        tuple.attr_[7 + k].value_ = stock.s_dist[k];
}

Stock attrVectorToStock(db_compress::AttrVector &attrVector) {
    Stock stock;
    stock.s_i_id = attrVector.attr_[0].Int();
    stock.s_w_id = attrVector.attr_[1].Int();
    stock.s_quantity = attrVector.attr_[2].Int();
    stock.s_ytd = attrVector.attr_[3].Int();
    stock.s_order_cnt = attrVector.attr_[4].Int();
    stock.s_remote_cnt = attrVector.attr_[5].Int();
    strcpy(stock.s_data, attrVector.attr_[6].String().c_str());
    for (int32_t k = 0; k < District::NUM_PER_WAREHOUSE; k++) {
        std::string &s = attrVector.attr_[7 + k].String();
        strcpy(stock.s_dist[k], s.c_str());
    }
    return stock;
}
