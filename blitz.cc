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
    orderlineToAttrVector(*order_line, buffer_);
    table_.push_back(buffer_);
    return true;
}

bool StockBlitz::pushTuple(Stock *stock) {
    if (stock == nullptr) return false;
    stockToAttrVector(*stock, buffer_);
    table_.push_back(buffer_);
    return true;
}

bool CustomerBlitz::pushTuple(Customer *customer) {
    if (customer == nullptr) return false;
    customerToAttrVector(*customer, buffer_);
    table_.push_back(buffer_);
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
    order_line.ol_o_id = attrVector.attr_[7].Int();
    order_line.ol_d_id = attrVector.attr_[8].Int();
    order_line.ol_w_id = attrVector.attr_[9].Int();
    order_line.ol_number = attrVector.attr_[2].Int();
    order_line.ol_i_id = attrVector.attr_[0].Int();
    order_line.ol_supply_w_id = attrVector.attr_[3].Int();
    order_line.ol_quantity = attrVector.attr_[4].Int();
    order_line.ol_amount = (float) attrVector.attr_[1].Double();
    std::string delivery_d = attrVector.attr_[5].String();
    std::string dist_info = attrVector.attr_[6].String();
    memcpy(order_line.ol_delivery_d, delivery_d.c_str(), DATETIME_SIZE + 1);
    memcpy(order_line.ol_dist_info, dist_info.c_str(), Stock::DIST + 1);
    return order_line;
}

void stockToAttrVector(const Stock &stock, db_compress::AttrVector &tuple) {
    tuple.attr_[0].value_ = stock.s_quantity;
    tuple.attr_[1].value_ = stock.s_ytd;
    tuple.attr_[2].value_ = stock.s_order_cnt;
    tuple.attr_[3].value_ = stock.s_remote_cnt;
    tuple.attr_[4].value_ = stock.s_data;
    for (int32_t k = 0; k < District::NUM_PER_WAREHOUSE; k++)
        tuple.attr_[5 + k].value_ = stock.s_dist[k];
    tuple.attr_[15].value_ = stock.s_i_id;
    tuple.attr_[16].value_ = stock.s_w_id;
}

Stock attrVectorToStock(db_compress::AttrVector &attrVector) {
    Stock stock;
    stock.s_i_id = attrVector.attr_[15].Int();
    stock.s_w_id = attrVector.attr_[16].Int();
    stock.s_quantity = attrVector.attr_[0].Int();
    stock.s_ytd = attrVector.attr_[1].Int();
    stock.s_order_cnt = attrVector.attr_[2].Int();
    stock.s_remote_cnt = attrVector.attr_[3].Int();
    strcpy(stock.s_data, attrVector.attr_[4].String().c_str());
    for (int32_t k = 0; k < District::NUM_PER_WAREHOUSE; k++) {
        std::string &s = attrVector.attr_[5 + k].String();
        strcpy(stock.s_dist[k], s.c_str());
    }
    return stock;
}

void customerToAttrVector(const Customer &customer, db_compress::AttrVector &tuple) {
    tuple.attr_[0].value_ = customer.c_id;
    tuple.attr_[1].value_ = customer.c_d_id;
    tuple.attr_[2].value_ = customer.c_w_id;
    // tuple.attr_[3].value_ = customer.c_credit_lim;
    tuple.attr_[3].value_ = EnumStrToId(std::to_string(customer.c_credit_lim), 3, "customer");
    tuple.attr_[4].value_ = customer.c_discount;
    tuple.attr_[5].value_ = customer.c_delivery_cnt;
    tuple.attr_[6].value_ = customer.c_balance;
    tuple.attr_[7].value_ = customer.c_ytd_payment;
    tuple.attr_[8].value_ = customer.c_payment_cnt;
    tuple.attr_[9].value_ = EnumStrToId(customer.c_credit, 9, "customer");
    tuple.attr_[10].value_ = customer.c_last;
    tuple.attr_[11].value_ = customer.c_first;
    tuple.attr_[12].value_ = EnumStrToId(customer.c_middle, 12, "customer");
    tuple.attr_[13].value_ = customer.c_street_1;
    tuple.attr_[14].value_ = customer.c_street_2;
    tuple.attr_[15].value_ = customer.c_city;
    tuple.attr_[16].value_ = EnumStrToId(customer.c_state, 16, "customer");
    tuple.attr_[17].value_ = customer.c_zip;
    tuple.attr_[18].value_ = customer.c_phone;
    tuple.attr_[19].value_ = customer.c_since;
    tuple.attr_[20].value_ = customer.c_data;
}

Customer attrVectorToCustomer(db_compress::AttrVector &attrVector) {
    Customer customer;
    customer.c_id = attrVector.attr_[0].Int();
    customer.c_d_id = attrVector.attr_[1].Int();
    customer.c_w_id = attrVector.attr_[2].Int();
    customer.c_credit_lim = std::stof(EnumIdToStr(attrVector.attr_[3].Int(), 3, "customer"));
    customer.c_discount = attrVector.attr_[4].Double();
    customer.c_balance = attrVector.attr_[6].Double();
    customer.c_ytd_payment = attrVector.attr_[7].Double();
    customer.c_payment_cnt = attrVector.attr_[8].Int();
    customer.c_delivery_cnt = attrVector.attr_[5].Int();
    strncpy(customer.c_last, attrVector.attr_[10].String().c_str(), Customer::MAX_LAST);
    memcpy(customer.c_middle, "OE\0", Customer::MIDDLE + 1);
    strncpy(customer.c_first, attrVector.attr_[11].String().c_str(), Customer::MAX_FIRST);
    strncpy(customer.c_street_1, attrVector.attr_[13].String().c_str(), Address::MAX_STREET);
    strncpy(customer.c_street_2, attrVector.attr_[14].String().c_str(), Address::MAX_STREET);
    strncpy(customer.c_city, attrVector.attr_[15].String().c_str(), Address::MAX_CITY);
    strncpy(customer.c_state, EnumIdToStr(attrVector.attr_[16].Int(), 16, "customer").c_str(), Address::STATE);
    strncpy(customer.c_zip, attrVector.attr_[17].String().c_str(), Address::ZIP);
    strncpy(customer.c_phone, attrVector.attr_[18].String().c_str(), Customer::PHONE);
    strncpy(customer.c_since, attrVector.attr_[19].String().c_str(), DATETIME_SIZE);
    strncpy(customer.c_credit, EnumIdToStr(attrVector.attr_[9].Int(), 9, "customer").c_str(), Customer::CREDIT);
    strncpy(customer.c_data, attrVector.attr_[20].String().c_str(), Customer::MAX_DATA);
    return customer;
}

// ------------------------ Enum Handle ------------------------------
namespace {
    std::vector<db_compress::BiMap> enum_map;
}

int EnumStrToId(const std::string &str, int32_t attr, const std::string &table_name) {
    int32_t idx = 0;
    if (table_name == "order line") idx = attr;
    else if (table_name == "stock") idx = OrderLineBlitz::kNumAttrs + attr;
    else if (table_name == "customer") idx = OrderLineBlitz::kNumAttrs + StockBlitz::kNumAttrs + attr;
    else printf("Unknown table name.\n");

    if (enum_map.size() <= idx) enum_map.resize(idx + 1);
    auto &enum2idx = enum_map[idx].enum2idx;
    auto &enums = enum_map[idx].enums;

    auto it = enum2idx.find(str);
    if (it != enum2idx.end())
        return it->second;
    else {
        enum2idx[str] = enums.size();
        enums.push_back(str);
        return enums.size() - 1;
    }
}

std::string &EnumIdToStr(int32_t id, int32_t attr, const std::string &table_name) {
    int32_t map_idx = 0;
    if (table_name == "order line") map_idx = attr;
    else if (table_name == "stock") map_idx = OrderLineBlitz::kNumAttrs + attr;
    else if (table_name == "customer") map_idx = OrderLineBlitz::kNumAttrs + StockBlitz::kNumAttrs + attr;
    else printf("Unknown table name.\n");

    assert(enum_map.size() > map_idx);
    db_compress::BiMap &map = enum_map[map_idx];
    assert(map.enums.size() > id);
    return map.enums[id];
}
