//
// Created by Qiao Yiming on 2023/2/22.
//

#ifndef TPCC_TPCC_STAT_H
#define TPCC_TPCC_STAT_H

#include <cstdint>
#include <string>

struct TPCCStat {
    uint64_t warehouse_mem_{};
    uint64_t warehouse_disk_{};
    uint64_t district_mem_{};
    uint64_t district_disk_{};
    uint64_t customer_mem_{};
    uint64_t customer_disk_{};
    uint64_t history_mem_{};
    uint64_t history_disk_{};
    uint64_t order_mem_{};
    uint64_t order_disk_{};
    uint64_t neworder_mem_{};
    uint64_t neworder_disk_{};
    uint64_t orderline_mem_{};
    uint64_t orderline_disk_{};
    uint64_t item_mem_{};
    uint64_t item_disk_{};
    uint64_t stock_mem_{};
    uint64_t stock_disk_{};

    uint64_t total_mem_{};
    uint64_t total_disk_{};

    uint64_t total_mem_limit_;

    uint64_t blitz_model_;

    const int32_t kPageSize = 4096;

    explicit TPCCStat(uint64_t total_mem_limit) : total_mem_limit_(total_mem_limit) {}

    inline void Insert(uint64_t size, bool is_mem, const std::string &table_name) {
        if (table_name != "history" && table_name != "order" && table_name != "neworder") {
            if (is_mem) total_mem_ += size;
            else total_disk_ += size;
        }

        if (table_name == "warehouse") {
            if (is_mem) warehouse_mem_ += size;
            else warehouse_disk_ += size;
        } else if (table_name == "district") {
            if (is_mem) district_mem_ += size;
            else district_disk_ += size;
        } else if (table_name == "customer") {
            if (is_mem) customer_mem_ += size;
            else customer_disk_ += size;
        } else if (table_name == "history") {
            if (is_mem) history_mem_ += size;
            else history_disk_ += size;
        } else if (table_name == "order") {
            if (is_mem) order_mem_ += size;
            else order_disk_ += size;
        } else if (table_name == "neworder") {
            if (is_mem) neworder_mem_ += size;
            else neworder_disk_ += size;
        } else if (table_name == "orderline") {
            if (is_mem) orderline_mem_ += size;
            else orderline_disk_ += size;
        } else if (table_name == "item") {
            if (is_mem) item_mem_ += size;
            else item_disk_ += size;
        } else if (table_name == "stock") {
            if (is_mem) stock_mem_ += size;
            else stock_disk_ += size;
        } else {
            printf("Error: table name not found!\n");
        }
    }

    inline void SwapTuple(uint64_t size, const std::string &table_name) {
        if (total_mem_ + blitz_model_ + size < total_mem_limit_) return;

        while (total_mem_ + blitz_model_ + size > total_mem_limit_) {
            total_mem_ -= kPageSize;

            if (table_name == "stock")
                stock_mem_ -= kPageSize;
            else if (table_name == "customer")
                customer_mem_ -= kPageSize;
            else if (table_name == "orderline")
                orderline_mem_ -= kPageSize;
            else
                printf("Error: table name not found!\n");

        }
        Insert(size, true, table_name);
    }

    inline bool ToMemory(uint64_t size) const {
        return total_mem_limit_ > kPageSize + total_mem_ + size;
    }

    inline void LoadBlitzModelSize(uint64_t model_size) {
        blitz_model_ = model_size;
    }
};

#endif //TPCC_TPCC_STAT_H
