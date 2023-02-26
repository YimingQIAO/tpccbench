// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#define __STDC_FORMAT_MACROS

#include <chrono>
#include <cinttypes>
#include <climits>
#include <cstdio>

#include "clock.h"
#include "randomgenerator.h"
#include "tpccclient.h"
#include "tpccgenerator.h"
#include "tpcctables.h"

static const int NUM_TRANSACTIONS = 100 * 1e6;
static const int kTxnsInterval = 0.1 * 1e5;
enum Mode {
    GenerateCSV,
    Benchmark
};
static Mode mode;
static long num_warehouses = 1;
static double memory_size;
static int running_time;

void welcome(int argc, const char *const *argv);

void MemDiskSize(TPCCTables &table, bool detailed, uint64_t blitz_model_size);

std::ifstream::pos_type filesize(const char *filename);

int main(int argc, const char *argv[]) {
    welcome(argc, argv);

    TPCCTables *tables = new TPCCTables(memory_size);
    SystemClock *clock = new SystemClock();

    // Create a generator for filling the database.
    tpcc::RealRandomGenerator *random = new tpcc::RealRandomGenerator();
    tpcc::NURandC cLoad = tpcc::NURandC::makeRandom(random);
    random->setC(cLoad);

    // Generate the data
    printf("Loading %ld warehouses... ", num_warehouses);
    fflush(stdout);
    char now[Clock::DATETIME_SIZE + 1];
    clock->getDateTimestamp(now);
    TPCCGenerator generator(random, now, Item::NUM_ITEMS, District::NUM_PER_WAREHOUSE,
                            Customer::NUM_PER_DISTRICT, NewOrder::INITIAL_NUM_PER_DISTRICT);

    int64_t begin = clock->getMicroseconds();
    generator.makeItemsTable(tables);
    for (int i = 0; i < num_warehouses; ++i) generator.makeWarehouse(tables, i + 1);
    int64_t end = clock->getMicroseconds();
    int64_t load_data_ms = (end - begin + 500) / 1000;
    printf("\tLoading Data Time: %lu\n", load_data_ms);
    // tableSize(tables, true, false);

    switch (mode) {
        case GenerateCSV: {
            // download the date
            tables->OrderlineToCSV(num_warehouses);
            tables->StockToCSV(num_warehouses);
            tables->CustomerToCSV(num_warehouses);
            tables->HistoryToCSV(num_warehouses);
            break;
        }
        case Benchmark: {
            printf("Transforming %ld warehouses... ", num_warehouses);
            fflush(stdout);
            srand(time(nullptr));
            int32_t model_id = rand();
            uint64_t learning_data_ms = 0;
            uint64_t blitz_model_size = 0;
            // orderline
            {
                OrderLineBlitz order_line_blitz;
                tables->OrderLineToBlitz(order_line_blitz, num_warehouses);
                std::string ol_model_name = std::to_string(model_id) + "_ol_model.blitz";

                begin = clock->getMicroseconds();
                static db_compress::RelationCompressor ol_compressor(ol_model_name.c_str(),
                                                                     order_line_blitz.schema(),
                                                                     order_line_blitz.compressionConfig(),
                                                                     kBlockSize);
                BlitzLearning(order_line_blitz, ol_compressor);
                static db_compress::RelationDecompressor ol_decompressor(ol_model_name.c_str(),
                                                                         order_line_blitz.schema(),
                                                                         kBlockSize);
                ol_decompressor.InitWithoutIndex();
                end = clock->getMicroseconds();
                learning_data_ms += (end - begin + 500) / 1000;

                tables->MountCompressor(ol_compressor, ol_decompressor, num_warehouses,
                                        "orderline");
                db_compress::NextTableAttrInterpreter();

                blitz_model_size += filesize(ol_model_name.c_str()) * 2.5;
            }
            // stock
            {
                StockBlitz stock_blitz;
                tables->StockToBlitz(stock_blitz, num_warehouses);
                std::string stock_model_name = std::to_string(model_id) + "_stock_model.blitz";

                begin = clock->getMicroseconds();
                static db_compress::RelationCompressor stock_compressor(stock_model_name.c_str(),
                                                                        stock_blitz.schema(),
                                                                        stock_blitz.compressionConfig(),
                                                                        kBlockSize);
                BlitzLearning(stock_blitz, stock_compressor);
                static db_compress::RelationDecompressor stock_decompressor(stock_model_name.c_str(),
                                                                            stock_blitz.schema(), kBlockSize);
                stock_decompressor.InitWithoutIndex();
                end = clock->getMicroseconds();
                learning_data_ms += (end - begin + 500) / 1000;

                tables->MountCompressor(stock_compressor, stock_decompressor, num_warehouses,
                                        "stock");
                db_compress::NextTableAttrInterpreter();

                blitz_model_size += filesize(stock_model_name.c_str());
            }
            // customer
            {
                CustomerBlitz cust_blitz;
                tables->CustomerToBlitz(cust_blitz, num_warehouses);
                std::string customer_model_name =
                        std::to_string(model_id) + "_customer_model.blitz";

                begin = clock->getMicroseconds();
                static db_compress::RelationCompressor cust_compressor(customer_model_name.c_str(),
                                                                       cust_blitz.schema(),
                                                                       cust_blitz.compressionConfig(),
                                                                       kBlockSize);
                BlitzLearning(cust_blitz, cust_compressor);
                static db_compress::RelationDecompressor cust_decompressor(customer_model_name.c_str(),
                                                                           cust_blitz.schema(),
                                                                           kBlockSize);
                cust_decompressor.InitWithoutIndex();
                end = clock->getMicroseconds();
                learning_data_ms += (end - begin + 500) / 1000;

                tables->MountCompressor(cust_compressor, cust_decompressor, num_warehouses, "customer");
                db_compress::NextTableAttrInterpreter();

                blitz_model_size += filesize(customer_model_name.c_str());
            }
            printf("Learning Data Time: %lu ms\n", learning_data_ms);
            fflush(stdout);


            // tell stat the size of blitz model
            tables->stat_.LoadBlitzModelSize(blitz_model_size);

            // Change the constants for run
            random = new tpcc::RealRandomGenerator();
            random->setC(tpcc::NURandC::makeRandomForRun(random, cLoad));

            // Client owns all the parameters
            TPCCClient client(clock, random, tables, Item::NUM_ITEMS,
                              static_cast<int>(num_warehouses),
                              District::NUM_PER_WAREHOUSE, Customer::NUM_PER_DISTRICT, now);
            printf("Running...\n");
            fflush(stdout);

            std::vector<double> throughput;
            std::vector<uint64_t> table_mem_size;
            std::vector<uint64_t> table_disk_size;
            std::vector<uint64_t> model_size;
            std::vector<uint64_t> bplus_tree_size;

            uint64_t total_nanoseconds = 0;
            uint64_t interval_ns = 0;
            int executed_txns;
            for (executed_txns = 0; executed_txns < NUM_TRANSACTIONS; ++executed_txns) {
                interval_ns += client.doOne();

                if (executed_txns % kTxnsInterval == 0 && executed_txns > 0) {
                    // show stat
                    uint64_t interval_ms = interval_ns / 1000;
                    throughput.push_back(kTxnsInterval / (double) interval_ms * 1000000.0);
                    table_mem_size.push_back(tables->stat_.total_mem_);
                    table_disk_size.push_back(tables->stat_.total_disk_);
                    model_size.push_back(tables->stat_.blitz_model_);
                    bplus_tree_size.push_back(tables->TreeSize());

                    total_nanoseconds += interval_ns;
                    interval_ns = 0;

                    printf("%d\t%f\t%lu\t%lu\t%lu\t%lu\n", executed_txns, throughput.back(), table_mem_size.back(),
                           table_disk_size.back(), model_size.back(), bplus_tree_size.back());

                    int32_t sec = total_nanoseconds / 1000 / 1000 / 1000;
                    if (sec > running_time * 60) break;
                }
            }
            uint64_t microseconds = total_nanoseconds / 1000;
            printf("%d transactions in %" PRId64 " ms = %f txns/s\n", executed_txns, (microseconds + 500) / 1000,
                   executed_txns / (double) microseconds * 1000000.0);
            MemDiskSize(*tables, true, blitz_model_size);
            break;
        }
        default:
            printf("Unknown mode\n");
    }
    return 0;
}

void welcome(int argc, const char *const *argv) {
    if (argc == 4) {
        num_warehouses = strtol(argv[1], NULL, 10);
        memory_size = strtod(argv[2], NULL);
        running_time = strtol(argv[3], NULL, 10);
    } else if (argc == 5) {
        num_warehouses = strtol(argv[1], NULL, 10);
        memory_size = strtod(argv[2], NULL);
        running_time = strtol(argv[3], NULL, 10);
        bool is_download = std::stoi(argv[4]);
        if (is_download) mode = GenerateCSV;
        else
            mode = Benchmark;
    } else {
        fprintf(stderr,
                "tpcc [num warehouses] [mode]\n Option: mode = 0 (default) for running test, mode = 1 for generating data\n");
        exit(1);
    }

    if (num_warehouses == LONG_MIN || num_warehouses == LONG_MAX) {
        fprintf(stderr, "Bad warehouse number (%s)\n", argv[1]);
        exit(1);
    }
    if (num_warehouses <= 0) {
        fprintf(stderr, "Number of warehouses must be > 0 (was %ld)\n",
                num_warehouses);
        exit(1);
    }
    if (num_warehouses > Warehouse::MAX_WAREHOUSE_ID) {
        fprintf(stderr, "Number of warehouses must be <= %d (was %ld)\n",
                Warehouse::MAX_WAREHOUSE_ID, num_warehouses);
        exit(1);
    }
}

void MemDiskSize(TPCCTables &table, bool detailed, uint64_t blitz_model_size) {
    if (detailed) {
        std::cout << "[Table Name]: " << "[Memory Size] + [Disk Size]" << std::endl;
        std::cout << "Warehouse: " << table.stat_.warehouse_mem_ << " byte" << std::endl;
        std::cout << "District: " << table.stat_.district_mem_ << " byte" << std::endl;
        std::cout << "Customer: " << table.stat_.customer_mem_ << " + " << table.stat_.customer_disk_ << " byte"
                  << std::endl;
        std::cout << "Order: " << table.stat_.order_mem_ << " byte" << std::endl;
        std::cout << "Orderline: " << table.stat_.orderline_mem_ << " + " << table.stat_.orderline_disk_
                  << " byte" << std::endl;
        std::cout << "NewOrder: " << table.stat_.neworder_mem_ << " byte" << std::endl;
        std::cout << "Item: " << table.stat_.item_mem_ << " byte" << std::endl;
        std::cout << "Stock: " << table.stat_.stock_mem_ << " + " << table.stat_.stock_disk_ << " byte"
                  << std::endl;
        std::cout << "History: " << table.stat_.history_mem_ << " byte" << std::endl;
        std::cout << "--------------------------------------------\n";
    }
    uint64_t mem_total = table.stat_.warehouse_mem_ + table.stat_.district_mem_ + table.stat_.customer_mem_ +
                         table.stat_.orderline_mem_ + table.stat_.item_mem_ + table.stat_.stock_mem_;
    uint64_t disk_total = table.stat_.customer_disk_ + table.stat_.orderline_disk_ + table.stat_.stock_disk_;
    uint64_t others = table.stat_.history_mem_ + table.stat_.neworder_mem_ + table.stat_.order_mem_;
    std::cout << "Index Size: " << table.TreeSize() << "\t";
    std::cout << "Blitz Model Size: " << blitz_model_size << "\t";
    std::cout << "Mem: " << mem_total << "\t"
              << "Disk: " << disk_total << "\t"
              << "Other: " << others << " byte" << std::endl;
    std::cout << "---------------------------------------------------------------------\n";
}

std::ifstream::pos_type filesize(const char *filename) {
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
}


