// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#define __STDC_FORMAT_MACROS

#include <cinttypes>
#include <climits>
#include <cstdio>

#include "clock.h"
#include "randomgenerator.h"
#include "tpccclient.h"
#include "tpccgenerator.h"
#include "tpcctables.h"

static const int NUM_TRANSACTIONS = 1000000;
static const int kTxnsInterval = 5000;

enum Mode {
    GenerateCSV,
    Benchmark
};
static Mode mode;
static long num_warehouses = 1;
static double memory_size;

void welcome(int argc, const char *const *argv);

void MemDiskSize(TPCCTables *table, bool detailed);

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
    int64_t loading_data_ms = (end - begin + 500) / 1000;
    std::cout << "\tLoading Data Time: " << loading_data_ms << "\n";

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
            printf("Load Compressor...");
            fflush(stdout);
            uint64_t learning_data_ms = 0;
            // stock
            {
                std::vector<std::vector<std::string>> samples;
                tables->StockToRaman(num_warehouses, samples);

                begin = clock->getMicroseconds();
                static RamanCompressor forest = RamanLearning(samples);
                end = clock->getMicroseconds();
                learning_data_ms += (end - begin + 500) / 1000;

                tables->MountCompression(&forest, "stock");
            }
            // order
            {
                std::vector<std::vector<std::string>> samples;
                tables->OrderToRaman(samples);

                begin = clock->getMicroseconds();
                static RamanCompressor forest = RamanLearning(samples);
                end = clock->getMicroseconds();
                learning_data_ms += (end - begin + 500) / 1000;

                tables->MountCompression(&forest, "order");
            }
            // orderline
            {
                std::vector<std::vector<std::string>> samples;
                tables->OrderlineToRaman(samples);

                begin = clock->getMicroseconds();
                static RamanCompressor forest = RamanLearning(samples);
                end = clock->getMicroseconds();
                learning_data_ms += (end - begin + 500) / 1000;

                tables->MountCompression(&forest, "orderline");
            }
            // customer
            {
                std::vector<std::vector<std::string>> samples;
                tables->CustomerToRaman(num_warehouses, samples);

                begin = clock->getMicroseconds();
                static RamanCompressor forest = RamanLearning(samples);
                end = clock->getMicroseconds();
                learning_data_ms += (end - begin + 500) / 1000;

                tables->MountCompression(&forest, "customer");
            }
            // history
            {
                std::vector<std::vector<std::string>> samples;
                tables->HistoryToRaman(samples);

                begin = clock->getMicroseconds();
                static RamanCompressor forest = RamanLearning(samples);
                end = clock->getMicroseconds();
                learning_data_ms += (end - begin + 500) / 1000;

                tables->MountCompression(&forest, "history");
            }
            printf("\tLearning Data Time: %lu\n", learning_data_ms);

            // Change the constants for run
            random = new tpcc::RealRandomGenerator();
            random->setC(tpcc::NURandC::makeRandomForRun(random, cLoad));

            // Client owns all the parameters
            TPCCClient client(clock, random, tables, Item::NUM_ITEMS,
                              static_cast<int>(num_warehouses),
                              District::NUM_PER_WAREHOUSE, Customer::NUM_PER_DISTRICT);
            printf("Running...\n");
            fflush(stdout);

            std::vector<double> throughput;
            std::vector<uint64_t> table_mem_size;
            std::vector<uint64_t> table_disk_size;
            std::vector<uint64_t> model_size;
            std::vector<uint64_t> bplus_tree_size;

            uint64_t total_nanoseconds = 0;
            uint64_t interval_ns = 0;
            for (int i = 0; i < NUM_TRANSACTIONS; ++i) {
                interval_ns += client.doOne();

                if (i % kTxnsInterval == 0 && i > 0) {
                    // show stat
                    uint64_t interval_ms = interval_ns / 1000;
                    throughput.push_back(kTxnsInterval / (double) interval_ms * 1000000.0);
                    table_mem_size.push_back(tables->stat_.total_mem_);
                    table_disk_size.push_back(tables->stat_.total_disk_);
                    model_size.push_back(tables->stat_.raman_dict);
                    bplus_tree_size.push_back(tables->TreeSize());

                    total_nanoseconds += interval_ns;
                    interval_ns = 0;

                    printf("%d\t%f\t%lu\t%lu\t%lu\t%lu\n", i, throughput.back(), table_mem_size.back(),
                           table_disk_size.back(), model_size.back(), bplus_tree_size.back());
                }
            }
            uint64_t microseconds = total_nanoseconds / 1000;
            printf("%d transactions in %" PRId64 " ms = %f txns/s\n", NUM_TRANSACTIONS,
                   (microseconds + 500) / 1000,
                   NUM_TRANSACTIONS / (double) microseconds * 1000000.0);
            MemDiskSize(tables, true);
            break;
        }
        default:
            printf("Unknown mode\n");
    }
    return 0;
}

void welcome(int argc, const char *const *argv) {
    if (argc == 3) {
        num_warehouses = strtol(argv[1], NULL, 10);
        memory_size = strtod(argv[2], NULL);
    } else if (argc == 4) {
        num_warehouses = strtol(argv[1], NULL, 10);
        memory_size = strtod(argv[2], NULL);
        bool is_download = std::stoi(argv[3]);
        if (is_download) mode = GenerateCSV;
        else mode = Benchmark;
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

void MemDiskSize(TPCCTables *table, bool detailed) {
    if (detailed) {
        std::cout << "[Table Name]: " << "[Memory Size] + [Disk Size] + [Raman Dict Size]" << std::endl;
        std::cout << "Warehouse: " << table->stat_.warehouse_mem_ << " byte" << std::endl;
        std::cout << "District: " << table->stat_.district_mem_ << " byte" << std::endl;
        std::cout << "Customer: " << table->stat_.customer_mem_ << " + " << table->stat_.customer_disk_ << " + "
                  << table->RamanDictSize("customer") << " byte" << std::endl;
        std::cout << "Order: " << table->stat_.order_mem_ << " + " << table->RamanDictSize("order") << " byte"
                  << std::endl;
        std::cout << "Orderline: " << table->stat_.orderline_mem_ << " + " << table->stat_.orderline_disk_ << " + "
                  << table->RamanDictSize("orderline") << " byte" << std::endl;
        std::cout << "NewOrder: " << table->stat_.neworder_mem_ << " byte" << std::endl;
        std::cout << "Item: " << table->stat_.item_mem_ << " byte" << std::endl;
        std::cout << "Stock: " << table->stat_.stock_mem_ << " + " << table->stat_.stock_disk_ << " + "
                  << table->RamanDictSize("stock") << " byte" << std::endl;
        std::cout << "History: " << table->stat_.history_mem_ << " + 0 + " << table->RamanDictSize("history") << " byte"
                  << std::endl;
        std::cout << "--------------------------------------------" << std::endl;
    }
    uint64_t others = table->stat_.history_mem_ + table->stat_.neworder_mem_ + table->stat_.order_mem_;
    std::cout << "Index Size: " << table->TreeSize() << ", Raman Model Size: " << table->stat_.raman_dict
              << ", Mem Data: " << table->stat_.total_mem_ << ", Disk Data: " << table->stat_.total_disk_ << ", Other: "
              << others << " byte" << std::endl;
    std::cout << "--------------------------------------------\n" << std::endl;
}

