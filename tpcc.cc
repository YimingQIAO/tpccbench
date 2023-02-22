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

static const int NUM_TRANSACTIONS = 1000000;
static const int kTxnsInterval = 50000;
enum Mode {
    GenerateCSV,
    Benchmark
};
static Mode mode;
static long num_warehouses = 1;
static double memory_size;

void welcome(int argc, const char *const *argv);

void MemDiskSize(TPCCStat &stat, bool detailed);

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
    printf("%" PRId64 " ms\n", (end - begin + 500) / 1000);
    // tableSize(tables);

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
            // Change the constants for run
            random = new tpcc::RealRandomGenerator();
            random->setC(tpcc::NURandC::makeRandomForRun(random, cLoad));

            // Client owns all the parameters
            TPCCClient client(clock, random, tables, Item::NUM_ITEMS,
                              static_cast<int>(num_warehouses),
                              District::NUM_PER_WAREHOUSE, Customer::NUM_PER_DISTRICT);
            printf("Running...\n");
            fflush(stdout);

            uint64_t total_nanoseconds = 0;
            uint64_t interval_ns = 0;
            for (int i = 0; i < NUM_TRANSACTIONS; ++i) {
                interval_ns += client.doOne();

                if (i % kTxnsInterval == 0 && i > 0) {
                    // show stat
                    uint64_t interval_ms = interval_ns / 1000;
                    double throughput = kTxnsInterval / (double) interval_ms * 1000000.0;
                    uint64_t mem = tables->stat_.total_mem_;
                    uint64_t disk = tables->stat_.total_disk_;
                    printf("%f, %llu, %llu\n", throughput, mem, disk);

                    total_nanoseconds += interval_ns;
                    interval_ns = 0;
                }
            }
            uint64_t microseconds = total_nanoseconds / 1000;
            printf("%d transactions in %" PRId64 " ms = %f txns/s\n", NUM_TRANSACTIONS,
                   (microseconds + 500) / 1000,
                   NUM_TRANSACTIONS / (double) microseconds * 1000000.0);
            MemDiskSize(tables->stat_, true);
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

void MemDiskSize(TPCCStat &stat, bool detailed) {
    if (detailed) {
        std::cout << "[Table Name]: " << "[Memory Size] + [Disk Size]" << std::endl;
        std::cout << "Warehouse: " << stat.warehouse_mem_ << " byte" << std::endl;
        std::cout << "District: " << stat.district_mem_ << " byte" << std::endl;
        std::cout << "Customer: " << stat.customer_mem_ << " + " << stat.customer_disk_ << " byte"
                  << std::endl;
        std::cout << "Order: " << stat.order_mem_ << " byte" << std::endl;
        std::cout << "Orderline: " << stat.orderline_mem_ << " + " << stat.orderline_disk_
                  << " byte" << std::endl;
        std::cout << "NewOrder: " << stat.neworder_mem_ << " byte" << std::endl;
        std::cout << "Item: " << stat.item_mem_ << " byte" << std::endl;
        std::cout << "Stock: " << stat.stock_mem_ << " + " << stat.stock_disk_ << " byte"
                  << std::endl;
        std::cout << "History: " << stat.history_mem_ << " byte" << std::endl;
        std::cout << "--------------------------------------------" << std::endl;
    }
    uint64_t mem_total = stat.warehouse_mem_ + stat.district_mem_ + stat.customer_mem_ +
                         stat.orderline_mem_ + stat.item_mem_ + stat.stock_mem_;
    uint64_t disk_total = stat.customer_disk_ + stat.orderline_disk_ + stat.stock_disk_;
    std::cout << "Mem: " << mem_total << ", " << "Disk: " << disk_total << " byte" << std::endl;
    uint64_t others = stat.history_mem_ + stat.neworder_mem_ + stat.order_mem_;
    std::cout << "Mem: " << mem_total << ", " << "Disk: " << disk_total << " byte " << "Other: "
              << others << " byte" << std::endl;
    std::cout << "Total: " << mem_total + disk_total << " byte" << std::endl;
}