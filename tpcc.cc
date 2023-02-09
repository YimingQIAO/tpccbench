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

static const int NUM_TRANSACTIONS = 200000;
enum Mode {
    GenerateCSV,
    Benchmark
};
static Mode mode;
static long num_warehouses = 1;

void tableSize(TPCCTables *tables);

void welcome(int argc, const char *const *argv);

int main(int argc, const char *argv[]) {
    welcome(argc, argv);

    TPCCTables *tables = new TPCCTables();
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
    tableSize(tables);

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
            printf("Running... ");
            fflush(stdout);

            uint64_t nanoseconds = 0;
            for (int i = 0; i < NUM_TRANSACTIONS; ++i) {
                nanoseconds += client.doOne();
            }
            uint64_t microseconds = nanoseconds / 1000;
            printf("%d transactions in %" PRId64 " ms = %f txns/s\n", NUM_TRANSACTIONS,
                   (microseconds + 500) / 1000,
                   NUM_TRANSACTIONS / (double) microseconds * 1000000.0);
            tableSize(tables);
            break;
        }
        default:
            printf("Unknown mode\n");
    }
    return 0;
}

void welcome(int argc, const char *const *argv) {
    if (argc == 2) {
        num_warehouses = strtol(argv[1], NULL, 10);
    } else if (argc == 3) {
        num_warehouses = strtol(argv[1], NULL, 10);
        bool is_download = std::stoi(argv[2]);
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

void tableSize(TPCCTables *tables) {
    int ini_warehouses = tables->warehouseSize(num_warehouses);
    int ini_districts = tables->districtSize(num_warehouses);
    int ini_customers = tables->customerSize(num_warehouses);
    int ini_orders = tables->orderSize(num_warehouses, 0);
    int ini_orderline = tables->orderlineSize(num_warehouses, 0);
    int ini_neworders = tables->newOrderSize();
    int ini_items = tables->itemSize();
    int ini_stocks = tables->stockSize(num_warehouses);
    int ini_history = tables->historySize();

    std::cout << "------------ Uncompressed Size ------------ \n";
    std::cout << "Warehouse: " << ini_warehouses << " byte" << std::endl;
    std::cout << "District: " << ini_districts << " byte" << std::endl;
    std::cout << "Customer: " << ini_customers << " byte" << std::endl;
    std::cout << "Order: " << ini_orders << " byte" << std::endl;
    std::cout << "Orderline: " << ini_orderline << " byte" << std::endl;
    std::cout << "NewOrder: " << ini_neworders << " byte" << std::endl;
    std::cout << "Item: " << ini_items << " byte" << std::endl;
    std::cout << "Stock: " << ini_stocks << " byte" << std::endl;
    std::cout << "History: " << ini_history << " byte" << std::endl;
    int32_t total =
            ini_warehouses + ini_districts + ini_customers + ini_orders + ini_orderline +
            ini_neworders + ini_items + ini_stocks + ini_history;
    std::cout << "Total: " << total << " byte" << std::endl;
}
