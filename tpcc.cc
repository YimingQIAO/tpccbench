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

static const int NUM_TRANSACTIONS = 30000;

enum Mode {
    GenerateCSV,
    Benchmark
};
static Mode mode;
static long num_warehouses = 1;
static double memory_size;

void tableSize(TPCCTables *tables);

void welcome(int argc, const char *const *argv);

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
            printf("Load Compressor...");
            fflush(stdout);
            // stock
            {
                std::vector<std::vector<std::string>> samples;
                tables->StockToRaman(num_warehouses, samples);
                static RamanCompressor forest = RamanLearning(samples);
                tables->MountCompression(&forest, "stock");
            }
            // order
            {
                std::vector<std::vector<std::string>> samples;
                tables->OrderToRaman(num_warehouses, samples);
                static RamanCompressor forest = RamanLearning(samples);
                tables->MountCompression(&forest, "order");

            }
            // orderline
            {
                std::vector<std::vector<std::string>> samples;
                tables->OrderlineToRaman(num_warehouses, samples);
                static RamanCompressor forest = RamanLearning(samples);
                tables->MountCompression(&forest, "orderline");
            }

            // customer
            {
                std::vector<std::vector<std::string>> samples;
                tables->CustomerToRaman(num_warehouses, samples);
                static RamanCompressor forest = RamanLearning(samples);
                tables->MountCompression(&forest, "customer");

            }
            // history
            {
                std::vector<std::vector<std::string>> samples;
                tables->HistoryToRaman(samples);
                static RamanCompressor forest = RamanLearning(samples);
                tables->MountCompression(&forest, "history");

            }
            printf("Done\n");

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

void tableSize(TPCCTables *tables) {
    // memory size
    int64_t ini_warehouses = tables->warehouseSize();
    int64_t ini_districts = tables->districtSize();
    int64_t ini_customers = tables->customerSize();
    int64_t ini_orders = tables->orderSize();
    int64_t ini_orderline = tables->orderlineSize();
    int64_t ini_neworders = tables->newOrderSize();
    int64_t ini_items = tables->itemSize();
    int64_t ini_stocks = tables->stockSize();
    int64_t ini_history = tables->historySize();

    // disk size
    int64_t disk_stock = tables->diskTableSize("stock");
    int64_t disk_ol = tables->diskTableSize("orderline");
    int64_t disk_c = tables->diskTableSize("customer");

    // raman dict size
    int64_t dict_stock = tables->RamanDictSize("stock");
    int64_t dict_customer = tables->RamanDictSize("customer");
    int64_t dict_order = tables->RamanDictSize("order");
    int64_t dict_orderline = tables->RamanDictSize("orderline");
    int64_t dict_history = tables->RamanDictSize("history");

    std::cout << "------------ After Transaction Size ------------ \n";
    std::cout << "[Table Name]: " << "[Memory Size] + [Disk Size]" << std::endl;
    std::cout << "Warehouse: " << ini_warehouses << " byte" << std::endl;
    std::cout << "District: " << ini_districts << " byte" << std::endl;
    std::cout << "Customer: " << ini_customers << " + " << disk_c << " + " << dict_customer << " byte" << std::endl;
    std::cout << "Order: " << ini_orders << " byte" << " + " << dict_order << " byte" << std::endl;
    std::cout << "Orderline: " << ini_orderline << " + " << disk_ol << " + " << dict_orderline << " byte" << std::endl;
    std::cout << "NewOrder: " << ini_neworders << " byte" << std::endl;
    std::cout << "Item: " << ini_items << " byte" << std::endl;
    std::cout << "Stock: " << ini_stocks << " + " << disk_stock << " + " << dict_stock << " byte" << std::endl;
    std::cout << "History: " << ini_history << " + " << dict_history << " byte" << std::endl;
    int64_t total =
            ini_warehouses + ini_districts + ini_customers + ini_orders + ini_orderline + ini_neworders + ini_items +
            ini_stocks + ini_history +
            disk_stock + disk_c + disk_ol +
            dict_history + dict_order + dict_orderline + dict_stock + dict_customer;
    std::cout << "Total: " << total << " byte" << std::endl;
}
