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

void tableSize(TPCCTables *tables, bool is_initial, bool is_compressed);

void Blitz(TPCCTables *tables);

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
    tableSize(tables, true, false);

    switch (mode) {
        case GenerateCSV: {
            // download the date
            tables->OrderlineToCSV(num_warehouses);
            tables->StockToCSV(num_warehouses);
            tables->CustomerToCSV(num_warehouses);
            tables->HistoryToCSV(num_warehouses);
        }
        case Benchmark: {
            printf("Learning %ld warehouses...\n", num_warehouses);
            // Transform tables into blitz format and train compression model
            OrderLineBlitz order_line_blitz;
            tables->OrderLineToBlitz(order_line_blitz, num_warehouses);
            static db_compress::RelationCompressor ol_compressor("ol_model.blitz",
                                                                 order_line_blitz.schema(),
                                                                 order_line_blitz.compressionConfig(),
                                                                 kBlockSize);
            BlitzLearning(order_line_blitz, ol_compressor);
            db_compress::RelationDecompressor ol_decompressor("ol_model.blitz",
                                                              order_line_blitz.schema(),
                                                              kBlockSize);
            ol_decompressor.InitWithoutIndex();
            tables->MountCompressor(ol_compressor, ol_decompressor, num_warehouses, "orderline");
            db_compress::NextTableAttrInterpreter();

            // stock
            StockBlitz stock_blitz;
            tables->StockToBlitz(stock_blitz, num_warehouses);
            db_compress::RelationCompressor stock_compressor("stock_model.blitz",
                                                             stock_blitz.schema(),
                                                             stock_blitz.compressionConfig(),
                                                             kBlockSize);
            BlitzLearning(stock_blitz, stock_compressor);
            db_compress::RelationDecompressor stock_decompressor("stock_model.blitz",
                                                                 stock_blitz.schema(), kBlockSize);
            stock_decompressor.InitWithoutIndex();
            tables->MountCompressor(stock_compressor, stock_decompressor, num_warehouses, "stock");
            db_compress::NextTableAttrInterpreter();

            // customer
            CustomerBlitz cust_blitz;
            tables->CustomerToBlitz(cust_blitz, num_warehouses);
            db_compress::RelationCompressor cust_compressor("cust_model.blitz", cust_blitz.schema(),
                                                            cust_blitz.compressionConfig(),
                                                            kBlockSize);
            BlitzLearning(cust_blitz, cust_compressor);
            db_compress::RelationDecompressor cust_decompressor("cust_model.blitz",
                                                                cust_blitz.schema(),
                                                                kBlockSize);
            cust_decompressor.InitWithoutIndex();
            tables->MountCompressor(cust_compressor, cust_decompressor, num_warehouses, "customer");
            db_compress::NextTableAttrInterpreter();

            tableSize(tables, true, true);

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
            tableSize(tables, false, true);
        }
    }
    return 0;
}

void welcome(int argc, const char *const *argv) {
    if (argc == 2) {
        num_warehouses = strtol(argv[1], NULL, 10);
    } else if (argc == 3) {
        num_warehouses = strtol(argv[1], NULL, 10);
        bool is_benchmark = std::stoi(argv[2]);
        if (is_benchmark) mode = Benchmark;
        else mode = GenerateCSV;
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

void tableSize(TPCCTables *tables, bool is_initial, bool is_compressed) {
    int ini_warehouses = tables->warehouseSize(num_warehouses);
    int ini_districts = tables->districtSize(num_warehouses);
    int ini_customers = tables->customerSize(num_warehouses);
    int ini_orders = tables->orderSize(num_warehouses, 0);
    int ini_orderline = tables->orderlineSize(num_warehouses, 0);
    int ini_neworders = tables->newOrderSize();
    int ini_items = tables->itemSize();
    int ini_stocks = tables->stockSize(num_warehouses);
    int ini_history = tables->historySize();

    int com_customers = tables->customerBlitzSize(num_warehouses);
    int com_stock = tables->stockBlitzSize(num_warehouses);
    int com_orderline = tables->orderlineBlitzSize(num_warehouses, NUM_TRANSACTIONS);

    if (is_initial && !is_compressed) {
        std::cout << "------------ Initial and uncompressed Size ------------ \n";
        std::cout << "Warehouse: " << ini_warehouses << " byte" << std::endl;
        std::cout << "District: " << ini_districts << " byte" << std::endl;
        std::cout << "Customer: " << ini_customers << " byte" << std::endl;
        std::cout << "Order: " << ini_orders << " byte" << std::endl;
        std::cout << "Orderline: " << ini_orderline << " byte" << std::endl;
        std::cout << "NewOrder: " << ini_neworders << " byte" << std::endl;
        std::cout << "Item: " << ini_items << " byte" << std::endl;
        std::cout << "Stock: " << ini_stocks << " byte" << std::endl;
        std::cout << "History: " << ini_history << " byte" << std::endl;
        int32_t total = ini_warehouses + ini_districts + ini_customers + ini_orders + ini_orderline +
                        ini_neworders + ini_items + ini_stocks + ini_history;
        std::cout << "Total: " << total << " byte" << std::endl;
    } else if (is_initial && is_compressed) {
        std::cout << "------------ Initial but compressed Size ------------ \n";
        std::cout << "Warehouse: " << ini_warehouses << " byte" << std::endl;
        std::cout << "District: " << ini_districts << " byte" << std::endl;
        std::cout << "Customer: " << ini_customers << " byte" << " --> " << com_customers << " byte"
                  << std::endl;
        std::cout << "Order: " << ini_orders << " byte" << std::endl;
        std::cout << "Orderline: " << ini_orderline << " byte" << " --> " << com_orderline
                  << " byte" << std::endl;
        std::cout << "NewOrder: " << ini_neworders << " byte" << std::endl;
        std::cout << "Item: " << ini_items << " byte" << std::endl;
        std::cout << "Stock: " << ini_stocks << " byte" << " --> " << com_stock << " byte"
                  << std::endl;
        std::cout << "History: " << ini_history << " byte" << std::endl;
    } else if (!is_initial && is_compressed) {
        std::cout << "----------- After transactions Size ------------ \n";
        std::cout << "Warehouse: " << ini_warehouses << " byte" << std::endl;
        std::cout << "District: " << ini_districts << " byte" << std::endl;
        std::cout << "Customer: " << com_customers << " byte" << std::endl;
        std::cout << "Order: " << ini_orders << " byte" << std::endl;
        std::cout << "Orderline: " << com_orderline << " byte" << std::endl;
        std::cout << "NewOrder: " << ini_neworders << " byte" << std::endl;
        std::cout << "Item: " << ini_items << " byte" << std::endl;
        std::cout << "Stock: " << com_stock << " byte" << std::endl;
        std::cout << "History: " << ini_history << " byte" << std::endl;
        int32_t total = ini_warehouses + ini_districts + com_customers + ini_orders + com_orderline +
                        ini_neworders + ini_items + com_stock + ini_history;
        std::cout << "Total: " << total << " byte" << std::endl;
    } else {
        std::cout << "Meaningless" << std::endl;
    }
}
