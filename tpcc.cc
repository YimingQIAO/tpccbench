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
static double memory_size;

void tableSize(TPCCTables *tables, bool is_initial, bool is_compressed);

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
            begin = clock->getMicroseconds();
            int32_t model_id = rand();
            // orderline
            {
                OrderLineBlitz order_line_blitz;
                tables->OrderLineToBlitz(order_line_blitz, num_warehouses);
                std::string ol_model_name = std::to_string(model_id) + "ol_model.blitz";
                static db_compress::RelationCompressor ol_compressor(ol_model_name.c_str(),
                                                                     order_line_blitz.schema(),
                                                                     order_line_blitz.compressionConfig(),
                                                                     kBlockSize);
                BlitzLearning(order_line_blitz, ol_compressor);
                static db_compress::RelationDecompressor ol_decompressor(ol_model_name.c_str(),
                                                                         order_line_blitz.schema(),
                                                                         kBlockSize);
                ol_decompressor.InitWithoutIndex();
                tables->MountCompressor(ol_compressor, ol_decompressor, num_warehouses, "orderline");
                db_compress::NextTableAttrInterpreter();
            }
            // stock
            {
                StockBlitz stock_blitz;
                tables->StockToBlitz(stock_blitz, num_warehouses);
                std::string stock_model_name = std::to_string(model_id) + "stock_model.blitz";
                static db_compress::RelationCompressor stock_compressor(stock_model_name.c_str(),
                                                                        stock_blitz.schema(),
                                                                        stock_blitz.compressionConfig(),
                                                                        kBlockSize);
                BlitzLearning(stock_blitz, stock_compressor);
                static db_compress::RelationDecompressor stock_decompressor(stock_model_name.c_str(),
                                                                            stock_blitz.schema(), kBlockSize);
                stock_decompressor.InitWithoutIndex();
                tables->MountCompressor(stock_compressor, stock_decompressor, num_warehouses, "stock");
                db_compress::NextTableAttrInterpreter();
            }
            // customer
            {
                CustomerBlitz cust_blitz;
                tables->CustomerToBlitz(cust_blitz, num_warehouses);
                std::string customer_model_name = std::to_string(model_id) + "customer_model.blitz";
                static db_compress::RelationCompressor cust_compressor(customer_model_name.c_str(),
                                                                       cust_blitz.schema(),
                                                                       cust_blitz.compressionConfig(),
                                                                       kBlockSize);
                BlitzLearning(cust_blitz, cust_compressor);
                static db_compress::RelationDecompressor cust_decompressor(customer_model_name.c_str(),
                                                                           cust_blitz.schema(),
                                                                           kBlockSize);
                cust_decompressor.InitWithoutIndex();
                tables->MountCompressor(cust_compressor, cust_decompressor, num_warehouses, "customer");
                db_compress::NextTableAttrInterpreter();
            }
            end = clock->getMicroseconds();
            printf("%" PRId64 " ms\n", (end - begin + 500) / 1000);
            fflush(stdout);
            tableSize(tables, true, true);

            // Change the constants for run
            random = new tpcc::RealRandomGenerator();
            random->setC(tpcc::NURandC::makeRandomForRun(random, cLoad));

            // Client owns all the parameters
            TPCCClient client(clock, random, tables, Item::NUM_ITEMS, static_cast<int>(num_warehouses),
                              District::NUM_PER_WAREHOUSE, Customer::NUM_PER_DISTRICT, now);
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

void tableSize(TPCCTables *tables, bool is_initial, bool is_compressed) {
    int64_t ini_warehouses = tables->TableSize("warehouse");
    int64_t ini_districts = tables->TableSize("district");
    int64_t ini_customers = tables->TableSize("customer");
    int64_t ini_orders = tables->TableSize("order");
    int64_t ini_orderline = tables->TableSize("orderline");
    int64_t ini_neworders = tables->TableSize("newOrder");
    int64_t ini_items = tables->TableSize("item");
    int64_t ini_stocks = tables->TableSize("stock");
    int64_t ini_history = tables->TableSize("history");

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
        int64_t total =
                ini_warehouses + ini_districts + ini_customers + ini_orders + ini_orderline +
                ini_neworders + ini_items + ini_stocks + ini_history;
        std::cout << "Total: " << total << " byte" << std::endl;
    } else if (is_initial && is_compressed) {
        int64_t com_customers = tables->TableSize("customer blitz");
        int64_t com_stocks = tables->TableSize("stock blitz");
        int64_t com_orderline = tables->TableSize("orderline blitz");

        std::cout << "------------ Initial but compressed Size ------------ \n";
        std::cout << "Warehouse: " << ini_warehouses << " byte" << std::endl;
        std::cout << "District: " << ini_districts << " byte" << std::endl;
        std::cout << "Customer: " << ini_customers << " byte"
                  << " --> " << com_customers << " byte"
                  << std::endl;
        std::cout << "Order: " << ini_orders << " byte" << std::endl;
        std::cout << "Orderline: " << ini_orderline << " byte"
                  << " --> " << com_orderline
                  << " byte" << std::endl;
        std::cout << "NewOrder: " << ini_neworders << " byte" << std::endl;
        std::cout << "Item: " << ini_items << " byte" << std::endl;
        std::cout << "Stock: " << ini_stocks << " byte"
                  << " --> " << com_stocks << " byte"
                  << std::endl;
        std::cout << "History: " << ini_history << " byte" << std::endl;
        int64_t total =
                ini_warehouses + ini_districts + ini_customers + ini_orders + ini_orderline +
                ini_neworders + ini_items + ini_stocks + ini_history;
        std::cout << "Orig Total: " << total << " byte" << std::endl;
    } else if (!is_initial && is_compressed) {
        ini_history = ini_history * 17 / 96;
        ini_orders = ini_orders * 12 / 61;

        int64_t com_customers = tables->TableSize("customer blitz");
        int64_t com_stocks = tables->TableSize("stock blitz");
        int64_t com_orderline = tables->TableSize("orderline blitz");

        // disk size
        int64_t disk_stock = tables->diskTableSize("stock");
        int64_t disk_ol = tables->diskTableSize("orderline");
        int64_t disk_c = tables->diskTableSize("customer");

        std::cout << "------------ After Transaction Size ------------ \n";
        std::cout << "[Table Name]: " << "[Memory Size] + [Disk Size]" << std::endl;
        std::cout << "Warehouse: " << ini_warehouses << " byte" << std::endl;
        std::cout << "District: " << ini_districts << " byte" << std::endl;
        std::cout << "Customer: " << com_customers << " + " << disk_c << " byte" << std::endl;
        std::cout << "Order: " << ini_orders << " byte" << std::endl;
        std::cout << "Orderline: " << com_orderline << " + " << disk_ol << " byte" << std::endl;
        std::cout << "NewOrder: " << ini_neworders << " byte" << std::endl;
        std::cout << "Item: " << ini_items << " byte" << std::endl;
        std::cout << "Stock: " << com_stocks << " + " << disk_stock << " byte" << std::endl;
        std::cout << "History: " << ini_history << " byte" << std::endl;
        int64_t total =
                ini_warehouses + ini_districts + com_customers + ini_orders + com_orderline +
                ini_neworders + ini_items + com_stocks + ini_history;
        std::cout << "Total: " << total << " byte" << std::endl;
    } else {
        std::cout << "Meaningless" << std::endl;
    }
}
