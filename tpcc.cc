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
static bool mode = 0;
static long num_warehouses = 1;

int main(int argc, const char *argv[]) {
    if (argc == 2) {
        num_warehouses = strtol(argv[1], NULL, 10);
    } else if (argc == 3) {
        num_warehouses = strtol(argv[1], NULL, 10);
        mode = strtol(argv[2], NULL, 10);
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
                            Customer::NUM_PER_DISTRICT,
                            NewOrder::INITIAL_NUM_PER_DISTRICT);

    int64_t begin = clock->getMicroseconds();
    generator.makeItemsTable(tables);
    for (int i = 0; i < num_warehouses; ++i) {
        generator.makeWarehouse(tables, i + 1);
    }
    int64_t end = clock->getMicroseconds();
    printf("%" PRId64 " ms\n", (end - begin + 500) / 1000);

    uint32_t initial_total_size, uncompressed_ol_size;
    std::cout << "----------------- Initial ----------------- \n";
    tables->DBSize(num_warehouses, initial_total_size, uncompressed_ol_size,
                   NUM_TRANSACTIONS, true);
    if (mode) {
        // download the date
        tables->OrderlineToCSV(num_warehouses);
        tables->StockToCSV(num_warehouses);
        tables->CustomerToCSV(num_warehouses);
    } else {
        // Transform table into blitz format and train compression model
        // order line
        OrderLineBlitz order_line_blitz;
        tables->OrderLineToBlitz(order_line_blitz, num_warehouses);
        db_compress::RelationCompressor ol_compressor("ol_model.blitz", order_line_blitz.schema(),
                                                      order_line_blitz.compressionConfig(),
                                                      kBlockSize);
        BlitzLearning(order_line_blitz, ol_compressor);
        db_compress::RelationDecompressor ol_decompressor("ol_model.blitz",
                                                          order_line_blitz.schema(), kBlockSize);
        ol_decompressor.InitWithoutIndex();
        tables->MountCompressor(ol_compressor, ol_decompressor, num_warehouses, "orderline");
        db_compress::NextTableAttrInterpreter();

        // stock
        StockBlitz stock_blitz;
        tables->StockToBlitz(stock_blitz, num_warehouses);
        db_compress::RelationCompressor stock_compressor("stock_model.blitz", stock_blitz.schema(),
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
                                                        cust_blitz.compressionConfig(), kBlockSize);
        BlitzLearning(cust_blitz, cust_compressor);
        db_compress::RelationDecompressor cust_decompressor("cust_model.blitz", cust_blitz.schema(),
                                                            kBlockSize);
        cust_decompressor.InitWithoutIndex();
        tables->MountCompressor(cust_compressor, cust_decompressor, num_warehouses, "customer");
        db_compress::NextTableAttrInterpreter();

        std::cout << "---------------- Compressed ---------------- \n";
        uint32_t orderline_blitz_size = tables->BlitzSize(num_warehouses, NUM_TRANSACTIONS,
                                                          "orderline");
        printf("orderline blitz size: %u\n", orderline_blitz_size);

        // Change the constants for run
        random = new tpcc::RealRandomGenerator();
        random->setC(tpcc::NURandC::makeRandomForRun(random, cLoad));

        // Client owns all the parameters
        TPCCClient client(clock, random, tables, Item::NUM_ITEMS, static_cast<int>(num_warehouses),
                          District::NUM_PER_WAREHOUSE, Customer::NUM_PER_DISTRICT);
        printf("Running... ");
        fflush(stdout);

        uint64_t nanoseconds = 0;
        for (int i = 0; i < NUM_TRANSACTIONS; ++i) {
            nanoseconds += client.doOne();
        }
        uint64_t microseconds = nanoseconds / 1000;
        printf("%d transactions in %" PRId64 " ms = %f txns/s\n", NUM_TRANSACTIONS,
               (microseconds + 500) / 1000, NUM_TRANSACTIONS / (double) microseconds * 1000000.0);
        orderline_blitz_size = tables->BlitzSize(num_warehouses, NUM_TRANSACTIONS, "orderline");
        int32_t stock_blitz_size = tables->BlitzSize(num_warehouses, NUM_TRANSACTIONS, "stock");
        int32_t customer_blitz_size = tables->BlitzSize(num_warehouses, NUM_TRANSACTIONS,
                                                        "customer");
        printf("orderline blitz size: %u\nstock blitz size: %u\ncustomer blitz size: %u\n",
               orderline_blitz_size,
               stock_blitz_size, customer_blitz_size);

        //        std::cout << "----------------- Result ----------------- \n";
        //        double ol_compression_ratio =
        //                (double) initial_ol_cm_size / uncompressed_ol_size;
        //
        //        uint32_t end_total_size;
        //        uint32_t initial_cm_total_size, end_cm_total_size;
        //        uint32_t end_ol_cm_size;
        //
        //        initial_cm_total_size =
        //                initial_total_size - uncompressed_ol_size + initial_ol_cm_size;
        //        printf("After load Size: %u -> %u (%.3f)\n", initial_total_size,
        //               initial_cm_total_size,
        //               (float) (initial_cm_total_size) / initial_total_size);
        //
        //        tables->DBSize(num_warehouses, end_total_size, uncompressed_ol_size,
        //                       NUM_TRANSACTIONS, false);
        //        end_ol_cm_size = tables->OrderlineBlitzSize(num_warehouses, NUM_TRANSACTIONS);
        //        end_cm_total_size = end_total_size - uncompressed_ol_size + end_ol_cm_size;
        //        end_total_size = end_total_size - uncompressed_ol_size +
        //                         (end_ol_cm_size / ol_compression_ratio);
        //        printf("After transaction Size: %u -> %u (%.3f)\n", end_total_size,
        //               end_cm_total_size, (float) (end_cm_total_size) / end_total_size);
    }
    return 0;
}
