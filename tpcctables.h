#ifndef TPCCTABLES_H__
#define TPCCTABLES_H__

#include <map>
#include <set>
#include <vector>

#include "btree.h"
#include "tpccdb.h"
#include "disk_storage.h"
#include "tpcc_zstd.h"

class CustomerByNameOrdering {
public:
    bool operator()(const Customer *a, const Customer *b) const;

    bool operator()(const Tuple<Customer> *ta, const Tuple<Customer> *tb) const;
};

// Stores all the tables in TPC-C
class TPCCTables : public TPCCDB {
public:
    virtual ~TPCCTables();

    virtual int32_t stockLevel(int32_t warehouse_id, int32_t district_id, int32_t threshold);

    virtual void orderStatus(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                             OrderStatusOutput *output);

    virtual void orderStatus(int32_t warehouse_id, int32_t district_id, const char *c_last,
                             OrderStatusOutput *output);

    virtual bool newOrder(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                          const std::vector<NewOrderItem> &items, const char *now,
                          NewOrderOutput *output, TPCCUndo **undo);

    virtual bool newOrderHome(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                              const std::vector<NewOrderItem> &items, const char *now,
                              NewOrderOutput *output, TPCCUndo **undo);

    virtual bool newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
                                const std::vector<NewOrderItem> &items,
                                std::vector<int32_t> *out_quantities,
                                TPCCUndo **undo);

    virtual void payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                         int32_t c_district_id, int32_t customer_id, float h_amount,
                         const char *now,
                         PaymentOutput *output, TPCCUndo **undo);

    virtual void payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                         int32_t c_district_id, const char *c_last, float h_amount, const char *now,
                         PaymentOutput *output, TPCCUndo **undo);

    virtual void paymentHome(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                             int32_t c_district_id, int32_t c_id, float h_amount, const char *now,
                             PaymentOutput *output, TPCCUndo **undo);

    virtual void paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                               int32_t c_district_id, int32_t c_id, float h_amount,
                               PaymentOutput *output,
                               TPCCUndo **undo);

    virtual void paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                               int32_t c_district_id, const char *c_last, float h_amount,
                               PaymentOutput *output,
                               TPCCUndo **undo);

    virtual void delivery(int32_t warehouse_id, int32_t carrier_id, const char *now,
                          std::vector<DeliveryOrderInfo> *orders, TPCCUndo **undo);

    virtual bool hasWarehouse(int32_t warehouse_id) { return findWarehouse(warehouse_id) != NULL; }

    virtual void applyUndo(TPCCUndo *undo);

    virtual void freeUndo(TPCCUndo *undo) {
        assert(undo != NULL);
        delete undo;
    }

    void reserveItems(int size) { items_.reserve(size); }

    // Copies item into the item table.
    void insertItem(const Item &item);

    Item *findItem(int32_t i_id);

    void insertWarehouse(const Warehouse &warehouse);

    Warehouse *findWarehouse(int32_t w_id);

    void insertStock(const Stock &stock, bool is_orig);

    Stock *findStock(int32_t w_id, int32_t s_id, bool is_orig);

    void insertDistrict(const District &district);

    District *findDistrict(int32_t w_id, int32_t d_id);

    void insertCustomer(const Customer &customer, bool is_orig);

    Customer *findCustomer(int32_t w_id, int32_t d_id, int32_t c_id, bool is_orig);

    // Finds all customers that match (w_id, d_id, *, c_last), taking the n/2th one (rounded up).
    Customer *findCustomerByName(int32_t w_id, int32_t d_id, const char *c_last);

    // Stores order in the database. Returns a pointer to the database's tuple.
    Order *insertOrder(const Order &order, bool is_orig);

    Order *findOrder(int32_t w_id, int32_t d_id, int32_t o_id, bool is_orig);

    Order *findLastOrderByCustomer(int32_t w_id, int32_t d_id, int32_t c_id);

    // Stores orderline in the database. Returns a pointer to the database's tuple.
    OrderLine *insertOrderLine(const OrderLine &orderline, bool is_orig);

    OrderLine *
    findOrderLine(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number, bool is_orig);

    // Creates a new order in the database. Returns a pointer to the database's tuple.
    NewOrder *insertNewOrder(int32_t w_id, int32_t d_id, int32_t o_id);

    NewOrder *findNewOrder(int32_t w_id, int32_t d_id, int32_t o_id);

    const std::vector<const History *> &history() const { return history_; }

    // Stores order in the database. Returns a pointer to the database's tuple.
    History *insertHistory(const History &history, bool is_orig);

    int64_t itemSize();

    int64_t warehouseSize();

    int64_t districtSize();

    int64_t stockSize();

    int64_t customerSize();

    int64_t orderSize();

    int64_t orderlineSize();

    int64_t newOrderSize();

    int64_t historySize();

    [[nodiscard]] static int64_t diskTableSize(const std::string &file_name);

    void OrderlineToCSV(int64_t num_warehouses);

    void StockToCSV(int64_t num_warehouses);

    void CustomerToCSV(int64_t num_warehouses);

    void HistoryToCSV(int64_t num_warehouses);

    static void DeleteDiskData();

    // -------------------- zstd ----------------------
    void StockToZstd(int64_t num_warehouses, std::vector<Stock> &samples,
                     std::vector<size_t> &sample_sizes) {
        for (int32_t w_id = 1; w_id <= num_warehouses; w_id++) {
            for (int32_t s_id = 1; s_id <= Stock::NUM_STOCK_PER_WAREHOUSE; s_id++) {
                Stock *stock = findStock(w_id, s_id, true);
                if (stock == nullptr) throw std::runtime_error("Stock not found in StockToZstd");
                if (samples.size() > 50 * Stock::NUM_STOCK_PER_WAREHOUSE) return;
                samples.push_back(*stock);
                sample_sizes.push_back(sizeof(Stock));
            }
        }
    }

    void CustomerToZstd(int64_t num_warehouses, std::vector<Customer> &samples,
                        std::vector<size_t> &sample_sizes) {
        for (int32_t w_id = 1; w_id <= num_warehouses; w_id++) {
            for (int32_t d_id = 1; d_id <= District::NUM_PER_WAREHOUSE; d_id++) {
                for (int32_t c_id = 1; c_id <= Customer::NUM_PER_DISTRICT; c_id++) {
                    Customer *customer = findCustomer(w_id, d_id, c_id, true);
                    if (customer == nullptr) throw std::runtime_error("Customer not found in CustomerToZstd");
                    if (samples.size() > 50 * District::NUM_PER_WAREHOUSE * Customer::NUM_PER_DISTRICT) return;
                    samples.push_back(*customer);
                    sample_sizes.push_back(sizeof(Customer));
                }
            }
        }
    }

    void OrderToZstd(int32_t num_warehouses, std::vector<Order> &samples,
                     std::vector<size_t> &sample_sizes) {
        for (int32_t w_id = 1; w_id <= num_warehouses; w_id++) {
            for (int32_t d_id = 1; d_id <= District::NUM_PER_WAREHOUSE; d_id++) {
                for (int32_t o_id = 1; o_id <= Order::INITIAL_ORDERS_PER_DISTRICT; o_id++) {
                    Order *order = findOrder(w_id, d_id, o_id, true);
                    if (order == nullptr) throw std::runtime_error("Order not found in OrderToZstd");
                    if (samples.size() > 50 * District::NUM_PER_WAREHOUSE * Order::INITIAL_ORDERS_PER_DISTRICT) return;
                    samples.push_back(*order);
                    sample_sizes.push_back(sizeof(Order));
                }
            }
        }
    }

    void OrderlineToZstd(int64_t num_warehouses, std::vector<OrderLine> &samples,
                         std::vector<size_t> &sample_sizes) {
        for (int32_t w_id = 1; w_id <= num_warehouses; w_id++) {
            for (int32_t d_id = 1; d_id <= District::NUM_PER_WAREHOUSE; d_id++) {
                for (int32_t o_id = 1; o_id <= OrderLine::INITIAL_QUANTITY; o_id++) {
                    for (int32_t ol_number = 1; ol_number <= Order::MAX_OL_CNT; ol_number++) {
                        OrderLine *orderline = findOrderLine(w_id, d_id, o_id, ol_number, true);
                        if (orderline == nullptr) break;
                        if (samples.size() >
                            50 * District::NUM_PER_WAREHOUSE * Order::INITIAL_ORDERS_PER_DISTRICT * Order::MAX_OL_CNT)
                            return;
                        samples.push_back(*orderline);
                        sample_sizes.push_back(sizeof(OrderLine));
                    }
                }
            }
        }
    }

    void HistoryToZstd(std::vector<History> &samples, std::vector<size_t> &sample_sizes) {
        for (const History *h: history_) {
            samples.push_back(*h);
            sample_sizes.push_back(sizeof(History));
        }
    }

    void MountCompression(ZSTD_CDict_s *cdict, ZSTD_DDict_s *ddict, const std::string &table_name);


    static const int KEYS_PER_INTERNAL = 8;
    static const int KEYS_PER_LEAF = 8;

private:
    static const int STOCK_LEVEL_ORDERS = 20;

// Loads each item from the items table. Returns true if they are all found.
    bool findAndValidateItems(const std::vector<NewOrderItem> &items,
                              std::vector<Item *> *item_tuples);

// Implements order status transaction after the customer tuple has been located.
    void internalOrderStatus(Customer *customer, OrderStatusOutput *output);

// Implements payment transaction after the customer tuple has been located.
    void internalPaymentRemote(int32_t warehouse_id, int32_t district_id, Customer *c,
                               float h_amount, PaymentOutput *output, TPCCUndo **undo);

// Allocates an undo buffer if needed, storing the pointer in *undo.
    void allocateUndo(TPCCUndo **undo) {
        if (undo != NULL && *undo == NULL) {
            *undo = new TPCCUndo();
        }
    }

// TODO: Use a data structure that supports deletes, appends, and sparse ranges.
// Using a vector instead of a BPlusTree reduced the new order run time by 3.65us. This was an
// improvement of 12%. It also saved 4141kB of RSS.
    std::vector<Item> items_;

    BPlusTree<int32_t, Warehouse *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> warehouses_;
    BPlusTree<int32_t, Stock *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> stock_;
    BPlusTree<int32_t, District *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> districts_;
    BPlusTree<int32_t, Customer *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> customers_;
    typedef std::set<Customer *, CustomerByNameOrdering> CustomerByNameSet;
    CustomerByNameSet customers_by_name_;
    BPlusTree<int64_t, Order *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> orders_;
    BPlusTree<int64_t, Order *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> orders_by_customer_;
    BPlusTree<int64_t, OrderLine *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> orderlines_;
    typedef std::map<int64_t, NewOrder *> NewOrderMap;
    NewOrderMap neworders_;
    std::vector<const History *> history_;

    // disk storage
    uint32_t num_mem_stock = 0;
    uint32_t num_disk_stock = 0;

    uint32_t num_mem_orderline = 0;
    uint32_t num_disk_orderline = 0;
    Tuple<std::string> ol_tuple_buf_;

    uint32_t num_mem_customer = 0;
    uint32_t num_disk_customer = 0;

    // zstd
    Stock stock_zstd_buf_;
    ZSTD_CDict_s *cdict_stock = nullptr;
    ZSTD_DDict_s *ddict_stock = nullptr;
    BPlusTree<int32_t, Tuple<std::string> *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> stock_zstd;

    Customer customer_zstd_buf_;
    ZSTD_CDict_s *cdict_c = nullptr;
    ZSTD_DDict_s *ddict_c = nullptr;
    BPlusTree<int32_t, Tuple<std::string> *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> customer_zstd;

    Order order_zstd_buf_;
    ZSTD_CDict_s *cdict_o = nullptr;
    ZSTD_DDict_s *ddict_o = nullptr;
    BPlusTree<int32_t, std::string *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> order_zstd;

    OrderLine ol_zstd_buf_;
    ZSTD_CDict_s *cdict_ol = nullptr;
    ZSTD_DDict_s *ddict_ol = nullptr;
    BPlusTree<int64_t, Tuple<std::string> *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> ol_zstd;

    ZSTD_CDict_s *cdict_h = nullptr;
    std::vector<std::string> history_zstd;
};

#endif
