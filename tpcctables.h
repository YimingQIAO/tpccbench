#ifndef TPCCTABLES_H__
#define TPCCTABLES_H__

#include <map>
#include <set>
#include <vector>
#include <limits>

#include "btree.h"
#include "tpccdb.h"
#include "disk_storage.h"
#include "tpcc_raman.h"


class CustomerByNameOrdering {
public:
    bool operator()(const Customer *a, const Customer *b) const;

    bool operator()(const Tuple<Customer> *ta, const Tuple<Customer> *tb) const;
};

// Stores all the tables in TPC-C
class TPCCTables : public TPCCDB {
public:
    explicit TPCCTables(double memory_size);

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

    void insertStock(const Stock &stock, bool is_orig, bool relearn);

    Stock *findStock(int32_t w_id, int32_t s_id, bool is_orig);

    void insertDistrict(const District &district);

    District *findDistrict(int32_t w_id, int32_t d_id);

    void insertCustomer(const Customer &customer, bool is_orig, bool relearn);

    Customer *findCustomer(int32_t w_id, int32_t d_id, int32_t c_id, bool is_orig);

    // Finds all customers that match (w_id, d_id, *, c_last), taking the n/2th one (rounded up).
    Customer *findCustomerByName(int32_t w_id, int32_t d_id, const char *c_last);

    // Stores order in the database. Returns a pointer to the database's tuple.
    Order *insertOrder(const Order &order, bool is_orig, bool relearn);

    Order *findOrder(int32_t w_id, int32_t d_id, int32_t o_id, bool is_orig);

    Order *findLastOrderByCustomer(int32_t w_id, int32_t d_id, int32_t c_id);

    // Stores orderline in the database. Returns a pointer to the database's tuple.
    OrderLine *insertOrderLine(const OrderLine &orderline, bool is_orig, bool relearn);

    OrderLine *
    findOrderLine(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number, bool is_orig);

    // Creates a new order in the database. Returns a pointer to the database's tuple.
    NewOrder *insertNewOrder(int32_t w_id, int32_t d_id, int32_t o_id);

    NewOrder *findNewOrder(int32_t w_id, int32_t d_id, int32_t o_id);

    const std::vector<const History *> &history() const { return history_; }

    // Stores order in the database. Returns a pointer to the database's tuple.
    History *insertHistory(const History &history, bool is_orig, bool relearn);

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

    // -------------------- raman ----------------------
    void StockToRaman(int64_t num_warehouses, std::vector<std::vector<std::string>> &samples) {
        for (int32_t w_id = 1; w_id <= num_warehouses; w_id++) {
            for (int32_t s_id = 1; s_id <= Stock::NUM_STOCK_PER_WAREHOUSE; s_id++) {
                Stock *stock = findStock(w_id, s_id, true);
                if (stock == nullptr) throw std::runtime_error("Stock not found in StockToRaman");
                // if (samples.size() > 50 * Stock::NUM_STOCK_PER_WAREHOUSE) return;
                samples.push_back(stock->toRamanFormat());
            }
        }
    }

    void CustomerToRaman(int64_t num_warehouses, std::vector<std::vector<std::string>> &samples) {
        for (int32_t w_id = 1; w_id <= num_warehouses; w_id++) {
            for (int32_t d_id = 1; d_id <= District::NUM_PER_WAREHOUSE; d_id++) {
                for (int32_t c_id = 1; c_id <= Customer::NUM_PER_DISTRICT; c_id++) {
                    Customer *customer = findCustomer(w_id, d_id, c_id, true);
                    if (customer == nullptr)
                        throw std::runtime_error("Customer not found in CustomerToraman");
                    // if (samples.size() > 50 * District::NUM_PER_WAREHOUSE * Customer::NUM_PER_DISTRICT) return;
                    samples.push_back(customer->toRamanFormat());
                }
            }
        }
    }

    void OrderToRaman(int64_t num_warehouses, std::vector<std::vector<std::string>> &samples) {
        int64_t key = std::numeric_limits<int64_t>::max();
        Order *order = nullptr;
        while (orders_.findLastLessThan(key, &order, &key)) {
            samples.push_back(order->toRamanFormat());
        }
    }

    void OrderlineToRaman(int64_t num_warehouses, std::vector<std::vector<std::string>> &samples) {
        int64_t key = std::numeric_limits<int64_t>::max();
        OrderLine *orderline = nullptr;
        while (orderlines_.findLastLessThan(key, &orderline, &key)) {
            samples.push_back(orderline->toRamanFormat());
        }
    }

    void HistoryToRaman(std::vector<std::vector<std::string>> &samples) {
        for (const History *h: history_) samples.push_back(h->toRamanFormat());
    }

    void MountCompression(RamanCompressor *forest, const std::string &table_name);

    inline uint32_t RamanDictSize(const std::string &name) {
        if (name == "stock") {
            return forest_stock_->Size() + block_stock_.Size();
        } else if (name == "customer") {
            return forest_customer_->Size() + block_customer_.Size();
        } else if (name == "order") {
            return forest_order_->Size() + block_order_.Size();
        } else if (name == "orderline") {
            return forest_orderline_->Size() + block_orderline_.Size();
        } else if (name == "history") {
            return forest_history_->Size() + block_history_.Size();
        } else {
            throw std::runtime_error("Unknown table name");
        }
    }

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
    Tuple<BitStream> stock_tuple_disk_;

    uint32_t num_mem_orderline = 0;
    uint32_t num_disk_orderline = 0;
    Tuple<BitStream> ol_tuple_disk_;

    uint32_t num_mem_customer = 0;
    uint32_t num_disk_customer = 0;
    Tuple<BitStream> customer_tuple_disk_;

    // disk storage
    std::string kStockFileName, kCustomerFileName, kOrderlineFileName;
    uint64_t kStockMT;
    uint64_t kCustomerMT;
    uint64_t kOrderlineMT;

    // raman
    RamanCompressor *forest_stock_;
    BPlusTree<int32_t, Tuple<BitStream> *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> stock_raman;
    Stock stock_raman_buf_;
    RamanTupleBuffer<Stock> block_stock_;

    RamanCompressor *forest_customer_;
    BPlusTree<int32_t, Tuple<BitStream> *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> customer_raman;
    Customer customer_raman_buf_;
    RamanTupleBuffer<Customer> block_customer_;

    RamanCompressor *forest_order_;
    BPlusTree<int64_t, Tuple<BitStream> *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> order_raman;
    Order order_raman_buf_;
    RamanTupleBuffer<Order> block_order_;

    RamanCompressor *forest_orderline_;
    BPlusTree<int64_t, Tuple<BitStream> *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> orderline_raman;
    OrderLine orderline_raman_buf_;
    RamanTupleBuffer<OrderLine> block_orderline_;

    RamanCompressor *forest_history_;
    std::vector<BitStream> history_raman;
    RamanTupleBuffer<History> block_history_;
};

#endif
