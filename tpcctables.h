#ifndef TPCCTABLES_H__
#define TPCCTABLES_H__

#include <map>
#include <set>
#include <vector>

#include "blitz.h"
#include "btree.h"
#include "tpccdb.h"
#include "disk_storage.h"
#include "tpcc_stat.h"

class CustomerByNameOrdering {
public:
    bool operator()(const Customer *a, const Customer *b) const;
};

// Stores all the tables in TPC-C
class TPCCTables : public TPCCDB {
public:
    TPCCTables(double memory_size);

    virtual ~TPCCTables();

    virtual int32_t stockLevel(int32_t warehouse_id, int32_t district_id,
                               int32_t threshold);

    virtual void orderStatus(int32_t warehouse_id, int32_t district_id,
                             int32_t customer_id, OrderStatusOutput *output);

    virtual void orderStatus(int32_t warehouse_id, int32_t district_id,
                             const char *c_last, OrderStatusOutput *output);

    virtual bool newOrder(int32_t warehouse_id, int32_t district_id,
                          int32_t customer_id,
                          const std::vector<NewOrderItem> &items, const char *now,
                          NewOrderOutput *output, TPCCUndo **undo);

    virtual bool newOrderHome(int32_t warehouse_id, int32_t district_id,
                              int32_t customer_id,
                              const std::vector<NewOrderItem> &items,
                              const char *now, NewOrderOutput *output,
                              TPCCUndo **undo);

    virtual bool newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
                                const std::vector<NewOrderItem> &items,
                                std::vector<int32_t> *out_quantities,
                                TPCCUndo **undo);

    virtual void payment(int32_t warehouse_id, int32_t district_id,
                         int32_t c_warehouse_id, int32_t c_district_id,
                         int32_t customer_id, float h_amount, const char *now,
                         PaymentOutput *output, TPCCUndo **undo);

    virtual void payment(int32_t warehouse_id, int32_t district_id,
                         int32_t c_warehouse_id, int32_t c_district_id,
                         const char *c_last, float h_amount, const char *now,
                         PaymentOutput *output, TPCCUndo **undo);

    virtual void paymentHome(int32_t warehouse_id, int32_t district_id,
                             int32_t c_warehouse_id, int32_t c_district_id,
                             int32_t c_id, float h_amount, const char *now,
                             PaymentOutput *output, TPCCUndo **undo);

    virtual void paymentRemote(int32_t warehouse_id, int32_t district_id,
                               int32_t c_warehouse_id, int32_t c_district_id,
                               int32_t c_id, float h_amount,
                               PaymentOutput *output, TPCCUndo **undo);

    virtual void paymentRemote(int32_t warehouse_id, int32_t district_id,
                               int32_t c_warehouse_id, int32_t c_district_id,
                               const char *c_last, float h_amount,
                               PaymentOutput *output, TPCCUndo **undo);

    virtual void delivery(int32_t warehouse_id, int32_t carrier_id,
                          const char *now, std::vector<DeliveryOrderInfo> *orders,
                          TPCCUndo **undo);

    virtual bool hasWarehouse(int32_t warehouse_id) {
        return findWarehouse(warehouse_id) != NULL;
    }

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

    void insertStock(const Stock &stock);

    void insertStockBlitz(db_compress::AttrVector &stock, int32_t stop_idx);

    Stock *findStock(int32_t w_id, int32_t s_id);

    db_compress::AttrVector *findStockBlitz(int32_t w_id, int32_t s_id, int32_t stop_idx);

    void insertDistrict(const District &district);

    District *findDistrict(int32_t w_id, int32_t d_id);

    void insertCustomer(const Customer &customer);

    void insertCustomerBlitz(db_compress::AttrVector &customer, int32_t attr_idx,
                             bool single_attr);

    Customer *findCustomer(int32_t w_id, int32_t d_id, int32_t c_id);

    db_compress::AttrVector *
    findCustomerBlitz(int32_t w_id, int32_t d_id, int32_t c_id, int32_t stop_idx);

    // Finds all customers that match (w_id, d_id, *, c_last), taking the n/2th
    // one (rounded up).
    Customer *findCustomerByName(int32_t w_id, int32_t d_id, const char *c_last);

    // Stores order in the database. Returns a pointer to the database's tuple.
    Order *insertOrder(const Order &order);

    Order *findOrder(int32_t w_id, int32_t d_id, int32_t o_id);

    Order *findLastOrderByCustomer(int32_t w_id, int32_t d_id, int32_t c_id);

    // Stores orderline in the database. Returns a pointer to the database's
    // tuple.
    OrderLine *insertOrderLine(const OrderLine &orderline);

    void insertOrderLineBlitz(db_compress::AttrVector &orderline, int32_t stop_idx);

    OrderLine *findOrderLine(int32_t w_id, int32_t d_id, int32_t o_id,
                             int32_t number);

    db_compress::AttrVector *
    findOrderLineBlitz(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number, int32_t stop_idx);

    // Creates a new order in the database. Returns a pointer to the database's
    // tuple.
    NewOrder *insertNewOrder(int32_t w_id, int32_t d_id, int32_t o_id);

    NewOrder *findNewOrder(int32_t w_id, int32_t d_id, int32_t o_id);

    const std::vector<const History *> &history() const { return history_; }

    // Stores order in the database. Returns a pointer to the database's tuple.
    History *insertHistory(const History &history);

    int64_t TableSize(const std::string &name);

    void OrderLineToBlitz(OrderLineBlitz &table, int64_t num_warehouses);

    void StockToBlitz(StockBlitz &table, int64_t num_warehouses);

    void CustomerToBlitz(CustomerBlitz &table, int64_t num_warehouses);

    void MountCompressor(db_compress::RelationCompressor &compressor,
                         db_compress::RelationDecompressor &decompressor,
                         int64_t num_warehouses, const std::string &table_name);

    void OrderlineToCSV(int64_t num_warehouses);

    void StockToCSV(int64_t num_warehouses);

    void CustomerToCSV(int64_t num_warehouses);

    void HistoryToCSV(int64_t num_warehouses);

    int64_t TreeSize() const {
        return stock_blitz_.TreeSize() + customers_blitz_.TreeSize() + orders_.TreeSize() +
               orderlines_blitz_.TreeSize() + warehouses_.TreeSize() + districts_.TreeSize();
    }

    static const int KEYS_PER_INTERNAL = 8;
    static const int KEYS_PER_LEAF = 8;

    TPCCStat stat_;

private:
    static const int STOCK_LEVEL_ORDERS = 20;

    // Loads each item from the items table. Returns true if they are all found.
    bool findAndValidateItems(const std::vector<NewOrderItem> &items,
                              std::vector<Item *> *item_tuples);

    // Implements order status transaction after the customer tuple has been
    // located.
    void internalOrderStatus(Customer *customer, OrderStatusOutput *output);

    void internalOrderStatusBlitz(db_compress::AttrVector *customer, OrderStatusOutput *output);

    // Implements payment transaction after the customer tuple has been located.
    void internalPaymentRemote(int32_t warehouse_id, int32_t district_id,
                               Customer *c, float h_amount, PaymentOutput *output,
                               TPCCUndo **undo);

    void internalPaymentRemoteBlitz(int32_t warehouse_id, int32_t district_id,
                                    db_compress::AttrVector *c, float h_amount,
                                    PaymentOutput *output,
                                    TPCCUndo **undo);

    // Erases order from the database. NOTE: This is only for undoing
    // transactions.
    void eraseOrder(const Order *order);

    // Erases order_line from the database. NOTE: This is only for undoing
    // transactions.
    void eraseOrderLine(const OrderLine *order_line);

    // Erases new_order from the database. NOTE: This is only for undoing
    // transactions.
    void eraseNewOrder(const NewOrder *new_order);

    // Erases history from the database. NOTE: This is only for undoing
    // transactions.
    void eraseHistory(const History *history);

    // Allocates an undo buffer if needed, storing the pointer in *undo.
    void allocateUndo(TPCCUndo **undo) {
        if (undo != NULL && *undo == NULL) {
            *undo = new TPCCUndo();
        }
    }

    // TODO: Use a data structure that supports deletes, appends, and sparse
    // ranges. Using a vector instead of a BPlusTree reduced the new order run
    // time by 3.65us. This was an improvement of 12%. It also saved 4141kB of
    // RSS.
    std::vector<Item> items_;

    BPlusTree<int32_t, Warehouse *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> warehouses_;
    BPlusTree<int32_t, Stock *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> stock_;
    BPlusTree<int32_t, Tuple<std::vector<uint8_t>> *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> stock_blitz_;
    BPlusTree<int32_t, District *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> districts_;
    BPlusTree<int32_t, Customer *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> customers_;
    BPlusTree<int32_t, Tuple<std::vector<uint8_t>> *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> customers_blitz_;
    typedef std::set<Customer *, CustomerByNameOrdering> CustomerByNameSet;
    CustomerByNameSet customers_by_name_;
    BPlusTree<int32_t, Order *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> orders_;
    // TODO: Tune the size of this tree for the bigger keys?
    BPlusTree<int64_t, Order *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> orders_by_customer_;
    BPlusTree<int64_t, OrderLine *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> orderlines_;
    BPlusTree<int64_t, Tuple<std::vector<uint8_t>> *, KEYS_PER_INTERNAL, KEYS_PER_LEAF> orderlines_blitz_;
    // TODO: Implement btree lower_bound?
    typedef std::map<int64_t, NewOrder *> NewOrderMap;
    NewOrderMap neworders_;
    std::vector<const History *> history_;

    db_compress::RelationCompressor *orderline_compressor_;
    db_compress::RelationDecompressor *orderline_decompressor_;
    db_compress::AttrVector ol_buffer_;
    db_compress::RelationCompressor *stock_compressor_;
    db_compress::RelationDecompressor *stock_decompressor_;
    db_compress::AttrVector stock_buffer_;
    db_compress::RelationCompressor *customer_compressor_;
    db_compress::RelationDecompressor *customer_decompressor_;
    db_compress::AttrVector customer_buffer_;

    // disk storage
    uint32_t num_mem_stock = 0;
    uint32_t num_disk_stock = 0;

    uint32_t num_mem_orderline = 0;
    uint32_t num_disk_orderline = 0;

    uint32_t num_mem_customer = 0;
    uint32_t num_disk_customer = 0;
    Tuple<std::vector<uint8_t>> disk_tuple_buf_;

    // disk storage
    std::string kStockFileName, kCustomerFileName, kOrderlineFileName, kHistoryFileName;
};

#endif
