#include "tpcctables.h"

#include <algorithm>
#include <cstdio>
#include <limits>
#include <vector>
#include <fstream>

#include "assert.h"
#include "stlutil.h"

using std::vector;

namespace {
    int stock_fd = DirectIOFile(Stock::TABLE_NAME);
    int orderline_fd = DirectIOFile(OrderLine::TABLE_NAME);
    int customer_fd = DirectIOFile(Customer::TABLE_NAME);
}

bool CustomerByNameOrdering::operator()(const Customer *a, const Customer *b) const {
    if (a->c_w_id < b->c_w_id) return true;
    if (a->c_w_id > b->c_w_id) return false;
    assert(a->c_w_id == b->c_w_id);

    if (a->c_d_id < b->c_d_id) return true;
    if (a->c_d_id > b->c_d_id) return false;
    assert(a->c_d_id == b->c_d_id);

    int diff = strcmp(a->c_last, b->c_last);
    if (diff < 0) return true;
    if (diff > 0) return false;
    assert(diff == 0);

    // Finally delegate to c_first
    return strcmp(a->c_first, b->c_first) < 0;
}

bool
CustomerByNameOrdering::operator()(const Tuple<Customer> *ta, const Tuple<Customer> *tb) const {
    if (ta->in_memory_ && tb->in_memory_) return (*this)(&ta->data_, &tb->data_);
    else if (ta->in_memory_) {
        Customer b;
        DiskTupleRead(customer_fd, &b, tb->id_pos_);
        return (*this)(&ta->data_, &b);
    } else if (tb->in_memory_) {
        Customer a;
        DiskTupleRead(customer_fd, &a, ta->id_pos_);
        return (*this)(&a, &tb->data_);
    } else {
        Customer a, b;
        DiskTupleRead(customer_fd, &a, ta->id_pos_);
        DiskTupleRead(customer_fd, &b, tb->id_pos_);
        return (*this)(&a, &b);
    }
}

template<typename KeyType, typename ValueType>
static void deleteBTreeValues(
        BPlusTree<KeyType, ValueType *, TPCCTables::KEYS_PER_INTERNAL, TPCCTables::KEYS_PER_LEAF> *btree) {
    KeyType key = std::numeric_limits<KeyType>::max();
    ValueType *value = NULL;
    while (btree->findLastLessThan(key, &value, &key)) {
        assert(value != NULL);
        delete value;
    }
}

template<typename KeyType, typename ValueType>
static int64_t
BTreeSize(
        BPlusTree<KeyType, ValueType *, TPCCTables::KEYS_PER_INTERNAL, TPCCTables::KEYS_PER_LEAF> *btree) {
    int64_t ret = 0;
    KeyType key = std::numeric_limits<KeyType>::max();
    ValueType *value = NULL;
    while (btree->findLastLessThan(key, &value, &key)) {
        assert(value != NULL);
        ret += value->size();
    }
    return ret;
}

TPCCTables::~TPCCTables() {
    // Clean up the b-trees with this gross hack
    deleteBTreeValues(&warehouses_);
    deleteBTreeValues(&stock_);
    deleteBTreeValues(&districts_);
    deleteBTreeValues(&orders_);
    deleteBTreeValues(&orderlines_);

    STLDeleteValues(&neworders_);
    STLDeleteElements(&customers_by_name_);
    STLDeleteElements(&history_);

    DeleteDiskData();

    std::cout << "# of customer: " << num_mem_customer << " - " << num_disk_customer << std::endl;
    std::cout << "# of stock: " << num_mem_stock << " - " << num_disk_stock << std::endl;
    std::cout << "# of orderline: " << num_mem_orderline << " - " << num_disk_orderline
              << std::endl;

    uint32_t raman_dict_size = 0;
    if (forest_customer_) raman_dict_size += forest_customer_->Size();
    if (forest_order_) raman_dict_size += forest_order_->Size();
    if (forest_orderline_) raman_dict_size += forest_orderline_->Size();
    if (forest_stock_) raman_dict_size += forest_stock_->Size();
    std::cout << "Raman Dict (Base) Size: " << raman_dict_size << std::endl;
}

int32_t TPCCTables::stockLevel(int32_t warehouse_id, int32_t district_id, int32_t threshold) {
    /* EXEC SQL SELECT d_next_o_id INTO :o_id FROM district
        WHERE d_w_id=:w_id AND d_id=:d_id; */
    //~ printf("stock level %d %d %d\n", warehouse_id, district_id, threshold);
    District *d = findDistrict(warehouse_id, district_id);
    int32_t o_id = d->d_next_o_id;

    /* EXEC SQL SELECT COUNT(DISTINCT (s_i_id)) INTO :stock_count FROM order_line, stock
        WHERE ol_w_id=:w_id AND ol_d_id=:d_id AND ol_o_id<:o_id AND ol_o_id>=:o_id-20
            AND s_w_id=:w_id AND s_i_id=ol_i_id AND s_quantity < :threshold;*/


    // retrieve up to 300 tuples from order line, using ( [o_id-20, o_id), d_id, w_id, [1, 15])
    //   and for each retrieved tuple, read the corresponding stock tuple using (ol_i_id, w_id)
    // NOTE: This is a cheat because it hard codes the maximum number of orders.
    // We really should use the ordered b-tree index to find (0, o_id-20, d_id, w_id) then iterate
    // until the end. This will also do less work (wasted finds). Since this is only 4%, it probably
    // doesn't matter much

    // TODO: Test the performance more carefully. I tried: std::set, std::hash_set, std::vector
    // with linear search, and std::vector with binary search using std::lower_bound. The best
    // seemed to be to simply save all the s_i_ids, then sort and eliminate duplicates at the end.
    std::vector<int32_t> s_i_ids;
    // Average size is more like ~30.
    s_i_ids.reserve(300);

    // Iterate over [o_id-20, o_id)
    for (int order_id = o_id - STOCK_LEVEL_ORDERS; order_id < o_id; ++order_id) {
        // HACK: We shouldn't rely on MAX_OL_CNT. See comment above.
        for (int line_number = 1; line_number <= Order::MAX_OL_CNT; ++line_number) {
            OrderLine *line = findOrderLine(warehouse_id, district_id, order_id, line_number,
                                            false);
            if (line == NULL) {
                // We can break since we have reached the end of the lines for this order.
                // TODO: A btree iterate in (w_id, d_id, o_id) order would be a clean way to do this
#ifndef NDEBUG
                for (int test_line_number = line_number + 1;
                     line_number < Order::MAX_OL_CNT; ++line_number) {
                    assert(findOrderLine(warehouse_id, district_id, order_id, test_line_number,
                                         false) == NULL);
                }
#endif
                break;
            }

            // Check if s_quantity < threshold
            Stock *stock = findStock(warehouse_id, line->ol_i_id, true);
            if (stock->s_quantity < threshold) {
                s_i_ids.push_back(line->ol_i_id);
            }
        }
    }

    // Filter out duplicate s_i_id: multiple order lines can have the same item
    std::sort(s_i_ids.begin(), s_i_ids.end());
    int num_distinct = 0;
    int32_t last = -1;  // NOTE: This relies on -1 being an invalid s_i_id
    for (size_t i = 0; i < s_i_ids.size(); ++i) {
        if (s_i_ids[i] != last) {
            last = s_i_ids[i];
            num_distinct += 1;
        }
    }

    return num_distinct;
}

void TPCCTables::orderStatus(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                             OrderStatusOutput *output) {
    //~ printf("order status %d %d %d\n", warehouse_id, district_id, customer_id);
    internalOrderStatus(findCustomer(warehouse_id, district_id, customer_id, true), output);
}

void TPCCTables::orderStatus(int32_t warehouse_id, int32_t district_id, const char *c_last,
                             OrderStatusOutput *output) {
    //~ printf("order status %d %d %s\n", warehouse_id, district_id, c_last);
    Customer *customer = findCustomerByName(warehouse_id, district_id, c_last);
    internalOrderStatus(customer, output);
}

void TPCCTables::internalOrderStatus(Customer *customer, OrderStatusOutput *output) {
    output->c_id = customer->c_id;
    // retrieve from customer: balance, first, middle, last
    output->c_balance = customer->c_balance;
    strcpy(output->c_first, customer->c_first);
    strcpy(output->c_middle, customer->c_middle);
    strcpy(output->c_last, customer->c_last);

    // Find the row in the order table with largest o_id
    Order *order = findLastOrderByCustomer(customer->c_w_id, customer->c_d_id, customer->c_id);
    output->o_id = order->o_id;
    output->o_carrier_id = order->o_carrier_id;
    strcpy(output->o_entry_d, order->o_entry_d);

    output->lines.resize(order->o_ol_cnt);
    for (int32_t line_number = 1; line_number <= order->o_ol_cnt; ++line_number) {
        OrderLine *line = findOrderLine(customer->c_w_id, customer->c_d_id, order->o_id,
                                        line_number, false);
        output->lines[line_number - 1].ol_i_id = line->ol_i_id;
        output->lines[line_number - 1].ol_supply_w_id = line->ol_supply_w_id;
        output->lines[line_number - 1].ol_quantity = line->ol_quantity;
        output->lines[line_number - 1].ol_amount = line->ol_amount;
        strcpy(output->lines[line_number - 1].ol_delivery_d, line->ol_delivery_d);
    }
#ifndef NDEBUG
    // Verify that none of the other OrderLines exist.
    for (int32_t line_number = order->o_ol_cnt + 1;
         line_number <= Order::MAX_OL_CNT; ++line_number) {
        assert(findOrderLine(customer->c_w_id, customer->c_d_id, order->o_id, line_number, false) ==
               NULL);
    }
#endif
}

bool TPCCTables::newOrder(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                          const std::vector<NewOrderItem> &items, const char *now,
                          NewOrderOutput *output,
                          TPCCUndo **undo) {
    // perform the home part
    bool result = newOrderHome(warehouse_id, district_id, customer_id, items, now, output, undo);
    if (!result) {
        return false;
    }

    // Process all remote warehouses
    WarehouseSet warehouses = newOrderRemoteWarehouses(warehouse_id, items);
    for (WarehouseSet::const_iterator i = warehouses.begin(); i != warehouses.end(); ++i) {
        vector<int32_t> quantities;
        result = newOrderRemote(warehouse_id, *i, items, &quantities, undo);
        assert(result);
        newOrderCombine(quantities, output);
    }

    return true;
}

bool TPCCTables::newOrderHome(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                              const vector<NewOrderItem> &items, const char *now,
                              NewOrderOutput *output, TPCCUndo **undo) {
    //~ printf("new order %d %d %d %d %s\n", warehouse_id, district_id, customer_id, items.size(), now);
    // 2.4.3.4. requires that we display c_last, c_credit, and o_id for rolled back transactions:
    // read those values first
    District *d = findDistrict(warehouse_id, district_id);
    output->d_tax = d->d_tax;
    output->o_id = d->d_next_o_id;
    assert(findOrder(warehouse_id, district_id, output->o_id, true) == NULL);

    Customer *c = findCustomer(warehouse_id, district_id, customer_id, true);
    assert(sizeof(output->c_last) == sizeof(c->c_last));
    memcpy(output->c_last, c->c_last, sizeof(output->c_last));
    memcpy(output->c_credit, c->c_credit, sizeof(output->c_credit));
    output->c_discount = c->c_discount;

    // CHEAT: Validate all items to see if we will need to abort
    vector<Item *> item_tuples(items.size());
    if (!findAndValidateItems(items, &item_tuples)) {
        strcpy(output->status, NewOrderOutput::INVALID_ITEM_STATUS);
        return false;
    }

    // Check if this is an all local transaction
    // TODO: This loops through items *again* which is slightly inefficient
    bool all_local = true;
    for (int i = 0; i < items.size(); ++i) {
        if (items[i].ol_supply_w_id != warehouse_id) {
            all_local = false;
            break;
        }
    }

    // We will not abort: update the status and the database state, allocate an undo buffer
    output->status[0] = '\0';
    allocateUndo(undo);

    // Modify the order id to assign it
    if (undo != NULL) {
        (*undo)->save(d);
    }
    d->d_next_o_id += 1;

    Warehouse *w = findWarehouse(warehouse_id);
    output->w_tax = w->w_tax;

    Order order;
    order.o_w_id = warehouse_id;
    order.o_d_id = district_id;
    order.o_id = output->o_id;
    order.o_c_id = customer_id;
    order.o_carrier_id = Order::NULL_CARRIER_ID;
    order.o_ol_cnt = static_cast<int32_t>(items.size());
    order.o_all_local = all_local ? 1 : 0;
    strcpy(order.o_entry_d, now);
    assert(strlen(order.o_entry_d) == DATETIME_SIZE);
    Order *o = insertOrder(order, true, true);
    NewOrder *no = insertNewOrder(warehouse_id, district_id, output->o_id);
    if (undo != NULL) {
        (*undo)->inserted(o);
        (*undo)->inserted(no);
    }

    OrderLine line;
    line.ol_o_id = output->o_id;
    line.ol_d_id = district_id;
    line.ol_w_id = warehouse_id;
    memset(line.ol_delivery_d, 0, DATETIME_SIZE + 1);

    output->items.resize(items.size());
    output->total = 0;
    for (int i = 0; i < items.size(); ++i) {
        line.ol_number = i + 1;
        line.ol_i_id = items[i].i_id;
        line.ol_supply_w_id = items[i].ol_supply_w_id;
        line.ol_quantity = items[i].ol_quantity;

        // Vertical Partitioning HACK: We read s_dist_xx from our local replica, assuming that
        // these columns are replicated everywhere.
        // TODO: I think this is unrealistic, since it will occupy ~23 MB per warehouse on all
        // replicas. Try the "two round" version in the future.
        Stock *stock = findStock(items[i].ol_supply_w_id, items[i].i_id, true);
        assert(sizeof(line.ol_dist_info) == sizeof(stock->s_dist[district_id]));
        memcpy(line.ol_dist_info, stock->s_dist[district_id], sizeof(line.ol_dist_info));
        // Since we *need* to replicate s_dist_xx columns, might as well replicate s_data
        // Makes it 290 bytes per tuple, or ~28 MB per warehouse.
        bool stock_is_original = (strstr(stock->s_data, "ORIGINAL") != NULL);
        if (stock_is_original && strstr(item_tuples[i]->i_data, "ORIGINAL") != NULL) {
            output->items[i].brand_generic = NewOrderOutput::ItemInfo::BRAND;
        } else {
            output->items[i].brand_generic = NewOrderOutput::ItemInfo::GENERIC;
        }

        assert(sizeof(output->items[i].i_name) == sizeof(item_tuples[i]->i_name));
        memcpy(output->items[i].i_name, item_tuples[i]->i_name, sizeof(output->items[i].i_name));
        output->items[i].i_price = item_tuples[i]->i_price;
        output->items[i].ol_amount =
                static_cast<float>(items[i].ol_quantity) * item_tuples[i]->i_price;
        line.ol_amount = output->items[i].ol_amount;
        output->total += output->items[i].ol_amount;
        OrderLine *ol = insertOrderLine(line, false, true);
        if (undo != NULL) {
            (*undo)->inserted(ol);
        }
    }

    // Perform the "remote" part for this warehouse
    // TODO: It might be more efficient to merge this into the loop above, but this is simpler.
    vector<int32_t> quantities;
    bool result = newOrderRemote(warehouse_id, warehouse_id, items, &quantities, undo);
    ASSERT(result);
    newOrderCombine(quantities, output);

    return true;
}

bool TPCCTables::newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
                                const vector<NewOrderItem> &items,
                                std::vector<int32_t> *out_quantities, TPCCUndo **undo) {
    // Validate all the items: needed so that we don't need to undo in order to execute this
    // TODO: item_tuples is unused. Remove?
    vector<Item *> item_tuples;
    if (!findAndValidateItems(items, &item_tuples)) {
        return false;
    }

    // We will not abort: allocate an undo buffer
    allocateUndo(undo);

    out_quantities->resize(items.size());
    for (int i = 0; i < items.size(); ++i) {
        // Skip items that don't belong to remote warehouse
        if (items[i].ol_supply_w_id != remote_warehouse) {
            (*out_quantities)[i] = INVALID_QUANTITY;
            continue;
        }

        // update stock
        Stock *stock = findStock(items[i].ol_supply_w_id, items[i].i_id, true);
        if (undo != NULL) {
            (*undo)->save(stock);
        }
        if (stock->s_quantity >= items[i].ol_quantity + 10) {
            stock->s_quantity -= items[i].ol_quantity;
        } else {
            stock->s_quantity = stock->s_quantity - items[i].ol_quantity + 91;
        }
        (*out_quantities)[i] = stock->s_quantity;
        stock->s_ytd += items[i].ol_quantity;
        stock->s_order_cnt += 1;
        // newOrderHome calls newOrderRemote, so this is needed
        if (items[i].ol_supply_w_id != home_warehouse) {
            // remote order
            stock->s_remote_cnt += 1;
        }
    }

    return true;
}

bool TPCCTables::findAndValidateItems(const vector<NewOrderItem> &items,
                                      vector<Item *> *item_tuples) {
    // CHEAT: Validate all items to see if we will need to abort
    item_tuples->resize(items.size());
    for (int i = 0; i < items.size(); ++i) {
        (*item_tuples)[i] = findItem(items[i].i_id);
        if ((*item_tuples)[i] == NULL) {
            return false;
        }
    }
    return true;
}


void TPCCTables::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                         int32_t c_district_id, int32_t customer_id, float h_amount,
                         const char *now,
                         PaymentOutput *output, TPCCUndo **undo) {
    //~ printf("payment %d %d %d %d %d %f %s\n", warehouse_id, district_id, c_warehouse_id, c_district_id, customer_id, h_amount, now);
    Customer *customer = findCustomer(c_warehouse_id, c_district_id, customer_id, true);
    paymentHome(warehouse_id, district_id, c_warehouse_id, c_district_id, customer_id, h_amount,
                now, output, undo);
    internalPaymentRemote(warehouse_id, district_id, customer, h_amount, output, undo);
}

void TPCCTables::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                         int32_t c_district_id, const char *c_last, float h_amount, const char *now,
                         PaymentOutput *output, TPCCUndo **undo) {
    //~ printf("payment %d %d %d %d %s %f %s\n", warehouse_id, district_id, c_warehouse_id, c_district_id, c_last, h_amount, now);
    Customer *customer = findCustomerByName(c_warehouse_id, c_district_id, c_last);
    paymentHome(warehouse_id, district_id, c_warehouse_id, c_district_id, customer->c_id, h_amount,
                now, output, undo);
    internalPaymentRemote(warehouse_id, district_id, customer, h_amount, output, undo);
}

#define COPY_ADDRESS(src, dest, prefix) \
    Address::copy( \
            dest->prefix ## street_1, dest->prefix ## street_2, dest->prefix ## city, \
            dest->prefix ## state, dest->prefix ## zip,\
            src->prefix ## street_1, src->prefix ## street_2, src->prefix ## city, \
            src->prefix ## state, src->prefix ## zip)

#define ZERO_ADDRESS(output, prefix) \
    output->prefix ## street_1[0] = '\0'; \
    output->prefix ## street_2[0] = '\0'; \
    output->prefix ## city[0] = '\0'; \
    output->prefix ## state[0] = '\0'; \
    output->prefix ## zip[0] = '\0'

static void zeroWarehouseDistrict(PaymentOutput *output) {
    // Zero the warehouse and district data
    // TODO: I should split this structure, but I'm lazy
    ZERO_ADDRESS(output, w_);
    ZERO_ADDRESS(output, d_);
}

void TPCCTables::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                               int32_t c_district_id, int32_t c_id, float h_amount,
                               PaymentOutput *output,
                               TPCCUndo **undo) {
    Customer *customer = findCustomer(c_warehouse_id, c_district_id, c_id, true);
    internalPaymentRemote(warehouse_id, district_id, customer, h_amount, output, undo);
    zeroWarehouseDistrict(output);
}

void TPCCTables::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                               int32_t c_district_id, const char *c_last, float h_amount,
                               PaymentOutput *output,
                               TPCCUndo **undo) {
    Customer *customer = findCustomerByName(c_warehouse_id, c_district_id, c_last);
    internalPaymentRemote(warehouse_id, district_id, customer, h_amount, output, undo);
    zeroWarehouseDistrict(output);
}

void TPCCTables::paymentHome(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                             int32_t c_district_id, int32_t customer_id, float h_amount,
                             const char *now,
                             PaymentOutput *output, TPCCUndo **undo) {
    Warehouse *w = findWarehouse(warehouse_id);
    if (undo != NULL) {
        allocateUndo(undo);
        (*undo)->save(w);
    }
    w->w_ytd += h_amount;
    COPY_ADDRESS(w, output, w_);

    District *d = findDistrict(warehouse_id, district_id);
    if (undo != NULL) {
        (*undo)->save(d);
    }
    d->d_ytd += h_amount;
    COPY_ADDRESS(d, output, d_);

    // Insert the line into the history table
    History h;
    h.h_w_id = warehouse_id;
    h.h_d_id = district_id;
    h.h_c_w_id = c_warehouse_id;
    h.h_c_d_id = c_district_id;
    h.h_c_id = customer_id;
    h.h_amount = h_amount;
    strcpy(h.h_date, now);
    strcpy(h.h_data, w->w_name);
    strcat(h.h_data, "    ");
    strcat(h.h_data, d->d_name);
    History *history = insertHistory(h, true, true);
    if (undo != NULL) {
        (*undo)->inserted(history);
    }

    // Zero all the customer fields: avoid uninitialized data for serialization
    output->c_credit_lim = 0;
    output->c_discount = 0;
    output->c_balance = 0;
    output->c_first[0] = '\0';
    output->c_middle[0] = '\0';
    output->c_last[0] = '\0';
    ZERO_ADDRESS(output, c_);
    output->c_phone[0] = '\0';
    output->c_since[0] = '\0';
    output->c_credit[0] = '\0';
    output->c_data[0] = '\0';
}

void TPCCTables::internalPaymentRemote(int32_t warehouse_id, int32_t district_id, Customer *c,
                                       float h_amount, PaymentOutput *output, TPCCUndo **undo) {
    if (undo != NULL) {
        allocateUndo(undo);
        (*undo)->save(c);
    }
    c->c_balance -= h_amount;
    c->c_ytd_payment += h_amount;
    c->c_payment_cnt += 1;
    if (strcmp(c->c_credit, Customer::BAD_CREDIT) == 0) {
        // Bad credit: insert history into c_data
        static const int HISTORY_SIZE = Customer::MAX_DATA + 1;
        char history[HISTORY_SIZE];
        int characters = snprintf(history, HISTORY_SIZE, " %d-%d-%d-%d-%d-%.0f",
                                  c->c_id, c->c_d_id, c->c_w_id, district_id, warehouse_id,
                                  h_amount);
        assert(characters < HISTORY_SIZE);

        // Perform the insert with a move and copy
        int current_keep = static_cast<int>(strlen(c->c_data));
        if (current_keep + characters > Customer::MAX_DATA) {
            current_keep = Customer::MAX_DATA - characters;
        }
        assert(current_keep + characters <= Customer::MAX_DATA);
        memmove(c->c_data + characters, c->c_data, current_keep);
        memcpy(c->c_data, history, characters);
        c->c_data[characters + current_keep] = '\0';
        assert(strlen(c->c_data) == characters + current_keep);
    }

    output->c_credit_lim = c->c_credit_lim;
    output->c_discount = c->c_discount;
    output->c_balance = c->c_balance;
#define COPY_STRING(dest, src, field) memcpy(dest->field, src->field, sizeof(src->field))
    COPY_STRING(output, c, c_first);
    COPY_STRING(output, c, c_middle);
    COPY_STRING(output, c, c_last);
    COPY_ADDRESS(c, output, c_);
    COPY_STRING(output, c, c_phone);
    COPY_STRING(output, c, c_since);
    COPY_STRING(output, c, c_credit);
    COPY_STRING(output, c, c_data);
#undef COPY_STRING
}

#undef ZERO_ADDRESS
#undef COPY_ADDRESS

// forward declaration for delivery
static int64_t makeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id);

void TPCCTables::delivery(int32_t warehouse_id, int32_t carrier_id, const char *now,
                          std::vector<DeliveryOrderInfo> *orders, TPCCUndo **undo) {
    //~ printf("delivery %d %d %s\n", warehouse_id, carrier_id, now);
    allocateUndo(undo);
    orders->clear();
    for (int32_t d_id = 1; d_id <= District::NUM_PER_WAREHOUSE; ++d_id) {
        // Find and remove the lowest numbered order for the district
        int64_t key = makeNewOrderKey(warehouse_id, d_id, 1);
        NewOrderMap::iterator iterator = neworders_.lower_bound(key);
        NewOrder *neworder = NULL;
        if (iterator != neworders_.end()) {
            neworder = iterator->second;
            assert(neworder != NULL);
        }
        if (neworder == NULL || neworder->no_d_id != d_id || neworder->no_w_id != warehouse_id) {
            // No orders for this district
            // TODO: 2.7.4.2: If this occurs in max(1%, 1) of transactions, report it (???)
            continue;
        }
        assert(neworder->no_d_id == d_id && neworder->no_w_id == warehouse_id);
        int32_t o_id = neworder->no_o_id;
        neworders_.erase(iterator);
        if (undo != NULL) {
            (*undo)->deleted(neworder);
        } else {
            delete neworder;
        }

        DeliveryOrderInfo order;
        order.d_id = d_id;
        order.o_id = o_id;
        orders->push_back(order);

        Order *o = findOrder(warehouse_id, d_id, o_id, true);
        assert(o->o_carrier_id == Order::NULL_CARRIER_ID);
        if (undo != NULL) {
            (*undo)->save(o);
        }
        o->o_carrier_id = carrier_id;

        float total = 0;
        // TODO: Select based on (w_id, d_id, o_id) rather than using ol_number?
        for (int32_t i = 1; i <= o->o_ol_cnt; ++i) {
            OrderLine *line = findOrderLine(warehouse_id, d_id, o_id, i, false);
            if (undo != NULL) {
                (*undo)->save(line);
            }
            assert(0 == strlen(line->ol_delivery_d));
            strcpy(line->ol_delivery_d, now);
            assert(strlen(line->ol_delivery_d) == DATETIME_SIZE);
            total += line->ol_amount;
        }

        Customer *c = findCustomer(warehouse_id, d_id, o->o_c_id, true);
        if (undo != NULL) {
            (*undo)->save(c);
        }
        c->c_balance += total;
        c->c_delivery_cnt += 1;
    }
}

template<typename T>
static void restoreFromMap(const T &map) {
    for (typename T::const_iterator i = map.begin(); i != map.end(); ++i) {
        // Copy the original data back
        *(i->first) = *(i->second);
    }
}

template<typename T>
static void eraseTuple(const T &set, TPCCTables *tables,
                       void (TPCCTables::*eraseFPtr)(typename T::value_type)) {
    for (typename T::const_iterator i = set.begin(); i != set.end(); ++i) {
        // Invoke eraseFPtr on each value
        (tables->*eraseFPtr)(*i);
    }
}

// Used by both applyUndo and insertNewOrder to put allocated NewOrder tuples in the map.
static NewOrder *insertNewOrderObject(std::map<int64_t, NewOrder *> *map, NewOrder *neworder) {
    int64_t key = makeNewOrderKey(neworder->no_w_id, neworder->no_d_id, neworder->no_o_id);
    assert(map->find(key) == map->end());
    map->insert(std::make_pair(key, neworder));
    return neworder;
}

void TPCCTables::applyUndo(TPCCUndo *undo) {
    restoreFromMap(undo->modified_warehouses());
    restoreFromMap(undo->modified_districts());
    restoreFromMap(undo->modified_customers());
    restoreFromMap(undo->modified_stock());
    restoreFromMap(undo->modified_orders());
    restoreFromMap(undo->modified_order_lines());

//    eraseTuple(undo->inserted_orders(), this, &TPCCTables::eraseOrder);
//    eraseTuple(undo->inserted_order_lines(), this, &TPCCTables::eraseOrderLine);
//    eraseTuple(undo->inserted_new_orders(), this, &TPCCTables::eraseNewOrder);
//    eraseTuple(undo->inserted_history(), this, &TPCCTables::eraseHistory);

    // Transfer deleted new orders back to the database
    for (TPCCUndo::NewOrderDeletedSet::const_iterator i = undo->deleted_new_orders().begin();
         i != undo->deleted_new_orders().end();
         ++i) {
        NewOrder *neworder = *i;
        insertNewOrderObject(&neworders_, neworder);
    }

    undo->applied();
    delete undo;
}

template<typename T>
static T *
insert(BPlusTree<int32_t, T *, TPCCTables::KEYS_PER_INTERNAL, TPCCTables::KEYS_PER_LEAF> *tree,
       int32_t key, const T &item) {
    assert(!tree->find(key));
    T *copy = new T(item);
    tree->insert(key, copy);
    return copy;
}

template<typename T>
static T *
insert(BPlusTree<int64_t, T *, TPCCTables::KEYS_PER_INTERNAL, TPCCTables::KEYS_PER_LEAF> *tree,
       int64_t key, const T &item) {
    assert(!tree->find(key));
    T *copy = new T(item);
    tree->insert(key, copy);
    return copy;
}

template<typename T>
static T *
find(const BPlusTree<int32_t, T *, TPCCTables::KEYS_PER_INTERNAL, TPCCTables::KEYS_PER_LEAF> &tree,
     int32_t key) {
    T *output = NULL;
    if (tree.find(key, &output)) {
        return output;
    }
    return NULL;
}

template<typename T>
static T *
find(const BPlusTree<int64_t, T *, TPCCTables::KEYS_PER_INTERNAL, TPCCTables::KEYS_PER_LEAF> &tree,
     int64_t key) {
    T *output = NULL;
    if (tree.find(key, &output)) {
        return output;
    }
    return NULL;
}

template<typename T, typename KeyType>
static void
erase(BPlusTree<KeyType, T *, TPCCTables::KEYS_PER_INTERNAL, TPCCTables::KEYS_PER_LEAF> *tree,
      KeyType key, const T *value) {
    T *out = NULL;
    ASSERT(tree->find(key, &out));
    ASSERT(out == value);
    bool result = tree->del(key);
    ASSERT(result);
    ASSERT(!tree->find(key));
}

void TPCCTables::insertItem(const Item &item) {
    assert(item.i_id == items_.size() + 1);
    items_.push_back(item);
}

Item *TPCCTables::findItem(int32_t id) {
    assert(1 <= id);
    id -= 1;
    if (id >= items_.size()) return NULL;
    return &items_[id];
}

void TPCCTables::insertWarehouse(const Warehouse &w) {
    insert(&warehouses_, w.w_id, w);
}

Warehouse *TPCCTables::findWarehouse(int32_t id) {
    return find(warehouses_, id);
}

static int32_t makeStockKey(int32_t w_id, int32_t s_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= s_id && s_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
    int32_t id = s_id + (w_id * Stock::NUM_STOCK_PER_WAREHOUSE);
    assert(id >= 0);
    return id;
}

void TPCCTables::insertStock(const Stock &stock, bool is_orig, bool relearn) {
    int64_t key = makeStockKey(stock.s_w_id, stock.s_i_id);
    if (!is_orig) {
        stock_tuple_disk_.in_memory_ = num_mem_stock < Stock::MEMORY_THRESHOLD;
        if (stock_tuple_disk_.in_memory_) {
            num_mem_stock++;

            // in memory
            if (!relearn) {
                stock_tuple_disk_.dict_id_ = -1;
                stock_tuple_disk_.data_ = RamanCompress(forest_stock_, &stock);
                insert(&stock_raman, (int32_t) key, stock_tuple_disk_);
                return;
            }

            // in buffer
            block_stock_.Append(stock, key);
            if (block_stock_.IsFull()) {
                int32_t dict_id;
                std::vector<int64_t> keys;
                std::vector<BitStream> compressed;

                block_stock_.BlockCompress(compressed, &keys, &dict_id);

                stock_tuple_disk_.dict_id_ = dict_id;
                for (int i = 0; i < compressed.size(); i++) {
                    stock_tuple_disk_.data_ = compressed[i];
                    insert(&stock_raman, keys[i], stock_tuple_disk_);
                }
            }
        } else {
            // on disk
            stock_tuple_disk_.id_pos_ = num_disk_stock;
            num_disk_stock++;
            SeqDiskTupleWrite(stock_fd, &stock);
            insert(&stock_raman, (int32_t) key, stock_tuple_disk_);
        }
    } else
        insert(&stock_, (int32_t) key, stock);
}

Stock *TPCCTables::findStock(int32_t w_id, int32_t s_id, bool is_orig) {
    int64_t key = makeStockKey(w_id, s_id);
    if (!is_orig) {
        // find it in buffer
        Stock *stock = block_stock_.find(key);
        if (stock != nullptr) return stock;

        Tuple<BitStream> *tuple = find(stock_raman, (int32_t) key);
        if (tuple == nullptr) return nullptr;

        // find it on disk
        if (!tuple->in_memory_) {
            DiskTupleRead(stock_fd, &stock_raman_buf_, tuple->id_pos_);
            return &stock_raman_buf_;
        }

        // find it in memory, decompress it with suitable dict.
        if (tuple->dict_id_ < 0)
            RamanDecompress(forest_stock_, &stock_raman_buf_, tuple->data_);
        else {
            RamanCompressor *compressor = block_stock_.GetCompressor(tuple->dict_id_);
            RamanDecompress(compressor, &stock_raman_buf_, tuple->data_);
        }
        return &stock_raman_buf_;
    } else {
        if (key < 0) return nullptr;
        return find(stock_, key);
    }
}

static int32_t makeDistrictKey(int32_t w_id, int32_t d_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    int32_t id = d_id + (w_id * District::NUM_PER_WAREHOUSE);
    assert(id >= 0);
    return id;
}

void TPCCTables::insertDistrict(const District &district) {
    insert(&districts_, makeDistrictKey(district.d_w_id, district.d_id), district);
}

District *TPCCTables::findDistrict(int32_t w_id, int32_t d_id) {
    return find(districts_, makeDistrictKey(w_id, d_id));
}

static int32_t makeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= c_id && c_id <= Customer::NUM_PER_DISTRICT);
    int32_t id = (w_id * District::NUM_PER_WAREHOUSE + d_id)
                 * Customer::NUM_PER_DISTRICT + c_id;
    assert(id >= 0);
    return id;
}

void TPCCTables::insertCustomer(const Customer &customer, bool is_orig, bool relearn) {
    int64_t key = makeCustomerKey(customer.c_w_id, customer.c_d_id, customer.c_id);
    if (!is_orig) {
        customer_tuple_disk_.in_memory_ = num_mem_customer < Customer::MEMORY_THRESHOLD;
        if (customer_tuple_disk_.in_memory_) {
            num_mem_customer++;
            // in memory
            if (!relearn) {
                customer_tuple_disk_.dict_id_ = -1;
                customer_tuple_disk_.data_ = RamanCompress(forest_customer_, &customer);
                insert(&customer_raman, (int32_t) key, customer_tuple_disk_);
                return;
            }

            // in buffer
            block_customer_.Append(customer, key);
            if (block_customer_.IsFull()) {
                int32_t dict_id;
                std::vector<int64_t> keys;
                std::vector<BitStream> compressed;

                block_customer_.BlockCompress(compressed, &keys, &dict_id);

                customer_tuple_disk_.dict_id_ = dict_id;
                for (int i = 0; i < compressed.size(); i++) {
                    customer_tuple_disk_.data_ = compressed[i];
                    insert(&customer_raman, keys[i], customer_tuple_disk_);
                }
            }
        } else {
            // on disk
            customer_tuple_disk_.id_pos_ = num_disk_customer;
            num_disk_customer++;
            SeqDiskTupleWrite(customer_fd, &customer);
            insert(&customer_raman, (int32_t) key, customer_tuple_disk_);
        }
    } else {
        Customer *c = insert(&customers_, (int32_t) key, customer);
        customers_by_name_.insert(c);
    }
}

Customer *TPCCTables::findCustomer(int32_t w_id, int32_t d_id, int32_t c_id, bool is_orig) {
    int64_t key = makeCustomerKey(w_id, d_id, c_id);
    if (!is_orig) {
        // find it in buffer
        Customer *customer = block_customer_.find(key);
        if (customer != nullptr) return customer;

        Tuple<BitStream> *tuple = find(customer_raman, (int32_t) key);
        if (tuple == nullptr) return nullptr;

        // find it on disk
        if (!tuple->in_memory_) {
            DiskTupleRead(customer_fd, &customer_raman_buf_, tuple->id_pos_);
            return &customer_raman_buf_;
        }

        // find it in memory, decompress it with suitable dict.
        if (tuple->dict_id_ < 0)
            RamanDecompress(forest_customer_, &customer_raman_buf_, tuple->data_);
        else {
            RamanCompressor *compressor = block_customer_.GetCompressor(tuple->dict_id_);
            RamanDecompress(compressor, &customer_raman_buf_, tuple->data_);
        }
        return &customer_raman_buf_;
    } else {
        if (key < 0) return nullptr;
        return find(customers_, (int32_t) key);
    }
}

Customer *TPCCTables::findCustomerByName(int32_t w_id, int32_t d_id, const char *c_last) {
    Customer c;
    c.c_w_id = w_id;
    c.c_d_id = d_id;
    strcpy(c.c_last, c_last);
    c.c_first[0] = '\0';

    auto it = customers_by_name_.lower_bound(&c);
    assert(it != customers_by_name_.end());

    // go to the "next" c_last
    // TODO: This is a GROSS hack. Can we do better?
    int length = static_cast<int>(strlen(c_last));
    if (length == Customer::MAX_LAST) {
        c.c_last[length - 1] = static_cast<char>(c.c_last[length - 1] + 1);
    } else {
        c.c_last[length] = 'A';
        c.c_last[length + 1] = '\0';
    }
    auto stop = customers_by_name_.lower_bound(&c);

    Customer *t_customer = nullptr;
    // Choose position n/2 rounded up (1 based addressing) = floor((n-1)/2)
    if (it != stop) {
        auto middle = it;
        ++it;
        int i = 0;
        while (it != stop) {
            // Increment the middle iterator on every second iteration
            if (i % 2 == 1) {
                ++middle;
            }
            ++it;
            ++i;
        }
        // There were i+1 matching last names
        t_customer = *middle;
    }
    return t_customer;
}

static int32_t makeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    // TODO: This is bad for locality since o_id is in the most significant position. Larger keys?
    int32_t id = (o_id * District::NUM_PER_WAREHOUSE + d_id)
                 * Warehouse::MAX_WAREHOUSE_ID + w_id;
    assert(id >= 0);
    return id;
}

static int64_t makeOrderByCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= c_id && c_id <= Customer::NUM_PER_DISTRICT);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    int32_t top_id = (w_id * District::NUM_PER_WAREHOUSE + d_id) * Customer::NUM_PER_DISTRICT
                     + c_id;
    assert(top_id >= 0);
    int64_t id = (((int64_t) top_id) << 32) | o_id;
    assert(id > 0);
    return id;
}

Order *TPCCTables::insertOrder(const Order &order, bool is_orig, bool relearn) {
    int64_t key = makeOrderKey(order.o_w_id, order.o_d_id, order.o_id);
    if (!is_orig) {
        if (!relearn) {
            Tuple<BitStream> tuple;
            tuple.in_memory_ = true;
            tuple.data_ = RamanCompress(forest_order_, &order);
            tuple.dict_id_ = -1;
            insert(&order_raman, key, tuple);
            return nullptr;
        }

        block_order_.Append(order, key);
        // if block is full
        if (block_order_.IsFull()) {
            int32_t dict_id;
            std::vector<int64_t> keys;
            std::vector<BitStream> compressed;

            block_order_.BlockCompress(compressed, &keys, &dict_id);

            Tuple<BitStream> tuple;
            tuple.in_memory_ = true;
            tuple.dict_id_ = dict_id;
            for (int i = 0; i < compressed.size(); i++) {
                tuple.data_ = compressed[i];
                insert(&order_raman, (int32_t) keys[i], tuple);
            }
        }
        return nullptr;
    } else {
        Order *tuple = insert(&orders_, key, order);
        // Secondary index based on customer id
        key = makeOrderByCustomerKey(order.o_w_id, order.o_d_id, order.o_c_id, order.o_id);
        assert(!orders_by_customer_.find(key));
        orders_by_customer_.insert(key, tuple);
        return tuple;
    }
}

Order *TPCCTables::findOrder(int32_t w_id, int32_t d_id, int32_t o_id, bool is_orig) {
    int32_t key = makeOrderKey(w_id, d_id, o_id);
    if (!is_orig) {
        // in buffer
        Order *order = block_order_.find(key);
        if (order != nullptr) return order;

        Tuple<BitStream> *tuple = find(order_raman, key);
        if (tuple == nullptr) return nullptr;

        // in memory
        if (tuple->dict_id_ < 0)
            RamanDecompress(forest_order_, &order_raman_buf_, tuple->data_);
        else {
            RamanCompressor *compressor = block_order_.GetCompressor(tuple->dict_id_);
            RamanDecompress(compressor, &order_raman_buf_, tuple->data_);
        }
        return &order_raman_buf_;
    } else {
        if (key < 0) return nullptr;
        return find(orders_, key);
    }
}

Order *
TPCCTables::findLastOrderByCustomer(const int32_t w_id, const int32_t d_id, const int32_t c_id) {
    Order *order = NULL;

    // Increment the (w_id, d_id, c_id) tuple
    int64_t key = makeOrderByCustomerKey(w_id, d_id, c_id, 1);
    key += ((int64_t) 1) << 32;
    ASSERT(key > 0);

    bool found = orders_by_customer_.findLastLessThan(key, &order);
    ASSERT(!found || (order->o_w_id == w_id && order->o_d_id == d_id && order->o_c_id == c_id));
    return order;
}

static int64_t makeOrderLineKey(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    assert(1 <= number && number <= Order::MAX_OL_CNT);
    // TODO: This may be bad for locality since o_id is in the most significant position. However,
    // Order status fetches all rows for one (w_id, d_id, o_id) tuple, so it may be fine,
    // but stock level fetches order lines for a range of (w_id, d_id, o_id) values
    int64_t id = ((o_id * District::NUM_PER_WAREHOUSE + d_id)
                  * Warehouse::MAX_WAREHOUSE_ID + w_id) * Order::MAX_OL_CNT + number;
    // if (id < 0) throw std::runtime_error("id < 0 in makeOrderLineKey");
    return id;
}

OrderLine *TPCCTables::insertOrderLine(const OrderLine &orderline, bool is_orig, bool relearn) {
    int64_t key = makeOrderLineKey(orderline.ol_w_id, orderline.ol_d_id, orderline.ol_o_id,
                                   orderline.ol_number);
    if (!is_orig) {
        ol_tuple_disk_.in_memory_ = num_mem_orderline < OrderLine::MEMORY_THRESHOLD;
        if (ol_tuple_disk_.in_memory_) {
            num_mem_orderline++;
            // in memory
            if (!relearn) {
                ol_tuple_disk_.dict_id_ = -1;
                ol_tuple_disk_.data_ = RamanCompress(forest_orderline_, &orderline);
                insert(&orderline_raman, key, ol_tuple_disk_);
                return nullptr;
            }

            // in buffer
            block_orderline_.Append(orderline, key);
            if (block_orderline_.IsFull()) {
                int32_t dict_id;
                std::vector<int64_t> keys;
                std::vector<BitStream> compressed;

                block_orderline_.BlockCompress(compressed, &keys, &dict_id);

                ol_tuple_disk_.dict_id_ = dict_id;
                for (int i = 0; i < compressed.size(); i++) {
                    ol_tuple_disk_.data_ = compressed[i];
                    insert(&orderline_raman, keys[i], ol_tuple_disk_);
                }
                return nullptr;
            }
        } else {
            // on disk
            ol_tuple_disk_.id_pos_ = num_disk_orderline;
            SeqDiskTupleWrite(orderline_fd, &orderline);
            num_disk_orderline++;
            insert(&orderline_raman, key, ol_tuple_disk_);
            return nullptr;
        }
    } else {
        OrderLine *tuple = insert(&orderlines_, key, orderline);
        return tuple;
    }
    return nullptr;
}

OrderLine *TPCCTables::findOrderLine(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number,
                                     bool is_orig) {
    int64_t key = makeOrderLineKey(w_id, d_id, o_id, number);
    if (!is_orig) {
        // find it in buffer
        OrderLine *orderline = block_orderline_.find(key);
        if (orderline != nullptr) {
            int64_t get_key = makeOrderLineKey(orderline->ol_w_id, orderline->ol_d_id,
                                               orderline->ol_o_id,
                                               orderline->ol_number);
            if (get_key != key)
                throw std::runtime_error("findOrderLine: key mismatch");

            return orderline;
        }

        Tuple<BitStream> *tuple = find(orderline_raman, key);
        if (tuple == nullptr) return nullptr;

        // find it on disk
        if (!tuple->in_memory_) {
            DiskTupleRead(orderline_fd, &orderline_raman_buf_, tuple->id_pos_);
            return &orderline_raman_buf_;
        }

        // find it in memory, decompress it with suitable dict.
        if (tuple->dict_id_ < 0)
            RamanDecompress(forest_orderline_, &orderline_raman_buf_, tuple->data_);
        else {
            RamanCompressor *compressor = block_orderline_.GetCompressor(tuple->dict_id_);
            RamanDecompress(compressor, &orderline_raman_buf_, tuple->data_);
        }
        return &orderline_raman_buf_;
    } else {
        if (key < 0) return nullptr;
        return find(orderlines_, key);
    }
}

static int64_t makeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    int32_t upper_id = w_id * Warehouse::MAX_WAREHOUSE_ID + d_id;
    assert(upper_id > 0);
    int64_t id = static_cast<int64_t>(upper_id) << 32 | o_id;
    assert(id > 0);
    return id;
}

NewOrder *TPCCTables::insertNewOrder(int32_t w_id, int32_t d_id, int32_t o_id) {
    NewOrder *neworder = new NewOrder();
    neworder->no_w_id = w_id;
    neworder->no_d_id = d_id;
    neworder->no_o_id = o_id;

    return insertNewOrderObject(&neworders_, neworder);
}

NewOrder *TPCCTables::findNewOrder(int32_t w_id, int32_t d_id, int32_t o_id) {
    NewOrderMap::const_iterator it = neworders_.find(makeNewOrderKey(w_id, d_id, o_id));
    if (it == neworders_.end()) return NULL;
    assert(it->second != NULL);
    return it->second;
}

History *TPCCTables::insertHistory(const History &history, bool is_orig, bool relearn) {
    if (!is_orig) {
        if (!relearn) history_raman.push_back(RamanCompress(forest_history_, &history));
        else {
            block_history_.Append(history, 0);
            if (block_history_.IsFull())
                block_history_.BlockCompress(history_raman, nullptr, nullptr);
        }
        return nullptr;
    } else {
        History *h = new History(history);
        history_.push_back(h);
        return h;
    }
}

void TPCCTables::OrderlineToCSV(int64_t num_warehouses) {
    std::ios::sync_with_stdio(false);
    std::ofstream ol_f("orderline.csv", std::ofstream::trunc);
    for (int32_t i = 1; i <= num_warehouses; ++i) {
        for (int32_t j = 1; j <= District::NUM_PER_WAREHOUSE; ++j) {
            for (int32_t k = 1; k <= Order::INITIAL_ORDERS_PER_DISTRICT; ++k) {
                for (int32_t l = 1; l <= Order::MAX_OL_CNT; ++l) {
                    OrderLine *ol = findOrderLine(i, j, k, l, true);
                    if (ol == nullptr)
                        continue;
                    ol_f << ol->ol_i_id << ","
                         << ol->ol_amount << ","
                         << ol->ol_number << ","
                         << ol->ol_supply_w_id << ","
                         << ol->ol_quantity << ","
                         << ol->ol_delivery_d << ","
                         << ol->ol_dist_info << ","
                         << ol->ol_o_id << ","
                         << ol->ol_d_id << ","
                         << ol->ol_w_id << "\n";
                }
            }
        }
    }
    ol_f.close();
}

void TPCCTables::StockToCSV(int64_t num_warehouses) {
    std::ios::sync_with_stdio(false);
    std::ofstream stock_f("stock.csv", std::ofstream::trunc);
    for (int32_t i = 1; i <= num_warehouses; ++i) {
        for (int32_t j = 1; j <= Stock::NUM_STOCK_PER_WAREHOUSE; ++j) {
            Stock *s = findStock(i, j, true);
            assert(s != nullptr);
            stock_f << s->s_i_id << ","
                    << s->s_w_id << ","
                    << s->s_quantity << ","
                    << s->s_ytd << ","
                    << s->s_order_cnt << ","
                    << s->s_remote_cnt << ",";
            stock_f << s->s_data << ",";
            for (int32_t k = 0; k < District::NUM_PER_WAREHOUSE; k++)
                stock_f << s->s_dist[k] << ",";
            stock_f << "\n";
        }
    }
    stock_f.close();
}

void TPCCTables::CustomerToCSV(int64_t num_warehouses) {
    std::ios::sync_with_stdio(false);
    std::ofstream cus_f("customer.csv", std::ofstream::trunc);
    for (int32_t i = 1; i <= num_warehouses; ++i) {
        for (int32_t j = 1; j <= District::NUM_PER_WAREHOUSE; ++j) {
            for (int32_t k = 1; k <= Customer::NUM_PER_DISTRICT; ++k) {
                Customer *c = findCustomer(i, j, k, true);
                assert(c != nullptr);
                cus_f << c->c_id << ","
                      << c->c_d_id << ","
                      << c->c_w_id << ","
                      << c->c_credit_lim << ","
                      << c->c_discount << ","
                      << c->c_delivery_cnt << ","
                      << c->c_balance << ","
                      << c->c_ytd_payment << ","
                      << c->c_payment_cnt << ","
                      << c->c_credit << ","
                      << c->c_last << ","
                      << c->c_first << ","
                      << c->c_middle << ","
                      << c->c_street_1 << ","
                      << c->c_street_2 << ","
                      << c->c_city << ","
                      << c->c_state << ","
                      << c->c_zip << ","
                      << c->c_phone << ","
                      << c->c_since << ","
                      << c->c_data << "\n";
            }
        }
    }
    cus_f.close();
}

void TPCCTables::HistoryToCSV(int64_t num_warehouses) {
    std::ios::sync_with_stdio(false);
    std::ofstream his_f("history.csv", std::ofstream::trunc);
    for (auto &h: history_) {
        his_f << h->h_c_id << ","
              << h->h_c_d_id << ","
              << h->h_c_w_id << ","
              << h->h_d_id << ","
              << h->h_w_id << ","
              << h->h_date << ","
              << h->h_amount << ","
              << h->h_data << "\n";
    }
    his_f.close();
}

void TPCCTables::DeleteDiskData() {
    close(stock_fd);
    close(orderline_fd);
    close(customer_fd);
    remove(Stock::TABLE_NAME);
    remove(OrderLine::TABLE_NAME);
    remove(Customer::TABLE_NAME);
}

int64_t TPCCTables::itemSize() {
    int32_t ret = 0;
    for (Item &item: items_) ret += item.Size();
    return ret;
}

int64_t TPCCTables::warehouseSize() {
    return BTreeSize(&warehouses_);
}

int64_t TPCCTables::districtSize() {
    return BTreeSize(&districts_);
}

int64_t TPCCTables::stockSize() {
    int64_t ret = 0;
    int32_t key = std::numeric_limits<int32_t>::max();
    Tuple<BitStream> *value;
    while (stock_raman.findLastLessThan(key, &value, &key)) {
        if (value == nullptr) throw std::runtime_error("null value in stockSize");
        if (value->in_memory_) ret += value->data_.size();
    }
    return ret;
}

int64_t TPCCTables::diskTableSize(const std::string &file_name) {
    if (file_name == "stock") return DiskTableSize<Stock>(stock_fd);
    else if (file_name == "orderline") return DiskTableSize<OrderLine>(orderline_fd);
    else if (file_name == "customer") return DiskTableSize<Customer>(customer_fd);
    else {
        throw std::runtime_error("Unknown file name");
    }
}

int64_t TPCCTables::customerSize() {
    int64_t ret = 0;
    int32_t key = std::numeric_limits<int32_t>::max();
    Tuple<BitStream> *value;
    while (customer_raman.findLastLessThan(key, &value, &key)) {
        if (value == nullptr) throw std::runtime_error("null value in customerSize");
        if (value->in_memory_) ret += value->data_.size();
    }
    return ret;
}

int64_t TPCCTables::orderSize() {
    return BTreeSize(&orders_);
}

int64_t TPCCTables::orderlineSize() {
    int64_t ret = 0;
    int64_t key = std::numeric_limits<int64_t>::max();
    Tuple<BitStream> *value;
    while (orderline_raman.findLastLessThan(key, &value, &key)) {
        if (value == nullptr) throw std::runtime_error("null value in orderlineSize");
        if (value->in_memory_) ret += value->data_.size();
    }
    return ret;

}

int64_t TPCCTables::newOrderSize() {
    int64_t ret = 0;
    for (auto &no: neworders_) ret += no.second->size();
    return ret;
}

int64_t TPCCTables::historySize() {
    int64_t ret = 0;
    for (auto &h: history_) ret += h->size();
    return ret;
}

void TPCCTables::MountCompression(RamanCompressor *forest, const std::string &table_name) {
    if (table_name == "stock") {
        forest_stock_ = forest;

        int32_t key = std::numeric_limits<int32_t>::max();
        Stock *value = nullptr;
        while (stock_.findLastLessThan(key, &value, &key)) {
            if (value == nullptr) throw std::runtime_error("value is nullptr in MountCompression");
            insertStock(*value, false, false);
        }
    } else if (table_name == "customer") {
        forest_customer_ = forest;

        int32_t key = std::numeric_limits<int32_t>::max();
        Customer *value = nullptr;
        while (customers_.findLastLessThan(key, &value, &key)) {
            if (value == nullptr) throw std::runtime_error("value is nullptr in MountCompression");
            insertCustomer(*value, false, false);
        }
    } else if (table_name == "orderline") {
        forest_orderline_ = forest;

        int64_t key = std::numeric_limits<int64_t>::max();
        OrderLine *value = nullptr;
        while (orderlines_.findLastLessThan(key, &value, &key)) {
            if (value == nullptr) throw std::runtime_error("value is nullptr in MountCompression");
            insertOrderLine(*value, false, false);
        }
    } else if (table_name == "history") {
        forest_history_ = forest;

        for (auto &h: history_) insertHistory(*h, false, false);
    } else if (table_name == "order") {
        forest_order_ = forest;

        int64_t key = std::numeric_limits<int64_t>::max();
        Order *value = nullptr;
        while (orders_.findLastLessThan(key, &value, &key)) {
            if (value == nullptr) throw std::runtime_error("value is nullptr in MountCompression");
            insertOrder(*value, false, false);
        }
    } else {
        throw std::runtime_error("Unknown table name");
    }
}
