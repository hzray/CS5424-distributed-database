import sys
from collections import Counter

from transactions.cql import utils
from datetime import datetime


def new_order_input_helper(n):
    items = []
    for i in range(0, n):
        line = sys.stdin.readline()
        info = line.split(",")
        items.append(OrderItem(int(info[0]), int(info[1]), int(info[2])))
    return items


class OrderItem:
    Id = 0
    supplier_id = 0
    quantity = 0
    price = 0
    name = ""
    stock_quantity = 0

    def __init__(self, item_id, supplier_id, quantity):
        self.Id = item_id
        self.supplier_id = supplier_id
        self.quantity = quantity


class NewOrderHandler:
    def __init__(self, cql_session, query, workload, c_id, w_id, d_id, n_item):
        self.session = cql_session
        self.query = query
        self.workload = workload
        self.w_id = int(w_id)
        self.d_id = int(d_id)
        self.c_id = int(c_id)
        self.n_item = int(n_item)

    def update_district_o_id_counter(self, w_id, d_id):
        args = [w_id, d_id]
        utils.update(self.session, self.query.update_d_o_id_counter, args)

    def update_stock(self, stock, w_id, quantity):
        qty = stock.s_quantity - quantity
        if qty < 10:
            qty += 100
        # update stock quantity
        args = [qty, stock.s_w_id, stock.s_i_id]
        utils.update(self.session, self.query.update_stock_quantity, args)

        # update counters
        args = [quantity, stock.s_w_id, stock.s_i_id]
        utils.update(self.session, self.query.update_stock_ytd_and_order_cnt_counter, args)
        if w_id != stock.s_w_id:
            args = [stock.s_w_id, stock.s_i_id]
            utils.update(self.session, self.query.inc_stock_remote_cnt, args)
        return qty

    def insert_order_line(self, w_id, d_id, o_id, i, i_id, t, sup_id, qty, item_amount, dist_info):
        args = [w_id, d_id, o_id, i, i_id, t, item_amount, sup_id, qty, dist_info]
        utils.insert(self.session, self.query.insert_ol, args)

    def insert_order(self, w_id, d_id, c_id, o_id, all_local, t):
        args = [w_id, d_id, o_id, c_id, -1, self.n_item, all_local, t]
        utils.insert(self.session, self.query.insert_order, args)

    def select_district(self, w_id, d_id):
        args = [w_id, d_id]
        return utils.select_one(self.session, self.query.select_district, args)

    def select_district_o_id_change(self, w_id, d_id):
        args = [w_id, d_id]
        district = utils.select_one(self.session, self.query.select_district_o_id_change, args)
        if district is None:
            return 0
        if district.d_o_id_change is None:
            return 0
        return district.d_o_id_change

    def select_item(self, i_id):
        args = [i_id]
        return utils.select_one(self.session, self.query.select_item, args)

    def select_stock(self, sup_id, i_id):
        args = [sup_id, i_id]
        return utils.select_one(self.session, self.query.select_stock, args)

    def select_warehouse(self, w_id):
        args = [w_id]
        return utils.select_one(self.session, self.query.select_warehouse, args)

    def select_customer(self, w_id, d_id, c_id):
        args = [w_id, d_id, c_id]
        return utils.select_one(self.session, self.query.select_customer, args)

    def insert_customer_order_items(self, w_id, d_id, c_id, o_id, i_id):
        args = [w_id, d_id, c_id, o_id, i_id]
        utils.insert(self.session, self.query.insert_coi, args)

    def insert_customer_order(self, w_id, d_id, c_id, o_id, i_ids):
        args = [w_id, d_id, c_id, o_id, i_ids]
        utils.insert(self.session, self.query.insert_co, args)

    def update_related_customer(self, w_id, d_id, c_id, items):
        for w in range(1, 11):
            if w == w_id:
                continue
            for d in range(1, 11):
                ids = self.helper(w, d, items)
                ids = set(ids)
                for r_c_id in ids:
                    args = [w_id, d_id, c_id, w, d, r_c_id]
                    utils.insert(self.session, self.query.insert_related_customer, args)

    def helper(self, r_w_id, r_d_id, items):
        args = [r_w_id, r_d_id, items]
        related_order_line = list(utils.select(self.session, self.query.select_customer_order_items_in, args))
        o_ids = [row.coi_o_id for row in related_order_line]
        counter = Counter(o_ids)
        related_orders = [c for c in counter if counter[c] >= 2]

        c_ids = []
        for ol in related_order_line:
            if ol.coi_o_id in related_orders:
                c_ids.append(ol.coi_c_id)
        return c_ids

    def run(self):
        items = new_order_input_helper(self.n_item)
        # Step 1 and 2
        district = self.select_district(self.w_id, self.d_id)
        o_id = district.d_base_o_id + self.select_district_o_id_change(self.w_id, self.d_id)
        self.update_district_o_id_counter(self.w_id, self.d_id)

        # Step 3
        all_local = 1
        for item in items:
            if item.supplier_id != self.w_id:
                all_local = 0
                break

        t = datetime.now()
        self.insert_order(self.w_id, self.d_id, self.c_id, o_id, all_local, t)

        # Step 4
        total_amount = 0

        # Step 5
        i = 1
        for item in items:
            item_info = self.select_item(item.Id)
            item.price = item_info.i_price
            item.name = item_info.i_name

            stock = self.select_stock(item.supplier_id, item.Id)
            adjusted_quantity = self.update_stock(stock, self.w_id, item.quantity)
            item.stock_quantity = adjusted_quantity

            item_amount = item.price * item.quantity
            total_amount += item_amount

            # query result is a namedtuple, field can be accessed by getattr
            d_id_str = str(self.d_id)
            d_id_str = d_id_str.zfill(2)
            dist_info = getattr(stock, 's_dist_' + d_id_str)

            self.insert_order_line(self.w_id, self.d_id, o_id, i, item.Id, datetime(1970, 1, 1, 0, 0), item.supplier_id,
                                   item.quantity,
                                   item_amount, dist_info)

            i += 1

        # Step 6
        warehouse = self.select_warehouse(self.w_id)
        customer = self.select_customer(self.w_id, self.d_id, self.c_id)
        c_discount = customer.c_discount
        total_amount = total_amount * (1 + district.d_tax + warehouse.w_tax) * (1 - c_discount)

        # Output
        customer_output = "customer: W_ID = {}, D_ID = {}, C_ID = {}, C_LAST = {}, C_CREDIT = {}, C_DISCOUNT = {}" \
            .format(self.w_id, self.d_id, self.c_id, customer.c_last, customer.c_credit, customer.c_discount)
        print(customer_output)
        print("W_TAX = {}, D_TAX = {}".format(warehouse.w_tax, district.d_tax))
        print("O_ID = {}, O_ENTRY_D = {}".format(o_id, t))
        print("NUM_ITEMS = {}, TOTAL_AMOUNT = {}".format(self.n_item, total_amount))

        for item in items:
            item_str = "ITEM_NUMBER = {}," \
                       "I_NAME = {}, " \
                       "SUPPLIER_WAREHOUSE = {}, " \
                       "QUANTITY = {}, " \
                       "OL_AMOUNT = {}, " \
                       "S_QUANTITY = {}".format(item.Id,
                                                item.name,
                                                item.supplier_id,
                                                item.quantity,
                                                item.price * item.quantity,
                                                item.stock_quantity)
            print(item_str)

        # Insert into customer_order_items
        for item in items:
            self.insert_customer_order_items(self.w_id, self.d_id, self.c_id, o_id, item.Id)

        # if workload = B, update related customer
        if self.workload == 'B':
            self.update_related_customer(self.w_id, self.d_id, self.c_id, [item.Id for item in items])