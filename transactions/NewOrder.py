import cql
from datetime import datetime


def new_order_input_helper(n):
    items = []
    for i in range(0, n):
        line = input(
            "enter information for item{} in format [OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY]\n".format(i + 1))
        info = line.split(" ")
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
    def __init__(self, cql_session, c_id, w_id, d_id, n_item):
        self.session = cql_session
        self.w_id = w_id
        self.d_id = d_id
        self.c_id = c_id
        self.n_item = n_item

    def update_district_next_o_id(self, w_id, d_id, n):
        query = "UPDATE CS5424.district SET d_next_o_id = %s WHERE d_w_id=%s AND d_id=%s"
        args = [n, w_id, d_id]
        cql.update(self.session, query, args)

    def update_stock(self, stock, w_id, quantity):
        qty = stock.s_quantity - quantity
        if qty < 10:
            qty += 100

        ytd = stock.s_ytd + quantity
        order_cnt = stock.s_order_cnt + 1
        remote_cnt = stock.s_remote_cnt
        if w_id != stock.s_w_id:
            remote_cnt += 1

        query = "UPDATE CS5424.stock SET s_quantity = %s, s_ytd =  %s, s_order_cnt = %s, s_remote_cnt = %s " \
                "WHERE s_w_id = %s AND s_i_id = %s"
        args = [qty, ytd, order_cnt, remote_cnt, stock.s_w_id, stock.s_i_id]
        cql.update(self.session, query, args)
        return qty

    def insert_order_line(self, w_id, d_id, o_id, i, i_id, t, sup_id, qty, item_amount, dist_info):
        query = """
                    INSERT INTO 
                    CS5424.order_line(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, 
                                ol_supply_w_id, ol_quantity, ol_dist_info)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
        args = [w_id, d_id, o_id, i, i_id, t, item_amount, sup_id, qty, dist_info]
        cql.insert(self.session, query, args)

    def insert_order(self, w_id, d_id, c_id,  o_id, all_local, t):
        query = "INSERT INTO " \
                "CS5424.orders(o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        args = [w_id, d_id, o_id, c_id, 'unknown', self.n_item, all_local, t]
        cql.insert(self.session, query, args)

    def select_district(self, w_id, d_id):
        query = "SELECT * FROM CS5424.district WHERE d_w_id=%s AND d_id=%s"
        args = [w_id, d_id]
        return cql.select_one(self.session, query, args)

    def select_item(self, i_id):
        query = "SELECT * FROM CS5424.item WHERE i_id=%s"
        args = [i_id]
        return cql.select_one(self.session, query, args)

    def select_stock(self, sup_id, i_id):
        query = "SELECT * FROM CS5424.stock WHERE s_w_id=%s AND s_i_id=%s"
        args = [sup_id, i_id]
        return cql.select_one(self.session, query, args)

    def select_warehouse(self, w_id):
        query = "SELECT * FROM CS5424.warehouse where w_id = %s"
        args = [w_id]
        return cql.select_one(self.session, query, args)

    def select_customer(self, w_id, d_id, c_id):
        query = "SELECT * FROM CS5424.customer WHERE c_w_id = %s and c_d_id = %s and c_id = %s"
        args = [w_id, d_id, c_id]
        return cql.select_one(self.session, query, args)

    def run(self):
        items = new_order_input_helper(self.n_item)
        # Step 1
        district = self.select_district(self.w_id, self.d_id)
        o_id = district.d_next_o_id

        # Step 2
        self.update_district_next_o_id(self.w_id, self.d_id, o_id + 1)

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

            self.insert_order_line(self.w_id, self.d_id, o_id, i, item.Id, datetime(1970, 1, 1, 0, 0), item.supplier_id, item.quantity,
                                   item_amount, dist_info)

            i += 1

        # Step 6
        warehouse = self.select_warehouse(self.w_id)
        customer = self.select_customer(self.w_id, self.d_id, self.c_id)
        c_discount = customer.c_discount
        total_amount = total_amount * (1 + district.d_tax + warehouse.w_tax) * (1 - c_discount)

        # Output
        customer_output = "customer: W_ID = {}, D_ID = {}, C_ID = {}, C_LAST = {}, C_CREDIT = {}, C_DISCOUNT = {}"\
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
