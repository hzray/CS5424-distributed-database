import decimal
import psycopg2
import random
import time
from datetime import datetime


class OrderItem:
    item_id = 0
    warehouse_id = 0
    quantity = 0
    price = 0
    name = ''
    ol_amount = 0
    s_quantity = 0

    def __init__(self, item_id, warehouse_id, quantity):
        self.item_id = int(item_id)
        self.warehouse_id = int(warehouse_id)
        self.quantity = decimal.Decimal(quantity)

    def select_item_info(self, conn):
        with conn.cursor() as cur:
            cur.execute("SELECT I_PRICE, I_NAME FROM CS5424.item WHERE I_ID = %s", (self.item_id,))
            rows = cur.fetchall()
            for row in rows:
                self.price = row[0]
                self.name = row[1]
                break
        conn.commit()

    def item_info_str(self, index):
        output_str = "    {}. item number: {}, item name: {}, supplier warehouse: {}, " \
                     "quantity: {}, order line amount: {}, stock quantity: {} ". \
            format(str(index), self.item_id, self.name, self.warehouse_id, self.quantity,
                   self.ol_amount, self.s_quantity)
        return output_str


def new_order_input(lines, num_items):
    items = []
    for i in range(0, int(num_items)):
        # line = input('item: ' + str(i) + ', please input the information of item: ')
        # line = input()
        line = lines[i]
        item = line.split(',')
        items.append(OrderItem(item[0], item[1], item[2]))
    return items


class Customer:
    w_id = 0
    d_id = 0
    c_id = 0
    c_last = ''
    c_credit = 0
    c_discount = 0

    def __init__(self, c_id, w_id, d_id):
        self.w_id = int(w_id)
        self.d_id = int(d_id)
        self.c_id = int(c_id)

    def select_c_info(self, conn):
        with conn.cursor() as cur:
            cur.execute("SELECT C_DISCOUNT, C_LAST, C_DISCOUNT FROM CS5424.customer WHERE C_ID = %s", (self.c_id,))
            rows = cur.fetchall()
            for row in rows:
                self.c_discount = row[0]
                self.c_last = row[1]
                self.c_discount = row[2]
                break
        conn.commit()


class NewOrder:
    def __init__(self, conn, c_id, w_id, d_id, num_items, items, file_out):
        self.items = items
        self.file_out = file_out
        self.conn = conn
        self.c_id = int(c_id)
        self.w_id = int(w_id)
        self.d_id = int(d_id)
        self.num_items = int(num_items)
        self.customer = Customer(c_id, w_id, d_id)

        self.w_tax = 0
        self.d_tax = 0
        self.o_id = 0
        self.o_entry_d = datetime.now()
        self.total_amount = 0

    def new_order_output(self):
        output_str = "\n1. customer information: w_id: {}, d_id: {}, c_id: {}, lastname: {}, credit: {}, discount: {}". \
                         format(self.w_id, self.d_id, self.c_id, self.customer.c_last, self.customer.c_credit,
                                self.customer.c_discount) + \
                     "\n2. warehouse tax rate: {}, district tax rate: {}".format(self.w_tax, self.d_tax) + \
                     "\n3. order number: {}, entry date: {}".format(self.o_id, self.o_entry_d) + \
                     "\n4. number of items: {}, total amount for order: {}".format(self.num_items, self.total_amount) + \
                     "\n5. items information: "
        index = 0
        for item in self.items:
            output_str += "\n" + item.item_info_str(index)
            index += 1
        print(output_str, file=self.file_out)

    def new_order_handler(self):
        self.new_order_transaction()
        self.new_order_output()


    def new_order_transaction(self):
        start = time.time()
        self.customer.select_c_info(self.conn)
        with self.conn.cursor() as cur:
            cur.execute("SELECT W_TAX FROM CS5424.warehouse WHERE W_ID = %s", (self.w_id,))
            rows = cur.fetchall()
            for row in rows:
                self.w_tax = row[0]
                break
        self.conn.commit()
        # step1
        with self.conn.cursor() as cur:
            cur.execute("SELECT D_NEXT_O_ID, D_TAX FROM CS5424.district WHERE D_W_ID = %s AND D_ID = %s",
                        (self.w_id, self.d_id))
            rows = cur.fetchall()
            for row in rows:
                self.o_id = row[0]
                self.d_tax = row[1]
                break
        self.conn.commit()
        # step2
        with self.conn.cursor() as cur:
            cur.execute("UPDATE CS5424.district SET D_NEXT_O_ID = %s WHERE D_W_ID = %s AND D_ID = %s",
                        (self.o_id + 1, self.w_id, self.d_id))
        self.conn.commit()
        # step3
        o_all_local = 0
        for item in self.items:
            if item.warehouse_id == self.w_id:
                o_all_local = 1
                break
        with self.conn.cursor() as cur:
            cur.execute("""INSERT INTO CS5424.orders 
                        (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """,
                        (self.o_id, self.d_id, self.w_id, self.c_id, self.o_entry_d, 0, self.num_items, o_all_local))
        self.conn.commit()
        # step4
        self.total_amount = 0

        # step5
        index = 0
        for item in self.items:
            # update stock
            d_id_str = str(self.d_id)
            d_id_str = d_id_str.zfill(2)
            with self.conn.cursor() as cur:
                cur.execute("""SELECT S_QUANTITY, S_REMOTE_CNT, S_DIST_{} 
                            FROM CS5424.stock WHERE S_W_ID = %s AND S_I_ID = %s""".format(d_id_str),
                            (item.warehouse_id, item.item_id))
                rows = cur.fetchall()
                for row in rows:
                    s_quantity = row[0]
                    s_remote_cnt = row[1]
                    s_dist_xx = row[2]
                    break
            self.conn.commit()
            quantity = item.quantity
            adjusted_qty = s_quantity - quantity
            if adjusted_qty < 10:
                adjusted_qty = adjusted_qty + 100
            self.items[index].s_quantity = adjusted_qty
            item = self.items[index]
            if self.w_id == item.warehouse_id:
                s_remote_cnt += 1
            with self.conn.cursor() as cur:
                cur.execute("""UPDATE CS5424.stock SET S_QUANTITY = %s, S_YTD = S_YTD + %s, 
                                S_ORDER_CNT = S_ORDER_CNT + %s, S_REMOTE_CNT = %s 
                                WHERE S_I_ID = %s AND S_W_ID = %s""",
                            (adjusted_qty, quantity, 1, s_remote_cnt, item.item_id, item.warehouse_id))
            self.conn.commit()
            # count total_amount
            self.items[index].select_item_info(self.conn)

            item = self.items[index]
            item_amount = item.quantity * item.price
            self.total_amount += item_amount
            # create a new order-line
            self.items[index].ol_amount = item_amount
            item = self.items[index]
            with self.conn.cursor() as cur:
                cur.execute("""INSERT INTO CS5424.order_line (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, 
                                OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D, OL_DIST_INFO)  
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (self.o_id, self.d_id, self.w_id, index, item.item_id, item.warehouse_id, item.quantity,
                             item_amount, '1970-01-01 00:00:00', s_dist_xx))
            self.conn.commit()

            # count total_amount with tax
            self.total_amount = self.total_amount * (1 + self.d_tax + self.w_tax) * (1 - self.customer.c_discount)
            index += 1

        end = time.time()
        latency = end - start
        return latency
