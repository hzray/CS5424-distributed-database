import decimal
from datetime import datetime
import psycopg2
import logging

class PopularItem:
    def __init__(self, conn, w_id, d_id, l, fo):
        self.conn = conn
        self.w_id = int(w_id)
        self.d_id = int(d_id)
        self.l = int(l)
        self.fo = fo

        self.d_next_o_id = 0
        self.output_str = ""
        self.customer_dic = {}
        self.item_dic = {}
        self.c_id_set = []
        self.order_set = []
        self.ol_p_dic = {}
        self.o_id_set = []
        self.ol_max_quan_set = []
        self.c_id_set = []

    def select_district(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "Select d_next_o_id from CS5424.district where d_w_id = %s and d_id = %s", [self.w_id, self.d_id])
            rows = cur.fetchall()
            self.conn.commit()
            for row in rows:
                d_next_o_id = row[0]
            return d_next_o_id

    def select_orders(self, o_id_list):
        with self.conn.cursor() as cur:
            cur.execute(
                "Select o_id, o_c_id, o_entry_d from CS5424.orders where o_w_id = %s and o_d_id = %s and o_id in %s",
                [self.w_id, self.d_id, o_id_list])
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            self.order_set.append(row)
            self.o_id_set.append(row[0])
            self.c_id_set.append(row[1])
            self.output_str += "\nOrder number: o_id = {}, entry date and time: o_entry_d = {}". \
                format(row[0], row[2])

    def select_customer(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "Select c_id, c_first, c_middle, c_last, c_balance from CS5424.customer"
                " where c_w_id = %s and c_d_id = %s and c_id in %s",
                [self.w_id, self.d_id, tuple(self.c_id_set)])
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            key = row[0]
            val = (row[1], row[2], row[3], row[4])
            self.output_str += "\nName of customer: c_first = {}, c_middle = {}, c_last = {}". \
                format(row[1], row[2], row[3])
            self.customer_dic [key] = val


    def get_max_quantity(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "Select max(ol_quantity) from CS5424.order_line where ol_w_id = %s and ol_d_id = %s and "
                "ol_o_id in %s group by ol_o_id",
                [self.w_id, self.d_id, tuple(self.o_id_set)])
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            self.ol_max_quan_set.append(row[0])

    def select_order_lines(self):

        with self.conn.cursor() as cur:
            cur.execute(
                "Select ol_o_id, ol_i_id, ol_quantity, ol_i_name from CS5424.order_line where ol_w_id = %s and ol_d_id = %s"
                "and ol_quantity in %s",
                (self.w_id, self.d_id, tuple(self.ol_max_quan_set)))
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            key = row[0]
            self.ol_p_dic[key] = (row[1], row[2], row[3])
            self.output_str += "\nItem name: i_name = {}, Quantity order: ol_quantity = {}". \
                format(row[3], row[2])

    # def select_item(self, item_id_list):
    #     with self.conn.cursor() as cur:
    #         cur.execute(
    #             "Select i_id, i_name from CS5424.item where i_id in %s", (tuple(item_id_list),))
    #         rows = cur.fetchall()
    #     self.conn.commit()
    #     for row in rows:
    #         key = row[0]
    #         self.item_dic[key] = row[1]

    def popularItem_handler(self):
        # print("1. District identifier: w_id = {}, d_id = {}".format(self.w_id, self.d_id))
        # print("2. Number of last orders to be examined: l = {}".format(self.l))
        self.output_str += "\n1. District identifier: w_id = {}, d_id = {}".format(self.w_id, self.d_id) + \
                           "\n2. Number of last orders to be examined: l = {}".format(self.l)

        # step 1
        self.d_next_o_id = self.select_district()
        # print("Debug: d_next_o_id: ", self.d_next_o_id)

        # step 2
        o_id_list = []
        for i in range(self.d_next_o_id - self.l, self.d_next_o_id):
            o_id_list.append(i)
        o_id_list = tuple(o_id_list)
        self.select_orders(o_id_list)

        # step 3
        # print("3. Orders Information: ")
        self.output_str += "\n3. Orders Information: "
        self.select_customer()
        self.select_order_lines()
        items = {}
        for ol in self.ol_p_dic:
            item_name = ol[2]
            item_count = items.get(item_name) if item_name in items else 0
            items.update({item_name: item_count + 1})
            # print("Item name: i_name = {}, Quantity order: ol_quantity = {}".format(item_name, order_line[1]))
        distinct_items = set(items.keys())
        # print("4. The percentage of examined orders:")
        self.output_str += "\n4. The percentage of examined orders:"
        for item in distinct_items:
            percentage = float(items.get(item)) / (float(self.l) * 100.0)
            # print("Item name: i_name = {}, the percentage = {}".format(item, percentage))
            self.output_str += "Item name: i_name = {}, the percentage = {}".format(item, percentage)
        self.fo.write(self.output_str)
