from decimal import Decimal
import time
from datetime import datetime


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
            # cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute("SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CS5424.customer "
                        "WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", (self.w_id, self.d_id, self.c_id))
            # "WHERE C_ID = %s AND C_W_ID = %s AND C_D_ID = %s", (self.c_id, self.w_id, self.d_id))
            rows = cur.fetchall()
            for row in rows:
                self.c_discount = row[0]
                self.c_last = row[1]
                self.c_credit = row[2]
                break
        conn.commit()


class NewOrder:
    def __init__(self, conn, c_id, w_id, d_id, num_items, file_out):
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

        self.lines = []
        self.i_id_set = []
        self.i_w_id_set = []
        self.i_quan_set = []
        self.stock_dic = {}
        self.items_info_dic = {}

        self.r_customer_set = []

        self.output_str = ''

    def new_order_input(self, lines, num_items):
        self.lines = lines
        for i in range(0, int(num_items)):
            line = lines[i]
            line = line.strip()
            item = line.split(',')
            self.i_id_set.append(item[0])
            self.i_w_id_set.append(item[1])
            self.i_quan_set.append(item[2])

    def new_order_output(self):
        # print(self.output_str, self.file_out)
        self.file_out.write(self.output_str)
        # print(output_str)

    def select_stock(self):
        d_id_str = str(self.d_id).zfill(2)
        # S_YTD + {}, S_ORDER_CNT + {}, S_REMOTE_CNT + {}
        # print(len(self.i_id_set))
        sql_str = ""
        if len(self.i_id_set) == 0:
            return
        for idx in range(0, len(self.i_id_set)):
            if idx == 0:
                sql_str += "SELECT S_I_ID, S_QUANTITY, S_DIST_{}, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM CS5424.stock " \
                           "WHERE (S_W_ID = {} AND S_I_ID = {}) ". \
                    format(d_id_str, self.i_w_id_set[0], self.i_id_set[0])
            else:
                sql_str += "OR (S_W_ID = {} AND S_I_ID = {}) ". \
                    format(self.i_w_id_set[idx], self.i_id_set[idx])

        with self.conn.cursor() as cur:
            cur.execute(sql_str)
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            key = str(row[0])
            if row[3] is None:
                val = (row[1], row[2], 0, row[4], row[5])
            else:
                val = (row[1], row[2], row[3], row[4], row[5])
            self.stock_dic[key] = val

    def select_items(self):
        with self.conn.cursor() as cur:
            cur.execute("""SELECT I_ID, I_PRICE, I_NAME FROM CS5424.item WHERE I_ID IN %s""", (tuple(self.i_id_set),))
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            key = str(row[0])
            val = (row[1], row[2])
            self.items_info_dic[key] = val

    def update_stock_ol(self):
        self.total_amount = 0
        sql_str = "UPSERT INTO CS5424.stock (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT) "
        sql_str1 = "INSERT INTO CS5424.order_line (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, " \
                   "OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D, OL_DIST_INFO, OL_I_NAME) "
        stock_num = 0
        for i in range(0, len(self.i_id_set)):
            if i not in self.stock_dic.keys():
                continue
            i_id = str(self.i_id_set[i])
            i_name = self.items_info_dic[i_id][1]
            i_w_id = self.i_w_id_set[i]
            i_quantity = self.i_quan_set[i]
            i_amount = Decimal(i_quantity) * Decimal(self.items_info_dic[i_id][0])
            self.total_amount += i_amount

            ol_dist_info = self.stock_dic[i_id][1]
            s_quantity = Decimal(self.stock_dic[i_id][0])
            i_quantity = Decimal(self.i_quan_set[i])
            ad_quantity = s_quantity - i_quantity
            if ad_quantity < 10:
                ad_quantity += 100

            s_remote_cnt = int(self.stock_dic[i_id][4])
            if self.i_w_id_set[i] != self.w_id:
                s_remote_cnt += 1

            w_id = self.i_w_id_set[i]

            s_ytd = Decimal(self.stock_dic[i_id][2]) + i_quantity
            s_order_cnt = int(self.stock_dic[i_id][3]) + 1

            if i != 0:
                sql_str += ", ({}, {}, {}, {}, {}, {})". \
                    format(w_id, i_id, ad_quantity, s_ytd, s_order_cnt, s_remote_cnt)
            else:
                sql_str += "VALUES ({}, {}, {}, {}, {}, {})". \
                    format(w_id, i_id, ad_quantity, s_ytd, s_order_cnt, s_remote_cnt)

            if i != 0:
                sql_str1 += ", ({}, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', {})". \
                    format(self.o_id, self.d_id, self.w_id, i, i_id, i_w_id,
                           i_quantity, i_amount, '1970-01-01 00:00:00', ol_dist_info, i_name)
            else:
                sql_str1 += "VALUES ({}, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', {})". \
                    format(self.o_id, self.d_id, self.w_id, i, i_id, i_w_id,
                           i_quantity, i_amount, '1970-01-01 00:00:00', ol_dist_info, i_name)

            self.output_str += "\ni_id = {}, i_name={}, i_w_id={}, ol_amount={}, s_quantity={}". \
                format(i_id, i_name, i_w_id, i_amount, s_quantity)
            stock_num += 1

        if stock_num >= 1:
            with self.conn.cursor() as cur:
                cur.execute(sql_str)
                cur.execute(sql_str1)
            self.conn.commit()

    def new_order_handler(self):
        self.new_order_transaction()
        self.new_order_output()
        self.item_relate_customer()

    def find_customer(self, warehouse_id, district_id, order_id):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute(
                "Select o_c_id from CS5424.orders where o_w_id = %s and o_d_id = %s and o_id = %s",
                [warehouse_id, district_id, order_id])
            rows = cur.fetchall()
            self.conn.commit()

            for row in rows:
                c_id = row

            return c_id

    def find_customer_dic(self, o_id_set):
        customer_dic = {}
        with self.conn.cursor() as cur:
            cur.execute("""SELECT O_C_ID, O_ID FROM CS5424.orders WHERE O_W_ID = %s AND O_ID IN %s""",
                        (self.w_id, tuple(o_id_set)))
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            customer_dic[row[1]] = row[0]

        return customer_dic

    def item_relate_customer(self):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute("""SELECT OL_W_ID, OL_D_ID, OL_O_ID, COUNT(*) FROM CS5424.order_line 
            WHERE OL_W_ID != %s AND OL_D_ID = %s AND OL_I_ID IN %s GROUP BY (OL_W_ID, OL_D_ID, OL_O_ID)""",
                        (self.w_id, self.d_id, tuple(self.i_id_set)))
            rows = cur.fetchall()
        self.conn.commit()
        o_id_set = []
        for row in rows:
            o_id_set.append(row[1])

        customer_dic = self.find_customer_dic(o_id_set)

        sql_str = "INSERT INTO CS5424.related_customer VALUES "
        i = 0
        for row in rows:
            if row[3] >= 2:

                if i == 0:
                    sql_str += "({}, {}, {}, {}, {}, {})".\
                        format(self.w_id, self.d_id, self.c_id, row[0], row[1], customer_dic[row[2]])
                else:
                    sql_str += ", ({}, {}, {}, {}, {}, {})".\
                        format(self.w_id, self.d_id, self.c_id, row[0], row[1], customer_dic[row[2]])
                # self.r_customer_set.append(self.w_id, self.d_id, self.c_id, row[0], row[1], row[3])
                i += 1
        # print(sql_str)
        if i == 0:
            return
        with self.conn.cursor() as cur:
            cur.execute(sql_str)
        self.conn.commit()

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

            cur.execute("UPDATE CS5424.district SET D_NEXT_O_ID = D_NEXT_O_ID + %s WHERE D_W_ID = %s AND D_ID = %s",
                        (1, self.w_id, self.d_id))
        self.conn.commit()
        # step3
        o_all_local = 0
        for w_id in self.i_w_id_set:
            if w_id == self.w_id:
                o_all_local = 1
                break
        with self.conn.cursor() as cur:

            cur.execute("""INSERT INTO CS5424.orders 
                        (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """,
                        (self.o_id, self.d_id, self.w_id, self.c_id, self.o_entry_d, 0, self.num_items, o_all_local))
        self.conn.commit()

        self.output_str = "\n1. customer information: w_id: {}, d_id: {}, c_id: {}, lastname: {}, credit: {}, discount: {}". \
                              format(self.w_id, self.d_id, self.c_id, self.customer.c_last, self.customer.c_credit,
                                     self.customer.c_discount) + \
                          "\n2. warehouse tax rate: {}, district tax rate: {}".format(self.w_tax, self.d_tax) + \
                          "\n3. order number: {}, entry date: {}".format(self.o_id, self.o_entry_d) + \
                          "\n4. number of items: {}, total amount for order: {}".format(self.num_items,
                                                                                        self.total_amount) + \
                          "\n5. items information: "

        # step5
        self.select_stock()
        self.select_items()
        self.update_stock_ol()

        # count total_amount with tax
        self.total_amount = self.total_amount * (1 + self.d_tax + self.w_tax) * (1 - self.customer.c_discount)

        end = time.time()
        latency = end - start
        return latency
