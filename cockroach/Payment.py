import time
import psycopg2
import random

class Payment:
    def __init__(self, conn, c_w_id, c_d_id, c_id, payment, fo):
        self.conn = conn
        self.c_w_id = c_w_id
        self.c_d_id = c_d_id
        self.c_id = c_id
        self.payment = payment
        self.fo = fo

        self.w_ytd = 0
        self.d_ytd = 0

        self.c_balance = 0
        self.c_ytd_payment = 0
        self.c_payment_cnt = 0

        self.output_str = ""

    def payment_handler(self):
        with self.conn.cursor() as cur:
            
            # step1
            cur.execute("UPDATE CS5424.warehouse SET W_YTD = W_YTD + %s WHERE W_ID = %s", (self.payment, self.c_w_id))
            # step2
            cur.execute("UPDATE CS5424.district SET D_YTD = D_YTD + %s WHERE D_W_ID = %s AND D_ID = %s",
                        (self.payment, self.c_w_id, self.c_d_id))
            # step3
            cur.execute("""UPDATE CS5424.customer SET C_BALANCE = C_BALANCE + %s, C_YTD_PAYMENT = C_YTD_PAYMENT + %s, 
                            C_PAYMENT_CNT = C_PAYMENT_CNT + %s
                            WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s""",
                        (self.payment, self.payment, 1,
                         self.c_w_id, self.c_d_id, self.c_id))
        self.conn.commit()
        # output
        with self.conn.cursor() as cur:
            
            cur.execute("""SELECT C_FIRST, C_MIDDLE, C_LAST,
                            C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
                            C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE,
                            C_YTD_PAYMENT, C_PAYMENT_CNT FROM CS5424.customer 
                            WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s""",
                        (self.c_w_id, self.c_d_id, self.c_id))
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            self.output_str += "\nCustomer's identifier: C_W_ID = {}, C_D_ID = {}, C_ID = {}". \
                                   format(self.c_w_id, self.c_d_id, self.c_id) + \
                               "\nCustomer's name: {} {} {}".format(row[0], row[1], row[2]) + \
                               "\nCustomer's address: {}, {}, {}, {}, {}". \
                                   format(row[3], row[4], row[5], row[6], row[7]) + \
                               "\nC_PHONE = {},"\
                               "\nC_SINCE = {}, C_CREDIT = {}, C_CREDIT_LIM = {}, C_DISCOUNT = {}, C_BALANCE = {}" \
                                   .format(row[8], row[9], row[10], row[11], row[12], row[13])
            break

        with self.conn.cursor() as cur:
            
            cur.execute("""SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP 
                            FROM CS5424.warehouse WHERE W_ID = %s""", (self.c_w_id,))
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            self.output_str += "\nWarehouse's address: {}, {}, {}, {}, {}". \
                format(row[0], row[1], row[2], row[3], row[4])
            break
        with self.conn.cursor() as cur:
            
            cur.execute("""SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP 
                            FROM CS5424.district WHERE D_W_ID = %s AND D_ID = %s""",
                        (self.c_w_id, self.c_d_id))
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            self.output_str += "\nDistrict's address: {}, {}, {}, {}, {}". \
                format(row[0], row[1], row[2], row[3], row[4])
            break
        self.output_str += "\nPayment amount: {}".format(self.payment)
        # print(self.output_str, self.fo)
        self.fo.write(self.output_str)
        # print(self.output_str)
