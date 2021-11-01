import time
import psycopg2
import random

class Customer:
    def __init__(self, c_first, c_middle, c_last, c_balance):
        self.c_first = c_first
        self.c_middle = c_middle
        self.c_last = c_last
        self.c_balance = c_balance


class Order:
    def __init__(self, o_id, i_entry_d, o_carrier_id):
        self.o_id = o_id
        self.i_entry_d = i_entry_d
        self.o_carrier_id = o_carrier_id


class OrderLine:
    def __init__(self, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d):
        self.ol_i_id = ol_i_id
        self.ol_supply_w_id = ol_supply_w_id
        self.ol_quantity = ol_quantity
        self.ol_amount = ol_amount
        self.ol_delivery_d = ol_delivery_d


class OrderStatus:
    def __init__(self, conn, c_w_id, c_d_id, c_id, fo):
        self.conn = conn
        self.c_w_id = c_w_id
        self.c_d_id = c_d_id
        self.c_id = c_id
        self.fo = fo

        self.last_o_id = 0
        self.output_str = ""

    def order_status_handler(self):
        # step 1
        with self.conn.cursor() as cur:
            cur.execute("""SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CS5424.customer 
                            WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s""",
                        (self.c_w_id, self.c_d_id, self.c_id))
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            self.output_str += "\nCustomer's name: {} {} {}".format(row[0], row[1], row[2]) + \
                               "\nCustomer's balance: {}".format(row[3])
            break
        # step 2
        with self.conn.cursor() as cur:
            cur.execute("""SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM CS5424.orders
                            WHERE O_C_ID = %s AND O_W_ID = %s AND O_D_ID = %s""",
                        (self.c_id, self.c_w_id, self.c_d_id))
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            self.output_str += "Order number: {}, entry date and time: {}, carrier identifer: {}". \
                                   format(row[0], row[1], row[2])
            self.last_o_id = row[0]
            break
        # step 3
        with self.conn.cursor() as cur:
            cur.execute("""SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM CS5424.order_line 
                            WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s""",
                        (self.c_w_id, self.c_d_id, self.last_o_id))
            rows = cur.fetchall()
        self.conn.commit()
        i = 0
        self.output_str += "\nEach item in the customer's last order: "
        for row in rows:
            i += 1
            self.output_str += "\n{}. OL_I_ID: {}, OL_SUPPLY_W_ID: {}, OL_QUANTITY: {}, "\
                               "OL_AMOUNT: {}, OL_DELIVERY_D: {}". \
                                   format(i, row[0], row[1], row[2], row[3], row[4])
        print(self.output_str, file=self.fo)
