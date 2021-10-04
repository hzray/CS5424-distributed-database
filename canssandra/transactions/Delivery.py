from canssandra import cql
from datetime import datetime


class DeliveryHandler:
    def __init__(self, cql_session, w_id, carrier_id):
        self.session = cql_session
        self.w_id = w_id
        self.carrier_id = carrier_id

    def find_smallest_order(self, w_id, d_id):
        query = "SELECT * FROM CS5424.orders WHERE o_w_id = %s and o_d_id = %s and o_carrier_id = %s"
        args = [w_id, d_id, -1]
        return cql.select_one(self.session, query, args)

    def update_order_carrier_id(self, w_id, d_id, o_id, carrier_id):
        query = "UPDATE CS5424.orders SET o_carrier_id = %s WHERE o_w_id = %s and o_d_id = %s and o_id = %s"
        args = [carrier_id, w_id, d_id, o_id]
        cql.update(self.session, query, args)

    def select_order_line(self, w_id, d_id, o_id):
        query = "SELECT * FROM CS5424.order_line WHERE ol_w_id = %s and ol_d_id = %s and ol_o_id = %s"
        args = [w_id, d_id, o_id]
        return cql.select(self.session, query, args)

    def update_delivery_d(self, w_id, d_id, o_id, ol_number, t):
        query = "UPDATE CS5424.order_line SET ol_delivery_d = %s " \
                "WHERE ol_w_id = %s and ol_d_id = %s and ol_o_id = %s and ol_number = %s"
        args = [t, w_id, d_id, o_id, ol_number]
        cql.update(self.session, query, args)

    def update_customer(self, w_id, d_id, c_id, balance, cnt):
        query = "UPDATE CS5424.customer SET c_balance = %s, c_delivery_cnt = %s " \
                "WHERE c_w_id = %s and c_d_id = %s and c_id = %s"
        args = [balance, cnt, self.w_id, d_id, c_id]
        cql.update(self.session, query, args)

    def select_customer(self, w_id, d_id, c_id):
        query = "SELECT * FROM CS5424.customer WHERE c_w_id = %s and c_d_id = %s and c_id = %s"
        args = [w_id, d_id, c_id]
        return cql.select_one(self.session, query, args)

    def sum_order_amount(self, w_id, d_id, o_id):
        query = "SELECT * FROM CS5424.order_line WHERE ol_w_id = %s and ol_d_id = %s and ol_o_id = %s"
        args = [w_id, d_id, o_id]
        rows = cql.select(self.session, query, args)
        amount = 0
        for row in rows:
            amount += row.ol_amount
        return amount

    def run(self):
        for d_id in range(1, 11):
            smallest_order = self.find_smallest_order(self.w_id, d_id)
            o_id = smallest_order.o_id
            print("w_id = {}, d_id={}, o_id={}".format(self.w_id, d_id, o_id))
            self.update_order_carrier_id(self.w_id, d_id, o_id, self.carrier_id)

            order_lines = self.select_order_line(self.w_id, d_id, o_id)
            for ol in order_lines:
                self.update_delivery_d(self.w_id, d_id, o_id, ol.ol_number, datetime.now())

            customer = self.select_customer(self.w_id, d_id, smallest_order.o_c_id)
            balance = customer.c_balance
            delivery_cnt = customer.c_delivery_cnt
            total_order_amount = self.sum_order_amount(self.w_id, d_id, o_id)
            self.update_customer(self.w_id, d_id, customer.c_id, balance+total_order_amount, delivery_cnt+1)







