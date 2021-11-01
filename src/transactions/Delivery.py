from datetime import datetime
from transactions.cql import utils

class DeliveryHandler:
    def __init__(self, cql_session, query, w_id, carrier_id):
        self.session = cql_session
        self.query = query
        self.w_id = int(w_id)
        self.carrier_id = int(carrier_id)

    def find_smallest_order(self, w_id, d_id):
        args = [w_id, d_id, -1]
        return utils.select_one(self.session, self.query.select_order_with_carrier, args)

    def update_order_carrier_id(self, w_id, d_id, o_id, carrier_id):
        args = [carrier_id, w_id, d_id, o_id]
        utils.update(self.session, self.query.update_order_carrier_id, args)

    def select_order_line(self, w_id, d_id, o_id):
        args = [w_id, d_id, o_id]
        return utils.select(self.session, self.query.select_ol, args)

    def update_delivery_d(self, w_id, d_id, o_id, ol_number, t):
        args = [t, w_id, d_id, o_id, ol_number]
        utils.update(self.session, self.query.update_ol_deliver_d, args)

    def update_customer(self, w_id, d_id, c_id, balance, cnt):
        args = [balance, cnt, w_id, d_id, c_id]
        utils.update(self.session, self.query.update_customer_delivery, args)

    def select_customer(self, w_id, d_id, c_id):
        args = [w_id, d_id, c_id]
        return utils.select_one(self.session, self.query.select_customer, args)

    def sum_order_amount(self, w_id, d_id, o_id):
        args = [w_id, d_id, o_id]
        rows = utils.select(self.session, self.query.select_ol, args)
        amount = 0
        for row in rows:
            amount += row.ol_amount
        return amount

    def select_and_update_customer(self, w_id, d_id, c_id, o_id):

        customer = self.select_customer(w_id, d_id, c_id)
        balance = customer.c_balance
        delivery_cnt = customer.c_delivery_cnt
        total_order_amount = self.sum_order_amount(self.w_id, d_id, o_id)
        self.update_customer(w_id, d_id, customer.c_id, balance + total_order_amount, delivery_cnt + 1)

    def run(self):
        for d_id in range(1, 11):
            smallest_order = self.find_smallest_order(self.w_id, d_id)
            if smallest_order is None:
                continue
            o_id = smallest_order.o_id
            print("w_id = {}, d_id={}, o_id={}".format(self.w_id, d_id, o_id))
            self.update_order_carrier_id(self.w_id, d_id, o_id, self.carrier_id)

            order_lines = self.select_order_line(self.w_id, d_id, o_id)
            for ol in order_lines:
                self.update_delivery_d(self.w_id, d_id, o_id, ol.ol_number, datetime.now())

            # select and update customer
            return self.select_and_update_customer(self.w_id, d_id, smallest_order.o_c_id, o_id)
