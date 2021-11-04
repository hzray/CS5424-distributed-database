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
        counter = 0
        while counter < 3:
            try:
                order = utils.select_one(self.session, self.query.select_order_with_carrier, args)
                return order
            except Exception as e:
                print(e)
                counter += 1
        return None

    def update_order_carrier_id(self, w_id, d_id, o_id, carrier_id):
        args = [carrier_id, w_id, d_id, o_id]
        utils.update(self.session, self.query.update_order_carrier_id, args)

    def select_order_line(self, w_id, d_id, o_id):
        args = [w_id, d_id, o_id]
        return utils.select(self.session, self.query.select_ol, args)

    def update_delivery_d(self, w_id, d_id, o_id, ol_number, t):
        args = [t, w_id, d_id, o_id, ol_number]
        utils.update(self.session, self.query.update_ol_deliver_d, args)

    def sum_order_amount(self, w_id, d_id, o_id):
        args = [w_id, d_id, o_id]
        rows = utils.select(self.session, self.query.select_ol, args)
        amount = 0
        for row in rows:
            amount += row.ol_amount
        return amount

    def update_customer(self, w_id, d_id, c_id, o_id):
        total_order_amount = self.sum_order_amount(self.w_id, d_id, o_id)
        args = [int(100*(round(total_order_amount, 2))), w_id, d_id, c_id]
        utils.update(self.session, self.query.update_customer_delivery_change, args)

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
            return self.update_customer(self.w_id, d_id, smallest_order.o_c_id, o_id)
