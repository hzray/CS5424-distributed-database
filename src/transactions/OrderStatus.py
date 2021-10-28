from datetime import datetime

from src import cql


class OrderStatusHandler:
    def __init__(self, cql_session, query, w_id, d_id, c_id):
        self.session = cql_session
        self.query = query
        self.w_id = int(w_id)
        self.d_id = int(d_id)
        self.c_id = int(c_id)

    def select_customer(self, w_id, d_id, c_id):
        args = [w_id, d_id, c_id]
        return cql.select_one(self.session, self.query.select_customer, args)

    def find_customer_last_order(self, w_id, d_id, c_id):
        args = [w_id, d_id, c_id]
        return cql.select_one(self.session, self.query.select_order_with_customer, args)

    def select_order_line(self, w_id, d_id, o_id):
        args = [w_id, d_id, o_id]
        return cql.select(self.session, self.query.select_ol, args)

    def run(self):
        # Step 1
        customer = self.select_customer(self.w_id, self.d_id, self.c_id)
        customer_output = "C_FIRST = {}, C_MIDDLE = {}, C_LAST = {}, C_BALANCE = {}".format(
            customer.c_first, customer.c_middle, customer.c_last, customer.c_balance
        )
        print(customer_output)

        # Step 2
        last_order = self.find_customer_last_order(self.w_id, self.d_id, self.c_id)
        order_output = "O_ID = {}, O_ENTRY_D = {}, O_CARRIER_ID = {}".format(
            last_order.o_id, last_order.o_entry_d, last_order.o_carrier_id
        )
        print(order_output)

        # Step 3
        order_lines = self.select_order_line(self.w_id, self.d_id, last_order.o_id)
        for order_line in order_lines:

            delivery_date = order_line.ol_delivery_d
            if delivery_date == datetime(1970, 1, 1, 0, 0):
                delivery_date = "has not been delivered"

            order_line_output = "OL_I_ID = {}, OL_SUPPLY_W_ID = {}, OL_QUANTITY = {}, OL_AMOUNT = {}, " \
                                "OL_DELIVERY_D = {}".format(order_line.ol_i_id, order_line.ol_supply_w_id,
                                                            order_line.ol_quantity, order_line.ol_amount,
                                                            delivery_date)
            print(order_line_output)
