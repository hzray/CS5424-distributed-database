import cql


class OrderStatusHandler:
    def __init__(self, cql_session, w_id, d_id, c_id):
        self.session = cql_session
        self.w_id = w_id
        self.d_id = d_id
        self.c_id = c_id

    def select_customer(self, w_id, d_id, c_id):
        query = "SELECT * FROM CS5424.customer WHERE c_w_id = %s and c_d_id = %s and c_id = %s"
        args = [w_id, d_id, c_id]
        return cql.select_one(self.session, query, args)

    def find_customer_last_order(self, w_id, d_id, c_id):
        query = "SELECT * FROM CS5424.orders_customer WHERE o_w_id = %s AND o_d_id = %s AND o_c_id = %s"
        args = [w_id, d_id, c_id]
        return cql.select_one(self.session, query, args)

    def select_order_line(self, w_id, d_id, o_id):
        query = "SELECT * FROM CS5424.order_line WHERE ol_w_id = %s and ol_d_id = %s and ol_o_id = %s"
        args = [w_id, d_id, o_id]
        return cql.select(self.session, query, args)

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
            order_line_output = "OL_I_ID = {}, OL_SUPPY_W_ID = {}, OL_QUANTITY = {}, OL_AMOUNT = {}, " \
                                "OL_DELIVERY_D = {}".format(order_line.ol_i_id, order_line.ol_supply_w_id,
                                                            order_line.ol_quantity, order_line.ol_amount,
                                                            order_line.ol_delivery_d)
            print(order_line_output)


