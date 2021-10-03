from canssandra import cql


class PopularItemHandler:
    def __init__(self, cql_session, w_id, d_id, n):
        self.session = cql_session
        self.w_id = w_id
        self.d_id = d_id
        self.n = n

    def select_district(self, w_id, d_id):
        query = "SELECT * FROM CS5424.district WHERE d_w_id=%s AND d_id=%s"
        args = [w_id, d_id]
        return cql.select_one(self.session, query, args)

    def find_last_n_orders(self, w_id, d_id, bot, top):
        query = "SELECT * FROM CS5424.orders WHERE o_w_id = %s AND o_d_id = %s AND o_id >= %s AND ol_id < %s"
        args = [w_id, d_id, bot, top]
        return cql.select(self.session, query, args)

    def select_order_line(self, w_id, d_id, o_id):
        query = "SELECT * FROM CS5424.order_line where ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s"
        args = [w_id, d_id, o_id]
        return cql.select(self.session, query, args)

    def select_customer(self, w_id, d_id, c_id):
        query = "SELECT * FROM CS5424.customer WHERE c_w_id = %s and c_d_id = %s and c_id = %s"
        args = [w_id, d_id, c_id]
        return cql.select_one(self.session, query, args)

    def select_item(self, i_id):
        query = "SELECT * FROM CS5424.item WHERE i_id=%s"
        args = [i_id]
        return cql.select_one(self.session, query, args)

    def run(self):
        district = self.select_district(self.w_id, self.d_id)
        next_o_id = district.d_next_o_id
        orders = self.find_last_n_orders(self.w_id, self.d_id, next_o_id - self.n, next_o_id)

        pop_order_lines = []
        order_lines_list = []
        for order in orders:
            order_lines = self.select_order_line(self.w_id, self.d_id, order.o_id)
            order_lines_list.append(order_lines)
            max_amount = 0
            for order_line in order_lines:
                if order_line.ol_quantity >= max_amount:
                    max_amount = order_line.ol_quantity

            sub_pop_order_lines = []
            for order_line in order_lines:
                if order_line.ol_quantity == max_amount:
                    sub_pop_order_lines.append(order_line)

            pop_order_lines.append(sub_pop_order_lines)

        # output
        print("W_ID = {}, D_ID = {}".format(self.w_id, self.d_id))
        print("L = " + self.n)

        pop_item_names = []
        pop_items = []
        for i in range(0, len(orders)):
            order = orders[i]
            print("O_ID = {}, O_ENTRY_D = {}".format(order.o_id, order.o_entry_d))
            customer = self.select_customer(self.w_id, self.d_id, order.o_c_id)
            print(
                "C_FIRST = {}, C_MIDDLE = {}, C_LAST = {}".format(customer.c_first, customer.c_middle, customer.c_last))
            sub_pop = pop_order_lines[i]
            for pop_ol in sub_pop:
                i_id = pop_ol.ol_i_id
                item = self.select_item(i_id)
                if item.i_name not in pop_item_names:
                    pop_item_names.append(item.i_name)
                    pop_items.append((item.i_id, item.i_name))
                print("I_NAME = {}, OL_QUANTITY = {}", item.i_name, pop_ol.ol_quantity)

        for (i_id, i_name) in pop_items:
            count = 0
            for order_lines in order_lines_list:
                for order_line in order_lines:
                    if order_line.ol_i_id == i_id:
                        count += 1
            print("I_NAME = {}, Percentage = {}".format(i_name, count/self.n))
