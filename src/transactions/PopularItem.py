from src import cql


class PopularItemHandler:
    def __init__(self, cql_session, query, w_id, d_id, n):
        self.session = cql_session
        self.query = query
        self.w_id = int(w_id)
        self.d_id = int(d_id)
        self.n = int(n)

    def select_district(self, w_id, d_id):
        args = [w_id, d_id]
        return cql.select_one(self.session, self.query.select_district, args)

    def find_last_n_orders(self, w_id, d_id, bot, top):
        args = [w_id, d_id, bot, top]
        return list(cql.select(self.session, self.query.select_order_in_range, args))

    def select_order_lines(self, w_id, d_id, o_id):
        args = [w_id, d_id, o_id]
        return list(cql.select(self.session, self.query.select_ol, args))

    def select_customer(self, w_id, d_id, c_id):
        args = [w_id, d_id, c_id]
        return cql.select_one(self.session, self.query.select_customer, args)

    def select_item(self, i_id):
        args = [i_id]
        return cql.select_one(self.session, self.query.select_item, args)

    def run(self):
        district = self.select_district(self.w_id, self.d_id)
        next_o_id = district.d_next_o_id
        orders = self.find_last_n_orders(self.w_id, self.d_id, next_o_id - self.n, next_o_id)

        print("next_o_id = {}".format(next_o_id))

        pop_order_lines = []
        order_lines_list = []
        for order in orders:
            order_lines = self.select_order_lines(self.w_id, self.d_id, order.o_id)
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
        print("L = {}".format(self.n))

        pop_item_names = []
        pop_items = []

        i = 0
        for order in orders:
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
                print("I_NAME = {}, OL_QUANTITY = {}".format(item.i_name, pop_ol.ol_quantity))
            i += 1

        for (i_id, i_name) in pop_items:
            count = 0
            for order_lines in order_lines_list:
                for order_line in order_lines:
                    if order_line.ol_i_id == i_id:
                        count += 1
            print("I_NAME = {}, Percentage = {}".format(i_name, count / self.n))
