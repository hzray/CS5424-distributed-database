from canssandra import cql
from collections import Counter


class RelatedCustomerHandler:
    def __init__(self, cql_session, query, w_id, d_id, c_id):
        self.session = cql_session
        self.query = query
        self.w_id = int(w_id)
        self.d_id = int(d_id)
        self.c_id = int(c_id)

    def find_customer_orders(self, w_id, d_id, c_id):
        args = [w_id, d_id, c_id]
        return cql.select(self.session, self.query.select_customer_order, args)

    def helper(self, r_w_id, r_d_id, items):
        args = [r_w_id, r_d_id, items]
        related_order_line = list(cql.select(self.session, self.query.select_customer_order_items, args))
        o_ids = [row.coi_o_id for row in related_order_line]
        counter = Counter(o_ids)
        related_orders = [c for c in counter if counter[c] >= 2]

        c_ids = []
        for ol in related_order_line:
            if ol.coi_o_id in related_orders and ol.coi_c_id not in c_ids:
                c_ids.append(ol.coi_c_id)
                print("W_ID={}, D_ID={}, C_ID={}".format(r_w_id, r_d_id, ol.coi_c_id))

    def find_related_customers(self, w_id, d_id, c_id):
        # find customer's orders
        customer_orders = self.find_customer_orders(w_id, d_id, c_id)
        # list of list of items
        items_list = [row.co_i_ids for row in customer_orders]
        for items in items_list:
            for w in range(1, 11):
                if w == w_id:
                    continue
                for d in range(1, 11):
                    self.helper(w, d, items)

    def run(self):
        self.find_related_customers(self.w_id, self.d_id, self.c_id)

    # def slow_run(self):
    #     args = [self.w_id, self.d_id, self.c_id]
    #     customer_orders = cql.select(self.session, self.query.select_customer_order, args)
    #     items_list = [row.co_i_ids for row in customer_orders]
    #     for w in range(1, 11):
    #         if w == self.w_id:
    #             continue
    #         for d in range(1, 11):
    #             query = "SELECT * FROM CS5424.customer_order where co_w_id = %s and co_d_id = %s"
    #             args = [w, d]
    #             orders = cql.select(self.session, query, args)
    #             for o in orders:
    #                 if self.isOverlap(items_list, o.co_i_ids):
    #                     print("W_ID = {}, D_ID = {}, C_ID = {}".format(w, d, o.co_c_id))
    #
    # def isOverlap(self, item_list, target):
    #     for item in item_list:
    #         if len(set(item).intersection(set(target))) >= 2:
    #             return True
    #     return False
