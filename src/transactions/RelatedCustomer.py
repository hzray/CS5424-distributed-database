from transactions.cql import utils
from collections import Counter
from collections import defaultdict


def flatten(t):
    return [item for sublist in t for item in sublist]

class RelatedCustomerHandler:
    def __init__(self, cql_session, query, workload, w_id, d_id, c_id):
        self.session = cql_session
        self.query = query
        self.workload = workload
        self.w_id = int(w_id)
        self.d_id = int(d_id)
        self.c_id = int(c_id)

    def helper(self, r_w_id, r_d_id, items):
        args = [r_w_id, r_d_id, items]
        related_order_line = list(utils.select(self.session, self.query.select_customer_order_items_in, args))
        o_ids = [row.coi_o_id for row in related_order_line]
        counter = Counter(o_ids)
        related_orders = [c for c in counter if counter[c] >= 2]

        c_ids = []
        for ol in related_order_line:
            if ol.coi_o_id in related_orders:
                c_ids.append(ol.coi_c_id)
        return c_ids

    def find_related_customers_A(self, w_id, d_id, c_id):
        # find customer's order items in list
        args = [w_id, d_id, c_id]
        customer_order_items = utils.select(self.session, self.query.select_customer_order_items, args)
        a = defaultdict(list)
        for row in customer_order_items:
            a[row.coi_o_id].append(row.coi_i_id)

        # list of list of items
        items_list = list(list(a.values()))
        for w in range(1, 11):
            if w == w_id:
                continue
            for d in range(1, 11):
                ids = []
                for items in items_list:
                    ids.append(self.helper(w, d, items))
                ids = flatten(ids)
                ids = set(ids)
                for c_id in ids:
                    print("W_ID = {}, D_ID = {}, C_ID = {}".format(w, d, c_id))

    def find_related_customers_B(self, w_id, d_id, c_id):
        args = [w_id, d_id, c_id]
        customers = utils.select(self.session, self.query.select_related_customer, args)
        for rc in customers:
            print("W_ID = {}, D_ID = {}, C_ID = {}".format(rc.r_w_id, rc.r_d_id, rc.r_c_id))

    def run(self):
        if self.workload == 'A':
            self.find_related_customers_A(self.w_id, self.d_id, self.c_id)
        elif self.workload == 'B':
            self.find_related_customers_B(self.w_id, self.d_id, self.c_id)



