from canssandra import cql


class RelatedCustomerHandler:
    def __init__(self, cql_session, w_id, d_id, c_id):
        self.session = cql_session
        self.w_id = w_id
        self.d_id = d_id
        self.c_id = c_id

    def findAllCustomerItems(self, w_id, d_id, c_id):
        query = "SELECT * FROM CS5424.customer_order_items WHERE col_w_id = %s AND col_d_id = %s AND col_c_id = %s"
        args = [w_id, d_id, c_id]
        orders = cql.select(self.session, query, args)
        items_by_order = []
        for order in orders:
            items_by_order.append(order.col_i_ids)
        return items_by_order

    def findAllCustomers(self, w_id, d_id):
        query = "SELECT * FROM CS5424.customer WHERE c_w_id = %s AND c_d_id = %s"
        args = [w_id, d_id]
        customers = cql.select(self.session, query, args)
        res = []
        for customer in customers:
            res.append(customer)
        return res

    def isRelatedCustomer(self, items_a, items_b):
        for a in items_a:
            for b in items_b:
                if len(list(set(a) & set(b))) >= 2:
                    return True
        return False

    def run(self):
        target_items = self.findAllCustomerItems(self.w_id, self.d_id, self.c_id)
        for i in range(1, 11):
            if i == self.w_id:
                continue
            for j in range(1, 11):
                customers = self.findAllCustomers(i, j)
                for customer in customers:
                    customer_items = self.findAllCustomerItems(self.w_id, self.d_id, customer.c_id)
                    if self.isRelatedCustomer(target_items, customer_items):
                        print("W_id = {}, D_ID = {}, C_ID={}".format(customer.c_w_id, customer.c_d_id, customer.c_id))




