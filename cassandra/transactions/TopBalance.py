from cassandra import cql


def flatten(t):
    return [item for sublist in t for item in sublist]


def takeBalance(customer):
    return customer.c_balance


class TopBalanceHandler:
    def __init__(self, cql_session, query):
        self.session = cql_session
        self.query = query

    def find_top_ten_customers(self):
        customers = []
        for w_id in range(1, 11):
            for d_id in range(1, 11):
                args = [w_id, d_id, 10]
                customers.append(list(cql.select(self.session, self.query.select_customer_sort_by_balance, args)))
        customers = flatten(customers)
        customers.sort(key=takeBalance, reverse=True)
        return customers[:10]

    def select_warehouse(self, w_id):
        args = [w_id]
        return cql.select_one(self.session, self.query.select_warehouse, args)

    def select_district(self, w_id, d_id):
        args = [w_id, d_id]
        return cql.select_one(self.session, self.query.select_district, args)

    def run(self):
        customers = self.find_top_ten_customers()
        for customer in customers:
            print("C_FIRST = {}, C_MIDDLE = {}, C_LAST = {}".format(customer.c_first, customer.c_middle,
                                                                    customer.c_last))
            print("C_BALANCE = {}".format(customer.c_balance))

            warehouse = self.select_warehouse(customer.c_w_id)
            district = self.select_district(customer.c_w_id, customer.c_d_id)

            print("W_NAME = {}, D_NAME = {}".format(warehouse.w_name, district.d_name))
