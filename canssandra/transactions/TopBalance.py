from canssandra import cql


class TopBalanceHandler:
    def __init__(self, cql_session):
        self.session = cql_session

    def find_top_ten_customers(self):
        query = "SELECT * FROM CS5424.customer_balance LIMIT %s"
        args = [10]
        return cql.select(query, args)

    def select_warehouse(self, w_id):
        query = "SELECT * FROM CS5424.warehouse where w_id = %s"
        args = [w_id]
        return cql.select_one(self.session, query, args)

    def select_district(self, w_id, d_id):
        query = "SELECT * FROM CS5424.district WHERE d_w_id=%s AND d_id=%s"
        args = [w_id, d_id]
        return cql.select_one(self.session, query, args)

    def run(self):
        customers = self.find_top_ten_customers()
        for customer in customers:
            print("C_FIRST = {}, C_MIDDLE = {}, C_LAST = {}".format(customer.c_first, customer.c_middle,
                                                                    customer.c_last))
            print("C_BALANCE = " + customer.c_balance)

            warehouse = self.select_warehouse(customer.c_w_id)
            district = self.select_district(customer.c_w_id, customer.c_d_id)

            print("W_NAME = {}, D_NAME = {}".format(warehouse.w_name, district.d_name))
