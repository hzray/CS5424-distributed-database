import decimal

from transactions.cql import utils


class PaymentHandler:
    def __init__(self, cql_session, query, w_id, d_id, c_id, payment):
        self.session = cql_session
        self.query = query
        self.w_id = int(w_id)
        self.d_id = int(d_id)
        self.c_id = int(c_id)
        self.payment = decimal.Decimal(payment)

    def select_warehouse(self, w_id):
        args = [w_id]
        return utils.select_one(self.session, self.query.select_warehouse, args)

    def update_warehouse_ytd_change(self, w_id, payment):
        args = [payment, w_id]
        utils.update(self.session, self.query.update_warehouse_ytd_change, args)

    def select_district(self, w_id, d_id):
        args = [w_id, d_id]
        return utils.select_one(self.session, self.query.select_district, args)

    def update_district_ytd_change(self, w_id, d_id, payment):
        args = [payment, w_id, d_id]
        utils.update(self.session, self.query.update_district_ytd_change, args)

    def select_customer(self, w_id, d_id, c_id):
        args = [w_id, d_id, c_id]
        return utils.select_one(self.session, self.query.select_customer, args)

    def update_customer_counters(self, w_id, d_id, c_id, payment):
        args = [payment, payment, w_id, d_id, c_id]
        utils.update(self.session, self.query.update_customer_payment_counters, args)

    def run(self):
        # Step 1
        warehouse = self.select_warehouse(self.w_id)
        self.update_warehouse_ytd_change(self.w_id, self.payment)

        # Step 2
        district = self.select_district(self.w_id, self.d_id)
        self.update_district_ytd_change(self.w_id, self.d_id, self.payment)

        # Step 3
        customer = self.select_customer(self.w_id, self.d_id, self.c_id)
        self.update_customer_counters(self.w_id, self.d_id, self.c_id, self.payment)

        # Output
        customer_output = "customer: W_ID = {}, D_ID = {}, C_ID = {}, C_FIRST = {}, C_MIDDLE = {}, C_LAST = {}, " \
                          "C_STREET_1 = {}, C_STREET_2 = {}, C_CITY = {}, C_STATE = {}, C_ZIP = {}, C_PHONE, " \
                          "C_SINCE = {}, C_CREDIT = {}, C_CREDIT_LIM = {}, C_DISCOUNT = {}, C_BALANCE = {}" \
            .format(customer.c_w_id, customer.c_d_id, customer.c_id, customer.c_first, customer.c_middle,
                    customer.c_last,
                    customer.c_street_1, customer.c_street_2, customer.c_city, customer.c_state, customer.c_zip,
                    customer.c_phone, customer.c_since, customer.c_credit, customer.c_credit_lim,
                    customer.c_discount, customer.c_balance-self.payment)

        print(customer_output)

        warehouse_address = "W_STREET_1 = {}, W_STREET_2 = {}, W_CITY = {}, W_STATE = {}, W_ZIP = {}".format(
            warehouse.w_street_1, warehouse.w_street_2, warehouse.w_city, warehouse.w_state, warehouse.w_zip
        )

        print(warehouse_address)

        district_address = "D_STREET_1 = {}, D_STREET_2 = {}, W_CITY = {}, W_STATE = {}, W_ZIP = {}".format(
            district.d_street_1, district.d_street_2, district.d_city, district.d_state, district.d_zip
        )

        print(district_address)

        print("payment = {}".format(self.payment))
