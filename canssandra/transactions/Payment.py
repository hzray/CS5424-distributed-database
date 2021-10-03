from canssandra import cql


class PaymentHandler:
    def __init__(self, cql_session, w_id, d_id, c_id, payment):
        self.session = cql_session
        self.w_id = w_id
        self.d_id = d_id
        self.c_id = c_id
        self.payment = payment

    def select_warehouse(self, w_id):
        query = "SELECT * FROM CS5424.warehouse where w_id = %s"
        args = [w_id]
        return cql.select_one(self.session, query, args)

    def update_warehouse_ytd(self, w_id, ytd):
        query = "UPDATE CS5424.warehouse SET w_ytd = %s where w_id = %s"
        args = [ytd, w_id]
        cql.update(self.session, query, args)

    def select_district(self, w_id, d_id):
        query = "SELECT * FROM CS5424.district WHERE d_w_id=%s AND d_id=%s"
        args = [w_id, d_id]
        return cql.select_one(self.session, query, args)

    def update_district_ytd(self, w_id, d_id, ytd):
        query = "UPDATE CS5424.district SET d_ytd = %s where d_w_id = %s AND d_id = %s"
        args = [ytd, w_id, d_id]
        cql.update(self.session, query, args)

    def select_customer(self, w_id, d_id, c_id):
        query = "SELECT * FROM CS5424.customer WHERE c_w_id = %s and c_d_id = %s and c_id = %s"
        args = [w_id, d_id, c_id]
        return cql.select_one(self.session, query, args)

    def update_customer(self, w_id, d_id, c_id, balance, ytd_payment, payment_cnt):
        query = "UPDATE CS5424.customer SET c_balance = %s, c_ytd_payment = %s, c_payment_cnt = %s " \
                "where c_w_id = %s AND c_d_id = %s AND c_id = %s"
        args = [balance, ytd_payment, payment_cnt, w_id, d_id, c_id]
        cql.update(self.session, query, args)

    def run(self):
        # Step 1
        warehouse = self.select_warehouse(self.w_id)
        self.update_warehouse_ytd(self.w_id, warehouse.w_ytd + self.payment)

        # Step 2
        district = self.select_district(self.w_id, self.d_id)
        self.update_district_ytd(self.w_id, self.d_id, district.d_ytd + self.payment)

        # Step 3
        customer = self.select_customer(self.w_id, self.d_id, self.c_id)
        self.update_customer(self.w_id, self.d_id, self.c_id, customer.c_balance-self.payment,
                             customer.c_ytd_payment+self.payment, customer.c_payment_cnt+1)

        # Output
        customer_output = "customer: W_ID = {}, D_ID = {}, C_ID = {}, C_FIRST = {}, C_MIDDLE = {}, C_LAST = {}, " \
                          "C_STREET_1 = {}, C_STREET_2 = {}, C_CITY = {}, C_STATE = {}, C_ZIP = {}, C_PHONE, " \
                          "C_SINCE = {}, C_CREDIT = {}, C_CREDIT_LIM = {}, C_DISCOUNT = {}, C_BALANCE = {}" \
            .format(customer.c_w_id, customer.c_d_id, customer.c_id, customer.c_first, customer.c_middle, customer.c_last,
                    customer.c_street_1, customer.c_street_2, customer.c_city, customer.c_state, customer.c_zip,
                    customer.c_phone, customer.c_since, customer.c_credit, customer.c_credit_lim,
                    customer.c_discount, customer.c_balance)

        print(customer_output)

        warehouse_address = "W_STREET_1 = {}, W_STREET_2 = {}, W_CITY = {}, W_STATE = {}, W_ZIP = {}".format(
            warehouse.w_street_1, warehouse.w_street_2, warehouse.w_city, warehouse.w_state, warehouse.w_zip
        )

        print(warehouse_address)

        district_address = "D_STREET_1 = {}, D_STREET_2 = {}, W_CITY = {}, W_STATE = {}, W_ZIP = {}".format(
            district.d_street_1, district.d_street_2, district.d_city, district.d_state, district.d_zip
        )

        print(district_address)

        print("payment = " + self.payment)




