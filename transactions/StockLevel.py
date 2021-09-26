import cql


class StockLevelHandler:
    def __init__(self, cql_session, w_id, d_id, threshold, n_orders):
        self.session = cql_session
        self.w_id = w_id
        self.d_id = d_id
        self.threshold = threshold
        self.n_orders = n_orders

    def select_district(self, w_id, d_id):
        query = "SELECT * FROM CS5424.district WHERE d_w_id=%s AND d_id=%s"
        args = [w_id, d_id]
        return cql.select_one(self.session, query, args)

    def find_items_from_last_l_orders(self, w_id, d_id, bot, top):
        query = "SELECT * FROM CS5424.order_line where ol_w_id = %s and ol_d_id = %s and ol_o_id >= %s and ol_o_id < %s"
        args = [w_id, d_id, bot, top]
        rows = cql.select(self.session, query, args)

        count = 0
        for row in rows:
            i_id = row.ol_i_id
            stock = self.select_stock(w_id, i_id)
            if stock.s_quantity < self.threshold:
                count += 1
        return count

    def select_stock(self, w_id, i_id):
        query = "SELECT * FROM CS5424.stock where s_w_id = %s and s_i_id = %s"
        args = [w_id, i_id]
        return cql.select_one(self.session, query, args)

    def run(self):
        district = self.select_district(self.w_id, self.d_id)
        next_o_id = district.d_next_o_id
        n = self.find_items_from_last_l_orders(self.w_id, self.d_id, next_o_id-self.n_orders, next_o_id)
        print(n)
