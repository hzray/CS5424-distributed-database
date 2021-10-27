from canssandra import cql


class StockLevelHandler:
    def __init__(self, cql_session, query, w_id, d_id, threshold, n_orders):
        self.session = cql_session
        self.query = query
        self.w_id = int(w_id)
        self.d_id = int(d_id)
        self.threshold = int(threshold)
        self.n_orders = int(n_orders)

    def select_district(self, w_id, d_id):
        args = [w_id, d_id]
        return cql.select_one(self.session, self.query.select_district, args)

    def find_items_from_last_l_orders(self, w_id, d_id, bot, top):
        args = [w_id, d_id, bot, top]
        rows = cql.select(self.session, self.query.select_ol_in_range, args)

        count = 0
        for row in rows:
            i_id = row.ol_i_id
            stock = self.select_stock(w_id, i_id)
            if stock.s_quantity < self.threshold:
                count += 1
        return count

    def select_stock(self, w_id, i_id):
        args = [w_id, i_id]
        return cql.select_one(self.session, self.query.select_stock, args)

    def run(self):
        district = self.select_district(self.w_id, self.d_id)
        next_o_id = district.d_next_o_id
        n = self.find_items_from_last_l_orders(self.w_id, self.d_id, next_o_id-self.n_orders, next_o_id)
        print(n)
