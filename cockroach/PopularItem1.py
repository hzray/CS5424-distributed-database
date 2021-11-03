class PopularItem:
    def __init__(self, conn, w_id, d_id, l, fo):
        self.conn = conn
        self.w_id = int(w_id)
        self.d_id = int(d_id)
        self.l = int(l)
        self.fo = fo

        self.d_next_o_id = 0
        self.output_str = ""

    def select_district(self):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute(
                "Select d_next_o_id from CS5424.district where d_w_id = %s and d_id = %s", [self.w_id, self.d_id])
            rows = cur.fetchall()
            self.conn.commit()
            for row in rows:
                d_next_o_id = row[0]
            return d_next_o_id

    def select_orders(self, o_id_list):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute(
                "Select o_id, o_c_id, o_entry_d from CS5424.orders where o_w_id = %s and o_d_id = %s and o_id in %s",
                [self.w_id, self.d_id, o_id_list])
            rows = cur.fetchall()
            self.conn.commit()
            return rows

    def select_customer(self, c_id):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute(
                "Select c_first, c_middle, c_last, c_balance from CS5424.customer "
                "where c_w_id = %s and c_d_id = %s and c_id = %s",
                [self.w_id, self.d_id, c_id])
            rows = cur.fetchall()
            self.conn.commit()
            for row in rows:
                customer = row
            return customer

    def get_max_quantity(self, o_id):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute(
                "Select ol_quantity from CS5424.order_line where ol_w_id = %s and ol_d_id = %s and ol_o_id = %s "
                "order by ol_quantity desc limit 1",
                [self.w_id, self.d_id, o_id])
            rows = cur.fetchall()
            self.conn.commit()
            return rows

    def select_order_lines(self, o_id):
        ol_quantity = self.get_max_quantity(o_id)
        rows = []
        if len(ol_quantity) > 0:
            with self.conn.cursor() as cur:
                cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
                cur.execute(
                    "Select ol_i_id, ol_quantity from CS5424.order_line where ol_w_id = %s and ol_d_id = %s "
                    "and ol_o_id = %s and ol_quantity = %s",
                    [self.w_id, self.d_id, o_id, ol_quantity[0]])
                rows = cur.fetchall()
            self.conn.commit()
        return rows

    def select_item(self, item_id):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute(
                "Select i_name from CS5424.item where i_id = %s", [item_id])
            rows = cur.fetchall()
            item = rows[0]
            self.conn.commit()
            return item

    def popularItem_handler(self):
        self.output_str += "\n1. District identifier: w_id = {}, d_id = {}".format(self.w_id, self.d_id) + \
                           "\n2. Number of last orders to be examined: l = {}".format(self.l)

        # step 1
        self.d_next_o_id = self.select_district()
        # print("Debug: d_next_o_id: ", self.d_next_o_id)

        # step 2
        o_id_list = []
        for i in range(self.d_next_o_id - self.l, self.d_next_o_id):
            o_id_list.append(i)
        o_id_list = tuple(o_id_list)
        orders = self.select_orders(o_id_list)

        # step 3
        self.output_str += "\n3. Orders Information: "
        items = {}
        for order in orders:
            self.output_str += "\nOrder number: o_id = {}, entry date and time: o_entry_d = {}".\
                format(order[0], order[2])
            customer = self.select_customer(order[1])
            self.output_str += "\nName of customer: c_first = {}, c_middle = {}, c_last = {}".\
                format(customer[0], customer[1], customer[2])

            order_lines = self.select_order_lines(order[0])

            for order_line in order_lines:
                item_id = order_line[0]
                item = self.select_item(item_id)
                item_name = item[0]
                item_count = items.get(item_name) if item_name in items else 0
                items.update({item_name: item_count + 1})
                self.output_str += "\nItem name: i_name = {}, Quantity order: ol_quantity = {}".\
                    format(item_name, order_line[1])

        distinct_items = set(items.keys())
        self.output_str += "\n4. The percentage of examined orders:"
        for item in distinct_items:
            percentage = float(items.get(item)) / (float(1) * 100.0)
            self.output_str += "Item name: i_name = {}, the percentage = {}".format(item, percentage)
        # print(self.output_str, self.fo)
        self.fo.write(self.output_str)
        # print(self.output_str)
