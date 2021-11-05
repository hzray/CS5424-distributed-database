
class RelatedCustomer:
    def __init__(self, conn, c_w_id, c_d_id, c_id, fo):
        self.conn = conn
        self.c_w_id = int(c_w_id)
        self.c_d_id = int(c_d_id)
        self.c_id = int(c_id)
        self.fo = fo

        self.output_str = ""

    def relatedCustomer_handler(self):
        self.output_str += "\n1. Customer identifier: c_w_id = {}, c_d_id = {}, c_id = {}". \
            format(self.c_w_id, self.c_d_id, self.c_id)
        orders = self.find_orders()
        self.output_str += "\n2. For each customer: "
        for order in orders:
            self.find_items(order[0])
            self.find_related_customers()

        # print(self.output_str, self.fo)
        self.fo.write(self.output_str)
        # print(self.output_str)

    def find_orders(self):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute(
                "Select o_id from CS5424.orders where o_w_id = %s and o_d_id = %s and o_c_id = %s",
                [self.c_w_id, self.c_d_id, self.c_id])
            rows = cur.fetchall()
            self.conn.commit()
            return rows

    def find_items(self, o_id):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute(
                "Select ol_i_id, ol_o_id from CS5424.order_line where ol_w_id = %s and ol_d_id = %s and ol_o_id = %s",
                [self.c_w_id, self.c_d_id, o_id])
            rows = cur.fetchall()
            self.conn.commit()
            items = []
            for row in rows:
                items.append(row[0])
            return items

    def find_related_customers(self):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute(
                "Select r_w_id, r_d_id, r_c_id from CS5424.related_customer "
                "where w_id = %s and d_id = %s and c_id = %s",
                (self.c_w_id, self.c_d_id, self.c_id))
            rows = cur.fetchall()
        self.conn.commit()

        for row in rows:
            self.output_str += "\nCustomer Identifier: w_id = {}, d_id = {}, c_id = {}". \
                format(row[0], row[1], row[2])

