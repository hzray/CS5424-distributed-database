
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
            items = self.find_items(order[0])
            related_customers = self.find_related_customers(items)

            # output:
            for customer in related_customers:
                self.output_str += "\nCustomer Identifier: w_id = {}, d_id = {}, c_id = {}".\
                    format(customer[0], customer[1], customer[2])
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

    def find_related_customers(self, items):
        related_customers = []
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute(
                "Select ol_w_id, ol_d_id, ol_o_id, COUNT(*) as Count from CS5424.order_line "
                "where ol_w_id != %s and ol_i_id in %s group by (ol_w_id, ol_d_id, ol_o_id)",
                [self.c_w_id, tuple(items)])
            rows = cur.fetchall()
            self.conn.commit()

            for row in rows:
                if row[3] >= 2:
                    c_id = self.find_customer(row[0], row[1], row[2])
                    customer = (row[0], row[1], c_id)
                    related_customers.append(customer)
        return set(related_customers)

    def find_customer(self, warehouse_id, district_id, order_id):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute(
                "Select o_c_id from CS5424.orders where o_w_id = %s and o_d_id = %s and o_id = %s",
                [warehouse_id, district_id, order_id])
            rows = cur.fetchall()
            self.conn.commit()

            for row in rows:
                c_id = row

            return c_id
