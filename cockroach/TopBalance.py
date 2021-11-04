
class TopBalance:
    def __init__(self, conn, fo):
        self.conn = conn
        self.customers = []
        self.w_id = []
        self.d_id = []
        self.warehouse = {}
        self.district = {}
        self.fo = fo

    def topBalance_handler(self):
        self.find_top_ten_customers()
        self.select_warehouse()
        self.select_district()
        # output
        for customer in self.customers:
            w_id = customer[0]
            d_id = customer[1]
            key = str(w_id) + ',' + str(d_id)
            output_str = "\n1. Name of customer: c_first = {}, c_middle = {}, c_last = {}". \
                             format(customer[2], customer[3], customer[4]) + \
                         "\n2. Balance of customer's outstanding payment: c_balance = {}". \
                             format(customer[5]) + \
                         "\n3. Warehouse name of customer: w_name = {}". \
                             format(self.warehouse.get(w_id)) + \
                         "\n4. District name of customer: d_name = {}". \
                             format(self.district.get(key))
        # print(output_str, self.fo)
        self.fo.write(output_str)
        # print(output_str)

    def find_top_ten_customers(self):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute(
                "Select c_w_id, c_d_id, c_first, c_middle, c_last, c_balance, c_id from CS5424.customer order by "
                "c_balance desc limit %s",
                [10])
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            self.customers.append(row)
            self.w_id.append(row[0])
            self.d_id.append(row[1])

    def select_warehouse(self):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute("Select w_id, w_name from CS5424.warehouse WHERE W_ID IN %s", (tuple(self.w_id),))
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            self.warehouse[row[0]] = row[1]

    def select_district(self):
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute("Select d_w_id, d_id, d_name from CS5424.district "
                        "WHERE D_ID IN %s AND D_W_ID IN %s", (tuple(self.d_id), tuple(self.w_id)))
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            key = str(row[0])+','+str(row[1])
            self.district[key] = row[2]
