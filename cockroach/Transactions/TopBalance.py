import time


class TopBalance:
    def __init__(self, conn):
        self.conn = conn
        self.warehouse = {}
        self.district = {}
        self.customers = []

    def topBalance_handler(self):
        start = time.time()
        warehouse_names = self.select_warehouse()
        for name in warehouse_names:
            self.warehouse[name[0]] = name[1]

        district_names = self.select_district()
        for name in district_names:
            key_str = str(name[0]) + ',' + str(name[1])
            self.district[key_str] = name[2]

        self.customers = self.find_top_ten_customers()
        # output
        for customer in self.customers:
            w_id = customer[0]
            d_id = customer[1]
            key = str(w_id) + ',' + str(d_id)

            print("1. Name of customer: c_first = {}, c_middle = {}, c_last = {}".format(customer[2], customer[3], customer[4]))
            print("2. Balance of customer’s outstanding payment: c_balance = {}".format(customer[5]))
            print("3. Warehouse name of customer: w_name = {}".format(self.warehouse.get(w_id)))
            print("4. District name of customer: d_name = {}".format(self.district.get(key)))
        end = time.time()
        latency = start - end
        return latency

    def find_top_ten_customers(self):
        with self.conn.cursor() as cur:
            cur.execute("Select c_w_id, c_d_id, c_first, c_middle, c_last, c_balance from CS5424.customer order by c_balance desc limit %s", [10])
            rows = cur.fetchall()
            self.conn.commit()
            return rows

    def select_warehouse(self):
        with self.conn.cursor() as cur:
            cur.execute("Select w_id, w_name from CS5424.warehouse")
            rows = cur.fetchall()
            self.conn.commit()
            return rows

    def select_district(self):
        with self.conn.cursor() as cur:
            cur.execute("Select d_w_id, d_id, d_name from CS5424.district")
            rows = cur.fetchall()
            self.conn.commit()
            return rows