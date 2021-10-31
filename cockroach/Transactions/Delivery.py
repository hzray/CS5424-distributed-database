from datetime import datetime
import time


class Delivery:
    def __init__(self, conn, w_id, carrier_id):
        self.conn = conn
        self.w_id = w_id
        self.carrier_id = carrier_id

        self.c = 0
        self.b = 0
        self.x = 0

    def delivery_handler(self):
        start = time.time()
        for district_no in range(1, 10):
            # step a
            with self.conn.cursor() as cur:
                cur.execute("""SELECT MIN(O_ID) FROM CS5424.orders 
                        WHERE O_W_ID = %s AND O_D_ID = %s AND O_CARRIER_ID = %s""",
                            (self.w_id, district_no, '0'))
                rows = cur.fetchall()
            self.conn.commit()
            for row in rows:
                self.x = row[0]
                break

            with self.conn.cursor() as cur:
                # step b
                cur.execute("""UPDATE CS5424.orders SET O_CARRIER_ID = %s WHERE O_ID = %s
                AND O_W_ID = %s AND O_D_ID = %s""",
                            (self.carrier_id, self.x, self.w_id, district_no))
                # step c
                cur.execute("""UPDATE CS5424.order_line SET OL_DELIVERY_D = %s  WHERE OL_O_ID = %s
                AND OL_W_ID = %s AND OL_D_ID = %s""",
                            (datetime.now(), self.x, self.w_id, district_no))

            self.conn.commit()

            # step d
            with self.conn.cursor() as cur:
                cur.execute("""SELECT O_C_ID FROM CS5424.orders WHERE O_ID = %s AND O_W_ID = %s AND O_D_ID = %s""",
                            (self.x, self.w_id, district_no))
                rows = cur.fetchall()
            self.conn.commit()
            for row in rows:
                self.c = row[0]
                break
            with self.conn.cursor() as cur:
                cur.execute("""SELECT SUM(OL_AMOUNT) FROM CS5424.order_line WHERE OL_O_ID = %s
                                AND OL_W_ID = %s AND OL_D_ID = %s""",
                            (self.x, self.w_id, district_no))
                rows = cur.fetchall()
            self.conn.commit()
            for row in rows:
                self.b = row[0]
            with self.conn.cursor() as cur:
                cur.execute("""UPDATE CS5424.customer SET C_BALANCE = C_BALANCE + %s, 
                                C_DELIVERY_CNT = C_DELIVERY_CNT + %s
                                WHERE C_ID = %s AND C_W_ID = %s AND C_D_ID = %s""",
                            (self.b, 1, self.c, self.w_id, district_no))
            self.conn.commit()
        end = time.time()
        latency = start - end
        return latency
