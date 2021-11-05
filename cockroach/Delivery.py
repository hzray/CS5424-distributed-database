from datetime import datetime
from decimal import Decimal
import time


class Delivery:
    def __init__(self, conn, w_id, carrier_id):
        self.conn = conn
        self.w_id = w_id
        self.carrier_id = carrier_id

        self.x = []
        self.d = []
        self.balance = {}

    def delivery_handler(self):
        start = time.time()
        # step a
        with self.conn.cursor() as cur:
            # cur.execute("SET TRANSACTION AS OF SYSTEM TIME '-0.1s'")
            cur.execute("""SELECT MIN(O_ID), O_D_ID FROM CS5424.orders 
                    WHERE O_W_ID = %s AND O_CARRIER_ID = %s GROUP BY (O_D_ID)""",
                        (self.w_id, '0'))
            rows = cur.fetchall()
        self.conn.commit()

        for row in rows:
            self.x.append(row[0])
            self.d.append(row[1])

        with self.conn.cursor() as cur:
            # step b
            cur.execute("""UPDATE CS5424.orders SET O_CARRIER_ID = %s 
                                WHERE O_W_ID = %s AND O_D_ID IN %s AND O_ID IN %s""",
                        (self.carrier_id, self.w_id, tuple(self.d), tuple(self.x)))
            # step c
            cur.execute("""UPDATE CS5424.order_line SET OL_DELIVERY_D = %s  
                               WHERE OL_W_ID = %s AND OL_D_ID IN %s AND OL_O_ID IN %s""",
                        (datetime.now(), self.w_id, tuple(self.d), tuple(self.x)))
        self.conn.commit()

        with self.conn.cursor() as cur:
            cur.execute("""SELECT SUM(OL_AMOUNT), OL_D_ID FROM CS5424.order_line 
                                WHERE OL_W_ID = %s AND OL_D_ID IN %s AND OL_O_ID IN %s GROUP BY (OL_D_ID, OL_O_ID)""",
                        (self.w_id, tuple(self.d), tuple(self.x)))
            rows = cur.fetchall()
        self.conn.commit()

        for row in rows:
            key = str(row[1])
            val = [Decimal(row[0])]
            self.balance[key] = val

        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT O_C_ID, O_D_ID FROM CS5424.orders WHERE O_W_ID = %s AND O_D_ID IN %s AND O_ID IN %s""",
                (self.w_id, tuple(self.d), tuple(self.x)))
            rows = cur.fetchall()
        self.conn.commit()

        for row in rows:
            key = str(row[1])
            if key in self.balance.keys():
                self.balance[key].append(row[0])

        with self.conn.cursor() as cur:
            for d in self.balance.keys():
                cur.execute("""UPDATE CS5424.customer SET C_BALANCE = C_BALANCE + %s, 
                                C_DELIVERY_CNT = C_DELIVERY_CNT + %s
                                WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s""",
                            (self.balance[d][0], 1, self.w_id, d, self.balance[d][1]))
                #     WHERE C_ID = %s AND C_W_ID = %s AND C_D_ID = %s""",
                # (self.balance[d][0], 1, self.balance[d][1], self.w_id, d))
        self.conn.commit()

        end = time.time()
        latency = start - end
        return latency
