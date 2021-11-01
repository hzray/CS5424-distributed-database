import time
import psycopg2
import random

class StockLevel:
    def __init__(self, conn, w_id, d_id, t, l, fo):
        self.conn = conn
        self.w_id = w_id
        self.d_id = d_id
        self.t = t
        self.l = l
        self.fo = fo

        self.n = 0
        self.s = []
        self.number = 0

    def stock_level_handler(self):
        # step 1
        with self.conn.cursor() as cur:
            cur.execute("""SELECT D_NEXT_O_ID FROM CS5424.district WHERE D_W_ID = %s AND D_ID = %s""",
                        (self.w_id, self.d_id))
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            self.n = row[0]
            break

        # step 2
        nl = int(self.n) - int(self.l)
        with self.conn.cursor() as cur:
            cur.execute("""SELECT OL_I_ID FROM CS5424.order_line WHERE OL_D_ID = %s AND OL_W_ID = %s 
                            AND OL_O_ID >= %s AND OL_O_ID < %s""",
                        (self.d_id, self.w_id, str(nl), self.n))
            rows = cur.fetchall()
        self.conn.commit()
        for row in rows:
            self.s.append(row)
        # step 3
        with self.conn.cursor() as cur:
            cur.execute("""SELECT COUNT(S_QUANTITY) FROM CS5424.stock WHERE S_W_ID = %s
                            AND S_I_ID IN %s AND S_QUANTITY < %s""",
                        (self.w_id, tuple(self.s), self.t))
            # cur.execute("""SELECT COUNT(S_QUANTITY) FROM CS5424.stock WHERE S_W_ID = %s
            #                             AND S_I_ID IN %s""",
            #             (self.w_id, tuple(self.s)))
            rows = cur.fetchall()
        for row in rows:
            self.number = row[0]
            break
        self.conn.commit()

        print("the total number of items: {}".format(self.number), file=self.fo)

