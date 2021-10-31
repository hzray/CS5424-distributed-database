from collections import Counter
from transactions.cql import utils

def flatten(t):
    return [item for sublist in t for item in sublist]


def find_customer_orders(session, query, w_id, d_id, c_id):
    args = [w_id, d_id, c_id]
    return utils.select(session, query.select_customer_order, args)


def helper(session, query, r_w_id, r_d_id, items):
    args = [r_w_id, r_d_id, items]
    related_order_line = list(utils.select(session, query.select_customer_order_items, args))
    o_ids = [row.coi_o_id for row in related_order_line]
    counter = Counter(o_ids)
    related_orders = [c for c in counter if counter[c] >= 2]

    c_ids = []
    for ol in related_order_line:
        if ol.coi_o_id in related_orders:
            c_ids.append(ol.coi_c_id)
    return c_ids


def find_related_customers(session, query, w_id, d_id, c_id):
    # find customer's orders
    customer_orders = find_customer_orders(session, query, w_id, d_id, c_id)
    # list of list of items
    items_list = [row.co_i_ids for row in customer_orders]
    for w in range(1, 11):
        if w == w_id:
            continue
        for d in range(1, 11):
            ids = []
            for items in items_list:
                ids.append(helper(session, query, w, d, items))
            ids = flatten(ids)
            ids = set(ids)
            for c_id in ids:
                print("{},{},{},{},{},{}".format(w_id, d_id, c_id, w, d, c_id))

def start(session, query, w, d):
    q = "select * from cs5424.customer where c_w_id = %s and c_d_id = %s"
    rows = session.execute(q, [w, d])
    for row in rows:
        find_related_customers(session, query, w, d, row.c_id)



