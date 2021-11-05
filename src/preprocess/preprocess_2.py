import multiprocessing
from collections import Counter
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import ExecutionProfile, Cluster
import pandas as pd
from src.transactions.cql import utils

DATA_PATH = "/temp/project_files/project_files_cassandra/data_files"

class prepareQuery:
    def __init__(self, session):
        self.select_customer = session.prepare("select * from CS5424RAW.customer where c_w_id = ? and c_d_id = ?")
        self.select_customer_order = session.prepare(
            "SELECT * FROM CS5424RAW.customer_order WHERE co_w_id = ? AND co_d_id = ? AND co_c_id = ?")
        self.select_customer_order_items = session.prepare(
            "SELECT * FROM CS5424RAW.customer_order_items WHERE coi_w_id = ? AND coi_d_id = ? AND coi_i_id in ?")


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
    ret = ""
    for w in range(1, 11):
        if w == w_id:
            continue
        for d in range(1, 11):
            ids = []
            for items in items_list:
                ids.append(helper(session, query, w, d, items))
            ids = flatten(ids)
            ids = set(ids)
            for c in ids:
                ret += "{},{},{},{},{},{}\n".format(w_id, d_id, c_id, w, d, c)
    return ret


def startFindRelatedCustomer(session, query, w, d):
    rows = session.execute(query.select_customer, [w, d])
    ret = ""
    for row in rows:
        ret += find_related_customers(session, query, w, d, row.c_id)
    return ret


def wrapper(arg):
    w = arg[0]
    d = arg[1]
    st = time.time()
    read_profile = ExecutionProfile(consistency_level=ConsistencyLevel.ONE, request_timeout=1000.0)
    exec_profile = {'read': read_profile}
    cluster = Cluster(['127.0.0.1'], 6042, execution_profiles=exec_profile)
    session = cluster.connect()
    query = prepareQuery(session)
    print("w={}, d={} begin".format(w, d))
    ret = startFindRelatedCustomer(session, query, w, d)
    print("w={}, d={} done, time = {}".format(w, d, time.time() - st))
    return ret


def createRelatedCustomer():
    pool = multiprocessing.Pool(processes=16)
    args = [[x, y] for x in range(1, 11) for y in range(1, 11)]
    outputs = pool.map(wrapper, args)
    ret = "\n".join(outputs)
    text_file = open("../output/related_customer.txt", "w")
    text_file.write(ret)
    text_file.close()
    rc = pd.read_csv('../output/related_customer.txt', header=None)
    rc.columns = ['w_id', 'd_id', 'c_id', 'r_w_id', 'r_d_id', 'r_c_id']
    rc.to_csv(f'{DATA_PATH}/related_customer.csv', index=None)


def main():
    # Before doing this, the table customer_order_items and customer_order should have been loaded into cs5424.RAW
    createRelatedCustomer()

if __name__ == "__main__":
    main()