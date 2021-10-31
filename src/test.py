import time

from cassandra.cluster import Cluster

from src.transactions import RelatedCustomer
from src.transactions.cql.QueryPrepare import PreparedQuery


def main():
    cluster = Cluster(['127.0.0.1'], 9042)
    session = cluster.connect()
    query = PreparedQuery(session)
    start = time.time()
    related_customer_handler = RelatedCustomer.RelatedCustomerHandler(session, query, 1, 1, 2123)
    related_customer_handler.slow_run()
    print(time.time() - start)



if __name__ == '__main__':
    main()