from cassandra.cluster import Cluster

from transactions import NewOrder
from transactions.cql.QueryPrepare import PreparedQuery


def main():
    cluster = Cluster(['127.0.0.1'], 6042)
    session = cluster.connect()
    query = PreparedQuery(session)
    new_order_handler = NewOrder.NewOrderHandler(session, query, 'A', 1207, 3, 10, 5)
    new_order_handler.run()

main()