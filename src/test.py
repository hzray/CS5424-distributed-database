from cassandra.cluster import Cluster

from transactions import RelatedCustomer
from transactions.cql.QueryPrepare import PreparedQuery


def main():
    cluster = Cluster(['127.0.0.1'], 9042)
    session = cluster.connect()
    query = PreparedQuery(session)

    related_customer_handler = RelatedCustomer.RelatedCustomerHandler(session, query, 'A',1,1,2123)
    related_customer_handler.run()

main()