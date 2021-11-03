from cassandra.cluster import Cluster

from transactions import RelatedCustomer
from transactions.cql.QueryPrepare import PreparedQuery


def main():
    cluster = Cluster(['127.0.0.1'], 9042)
    session = cluster.connect()
    # query = PreparedQuery(session)

    sql = "update cs5424.warehouse set w_city='xxx' where w_id=1"
    res = session.execute(sql,[]).one()
    print(res)
main()