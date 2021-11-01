from cassandra.cluster import Cluster

from transactions import NewOrder
from transactions.cql.QueryPrepare import PreparedQuery


def main():
    cluster = Cluster(['127.0.0.1'], 9042)
    session = cluster.connect()
    sql = "select * from cs5424.warehouse"
    result = session.execute(sql, []).one()
    result.w_id = 100
    print(result)

main()