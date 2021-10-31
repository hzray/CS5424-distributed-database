from cassandra.cluster import Cluster

from transactions.cql.QueryPrepare import PreparedQuery
from preprocessing import preprocessRC

def main():
    cluster = Cluster(['127.0.0.1'], 6042)
    session = cluster.connect()
    query = PreparedQuery(session)
    for w in range(1, 11):
        for d in range(1, 11):
            preprocessRC.start(session, query, w, d)

main()