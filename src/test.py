from cassandra.cluster import Cluster

from src.transactions.cql.QueryPrepare import PreparedQuery


def main():
    cluster = Cluster()
    session = cluster.connect()
    query = PreparedQuery(session)

