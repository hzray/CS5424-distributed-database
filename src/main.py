import csv
import sys
import time
import numpy as np
from cassandra import ConsistencyLevel

from transactions import OrderStatus, Payment, StockLevel, RelatedCustomer, Delivery, NewOrder, PopularItem, \
    TopBalance
from cassandra.cluster import Cluster, ExecutionProfile
from transactions.cql.QueryPrepare import PreparedQuery

PROJECT_PATH = "/home/stuproj/cs4224m/cs5424_cassandra"

def main():
    if len(sys.argv) < 3:
        sys.exit('Must pass workload number and client number')

    workload = sys.argv[1]
    client_id = sys.argv[2]
    cons_level = sys.argv[3]
    if cons_level == 'ROWA':
        read_profile = ExecutionProfile(consistency_level=ConsistencyLevel.ONE, request_timeout=1000.0)
        write_profile = ExecutionProfile(consistency_level=ConsistencyLevel.ALL, request_timeout=1000.0)
        exec_profile = {'read': read_profile, 'write': write_profile}
    elif cons_level == 'QUORUM':
        read_profile = ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM, request_timeout=1000.0)
        write_profile = ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM, request_timeout=1000.0)
        exec_profile = {'read': read_profile, 'write': write_profile}
    else:
        print("Using Default ConsistencyLevel Plan: QUORUM")
        read_profile = ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM, request_timeout=1000.0)
        write_profile = ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM, request_timeout=1000.0)
        exec_profile = {'read': read_profile, 'write': write_profile}


    cluster = Cluster(['127.0.0.1'], 6042, execution_profiles=exec_profile)
    session = cluster.connect()
    query = PreparedQuery(session)

    total_time_start = time.time()
    latencies = []
    success_count = 0
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        args = line.split(",")
        command = args[0]
        xact_time_start = time.time()
        if command == 'N':
            new_order_handler = NewOrder.NewOrderHandler(session, query, workload, *args[1:])
            new_order_handler.run()
        elif command == 'P':
            payment_handler = Payment.PaymentHandler(session, query, *args[1:])
            payment_handler.run()
        elif command == 'D':
            delivery_handler = Delivery.DeliveryHandler(session, query, *args[1:])
            delivery_handler.run()
        elif command == 'O':
            order_status_handler = OrderStatus.OrderStatusHandler(session, query, *args[1:])
            order_status_handler.run()
        elif command == 'S':
            stock_handler = StockLevel.StockLevelHandler(session, query, *args[1:])
            stock_handler.run()
        elif command == 'I':
            pop_item_handler = PopularItem.PopularItemHandler(session, query, *args[1:])
            pop_item_handler.run()
        elif command == 'T':
            top_balance_handler = TopBalance.TopBalanceHandler(session, query)
            top_balance_handler.run()
        elif command == 'R':
            related_customer_handler = RelatedCustomer.RelatedCustomerHandler(session, query, workload, *args[1:])
            related_customer_handler.run()
        xact_time_end = time.time()

        success_count += 1
        latencies.append((xact_time_end - xact_time_start) * 1000)
        print()

    total_time_end = time.time()
    total_time = total_time_end - total_time_start

    throughput = success_count / total_time
    average_latency = np.average(latencies)
    median_latency = np.median(latencies)
    ninety_five_percentile = np.percentile(latencies, 95)
    ninety_nine_percentile = np.percentile(latencies, 99)

    print("Total number of successful transactions = {}".format(success_count))
    print("Total elapsed time for processing the transactions = {:.2f} seconds".format(total_time))
    print("Transaction throughput = {:.2f} per second".format(throughput))
    print("Average transaction latency = {:.2f} ms".format(average_latency))
    print("Median transaction latency = {:.2f} ms".format(np.median(median_latency)))
    print("95th percentile transactions latency = {:.2f} ms".format(ninety_five_percentile))
    print("99th percentile transactions latency = {:.2f} ms".format(ninety_nine_percentile))

    f = open(f'{PROJECT_PATH}/output/workload{workload}/measurement/{client_id}.csv', 'w')
    writer = csv.writer(f)
    row = [client_id, success_count, total_time, throughput, average_latency, median_latency, ninety_five_percentile,
           ninety_nine_percentile]
    writer.writerow(row)
    f.close()


if __name__ == "__main__":
    main()
