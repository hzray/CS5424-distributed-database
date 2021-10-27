import sys
import time
import numpy as np

from transactions import NewOrder, Delivery, Payment, OrderStatus, StockLevel, PopularItem, RelatedCustomer, TopBalance
from cassandra.cluster import Cluster
from QueryPrepare import Query


def main():
    cluster = Cluster()
    session = cluster.connect()
    query = Query(session)

    total_time_start = time.time()
    latencies = []
    xact_count = 0
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        args = line.split(",")
        command = args[0]
        xact_count += 1
        xact_time_start = time.time()
        if command == 'N':
            new_order_handler = NewOrder.NewOrderHandler(session, query, *args[1:])
            new_order_handler.run()
        elif command == 'P':
            payment_handler = Payment.PaymentHandler(session, query,  *args[1:])
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
            related_customer_handler = RelatedCustomer.RelatedCustomerHandler(session, query, *args[1:])
            related_customer_handler.run()
        xact_time_end = time.time()
        latencies.append((xact_time_end - xact_time_start)*1000)
        print()

    total_time_end = time.time()
    total_time = total_time_end - total_time_start

    print("total number of transactions = {}".format(xact_count))
    print("Total elapsed time for processing the transactions = {:.2f} seconds".format(total_time))
    print("Transaction throughput = {:.2f} per second".format(xact_count / total_time))
    print("Average latency = {:.2f}".format(np.average(latencies)))
    print("95th percentile transaction latency = {:.2f)".format(np.percentile(latencies, 95)))
    print("99th percentile transaction latency = {:.2f)".format(np.percentile(latencies, 99)))


if __name__ == "__main__":
    main()
