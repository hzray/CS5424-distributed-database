from transactions import NewOrder, Delivery, Payment, OrderStatus, StockLevel, PopularItem, RelatedCustomer, TopBalance

from cassandra.cluster import Cluster

from QueryPrepare import Query


def main():
    cluster = Cluster()
    session = cluster.connect()
    query = Query(session)

    file = open('/Users/ziruih/Desktop/0.txt', 'r')
    while True:
        line = file.readline()
        if not line:
            break
        args = line.split(",")
        command = args[0]
        if command == 'N':
            new_order_handler = NewOrder.NewOrderHandler(session, query, file, *args[1:])
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


if __name__ == "__main__":
    main()
