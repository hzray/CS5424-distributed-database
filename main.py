from transactions import NewOrder, Delivery, Payment, OrderStatus, StockLevel, PopularItem, RelatedCustomer, TopBalance

from cassandra.cluster import Cluster


def main():
    cluster = Cluster()
    session = cluster.connect()
    while True:
        line = input("Please enter command\n")
        args = line.split(" ")
        command = args[0]
        if command == 'N':
            new_order_handler = NewOrder.NewOrderHandler(session, *[int(x) for x in args[1:]])
            new_order_handler.run()
        elif command == 'P':
            payment_handler = Payment.PaymentHandler(session, *[int(x) for x in args[1:]])
            payment_handler.run()
        elif command == 'D':
            delivery_handler = Delivery.DeliveryHandler(session, *[int(x) for x in args[1:]])
            delivery_handler.run()
        elif command == 'O':
            order_status_handler = OrderStatus.OrderStatusHandler(session, *[int(x) for x in args[1:]])
            order_status_handler.run()
        elif command == 'S':
            stock_handler = StockLevel.StockLevelHandler(session, *[int(x) for x in args[1:]])
            stock_handler.run()
        elif command == 'I':
            pop_item_handler = PopularItem.PopularItemHandler(session, *[int(x) for x in args[1:]])
            pop_item_handler.run()
        elif command == 'T':
            top_balance_handler = TopBalance.TopBalanceHandler(session)
            top_balance_handler.run()
        elif command == 'R':
            related_customer_handler = RelatedCustomer.RelatedCustomerHandler(session, *[int(x) for x in args[1:]])
            related_customer_handler.run()


if __name__ == "__main__":
    main()
