from transactions import NewOrder, Delivery, Payment, OrderStatus, StockLevel, PopularItem

from cassandra.cluster import Cluster


def main():
    cluster = Cluster()
    session = cluster.connect()
    while True:
        line = input("Please enter command\n")
        args = line.split(" ")
        command = args[0]
        if command == 'N':
            new_order_args = [int(x) for x in args[1:]]
            # [c_id, w_id, d_id, M]
            new_order_handler = NewOrder.NewOrderHandler(session, *new_order_args)
            new_order_handler.run()
        elif command == 'P':
            payment_args = [int(x) for x in args[1:]]
            # [c_w_id, c_d_id, c_id, payment]
            payment_handler = Payment.PaymentHandler(session, *payment_args)
            payment_handler.run()
        elif command == 'D':
            # [w_id, carrier_id]
            delivery_args = [int(x) for x in args[1:]]
            delivery_handler = Delivery.DeliveryHandler(session, *delivery_args)
            delivery_handler.run()
        elif command == 'O':
            order_status_args = [int(x) for x in args[1:]]
            order_status_handler = OrderStatus.OrderStatusHandler(session, *order_status_args)
            order_status_handler.run()
        elif command == 'S':
            stock_args = [int(x) for x in args[1:]]
            stock_handler = StockLevel.StockLevelHandler(session, *stock_args)
            stock_handler.run()
        elif command == 'I':
            pop_item_args = [int(x) for x in args[1:]]
            pop_item_handler = PopularItem.PopularItemHandler(session, *pop_item_args)
            pop_item_handler.run()
        elif command == 'T':
            print("")
        elif command == 'R':
            print("")


if __name__ == "__main__":
    main()
