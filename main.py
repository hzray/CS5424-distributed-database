import Delivery
import NewOrder
from cassandra.cluster import Cluster

import StockLevel


def main():
    cluster = Cluster()
    session = cluster.connect()
    while True:
        line = input("Please enter command\n")
        args = line.split(" ")
        command = args[0]
        if command == 'N':
            new_order_args = [int(x) for x in args[1:]]
            new_order_handler = NewOrder.NewOrderHandler(session, *new_order_args)
            new_order_handler.run()
        elif command == 'P':
            print("")
        elif command == 'D':
            delivery_handler = Delivery.DeliveryHandler(session, int(args[1]), args[2])
            delivery_handler.run()
        elif command == 'O':
            print("")
        elif command == 'S':
            stock_args = [int(x) for x in args[1:]]
            stock_handler = StockLevel.StockLevelHandler(session, *stock_args)
            stock_handler.run()
        elif command == 'I':
            print("")
        elif command == 'T':
            print("")
        elif command == 'R':
            print("")


if __name__ == "__main__":
    main()
