import NewOrder
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
            new_order_handler = NewOrder.NewOrderHandler(session, *new_order_args)
            new_order_handler.create_new_order()
        elif command == 'P':
            print("")
        elif command == 'D':
            print("")
        elif command == 'O':
            print("")
        elif command == 'S':
            print("")
        elif command == 'I':
            print("")
        elif command == 'T':
            print("")
        elif command == 'R':
            print("")


if __name__ == "__main__":
    main()
