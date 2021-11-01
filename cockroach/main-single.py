from Transactions import NewOrder1, Delivery, Payment, OrderStatus, StockLevel, PopularItem, RelatedCustomer, TopBalance

import psycopg2
import time


def main():
    # conn = psycopg2.connect(
    #     database='CS5424',
    #     user='xu',
    #     sslmode='verify-full',
    #     sslrootcert='/Users/xujiayi/Desktop/CS5424-cockroach/certs/ca.crt',
    #     port=26257,
    #     host='localhost',
    #     password='123456'
    # )
    dsn = "postgresql://root@localhost:26257?sslmode=disable"
    conn = psycopg2.connect(dsn=dsn)

    while True:
        total_time_start = time.time()
        line = input('please input command: ')
        args = line.split(',')
        command = args[0]
        if command == 'N':
            NewOrder1.NewOrder1(conn, args[1], args[2], args[3], args[4]).new_order_handler()
        elif command == 'P':
            Payment.Payment(conn, args[1], args[2], args[3], args[4]).payment_handler()
        elif command == 'D':
            Delivery.Delivery(conn, args[1], args[2]).delivery_handler()
        elif command == 'O':
            OrderStatus.OrderStatus(conn, args[1], args[2], args[3]).order_status_handler()
        elif command == 'S':
            StockLevel.StockLevel(conn, args[1], args[2], args[3], args[4]).stock_level_handler()
        elif command == 'I':
            PopularItem.PopularItem(conn, args[1], args[2], args[3]).popularItem_handler()
        elif command == 'T':
            TopBalance.TopBalance(conn).topBalance_handler()
        elif command == 'R':
            RelatedCustomer.RelatedCustomer(conn, args[1], args[2], args[3]).relatedCustomer_handler()

        total_time_end = time.time()
        total_time = total_time_end - total_time_start
        print(total_time)


if __name__ == "__main__":
    main()
