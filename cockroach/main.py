from Transactions import NewOrder, Delivery, Payment, OrderStatus, StockLevel, PopularItem, RelatedCustomer, TopBalance

import psycopg2


def main():
    conn = psycopg2.connect(
        database='CS5424',
        user='xu',
        sslmode='verify-full',
        sslrootcert='/Users/xujiayi/Desktop/CS5424-cockroach/certs/ca.crt',
        port=26257,
        host='localhost',
        password='123456'
    )

    while True:
        line = input('please input command: ')
        args = line.split(',')
        command = args[0]
        if command == 'N':
            NewOrder.NewOrder(conn, args[1], args[2], args[3], args[4]).new_order_handler()
        # elif command == '':


if __name__ == "__main__":
    main()