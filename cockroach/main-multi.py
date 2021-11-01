#!/usr/bin/env python

from Transactions import NewOrder, Delivery, Payment, OrderStatus, StockLevel, PopularItem, RelatedCustomer, TopBalance
import psycopg2
import time
import threading
import numpy as np
import csv
from argparse import ArgumentParser
import random


success = False


def driver(line, lines, conn, po, max_retries=3):
    line = line.strip()
    args = line.split(',')
    command = args[0]
    items = []
    for retry in range(0, max_retries):
        start = time.time()
        try:
            if command == 'N':
                fo = open(po + 'NewOrderOutPut.txt', 'a+')
                if retry == 0:
                    items = NewOrder.new_order_input(lines, args[4])
                new_order = NewOrder.NewOrder(conn, args[1], args[2], args[3], args[4], items, fo)
                new_order.new_order_handler()
            elif command == 'P':
                fo = open(po + 'PaymentOutPut.txt', 'a+')
                payment = Payment.Payment(conn, args[1], args[2], args[3], args[4], fo)
                payment.payment_handler()
            elif command == 'D':
                delivery = Delivery.Delivery(conn, args[1], args[2])
                delivery.delivery_handler()
            elif command == 'O':
                fo = open(po + 'OrderStatusOutPut.txt', 'a+')
                order_status = OrderStatus.OrderStatus(conn, args[1], args[2], args[3], fo)
                order_status.order_status_handler()
            elif command == 'S':
                fo = open(po + 'StockLevelOutPut.txt', 'a+')
                stock_level = StockLevel.StockLevel(conn, args[1], args[2], args[3], args[4], fo)
                stock_level.stock_level_handler()
            elif command == 'I':
                fo = open(po + 'PopularItemOutPut.txt', 'a+')
                popular_item = PopularItem.PopularItem(conn, args[1], args[2], args[3], fo)
                popular_item.popularItem_handler()
            elif command == 'T':
                fo = open(po + 'TopBalanceOutPut.txt', 'a+')
                top_balance = TopBalance.TopBalance(conn, fo)
                top_balance.topBalance_handler()
            elif command == 'R':
                fo = open(po + 'RelatedCustomerOutPut.txt', 'a+')
                related_customer = RelatedCustomer.RelatedCustomer(conn, args[1], args[2], args[3], fo)
                related_customer.relatedCustomer_handler()
            else:
                print("command is wrong!" + line)
                return

            end = time.time()
            latency = end - start
            global success
            success = True
            return latency
        except (psycopg2.errors.UniqueViolation, psycopg2.errors.SerializationFailure) as e:
            print("got error: %s", e)
            conn.rollback()
            sleep_ms = (2 ** retry) * 0.1 * (random.random() + 0.5)
            print("Sleeping %s seconds", sleep_ms)
            time.sleep(sleep_ms)
    raise ValueError("Transaction " + command + " did not succeed after {} retries".format(max_retries))


class ClientReport:
    def __init__(self, a, b, c, d, e, f, g):
        self.a = a
        self.b = b
        self.c = c
        self.d = d
        self.e = e
        self.f = f
        self.g = g


# one client run one txt file
class ClientThread(threading.Thread):
    def __init__(self, file_path_i, ipaddress, port, fid, client_reports, file_path_o):
        threading.Thread.__init__(self)
        self.file_path_i = file_path_i
        self.file_path_o = file_path_o
        self.client_reports = client_reports
        self.ipaddress = ipaddress
        self.port = port
        self.fid = fid
        self.latencies = []

    def run(self):
        dsn = "postgresql://root@{}:{}?sslmode=disable".format(self.ipaddress, self.port)
        conn = psycopg2.connect(dsn=dsn)
        file_name = self.file_path_i + self.fid + '.txt'
        print("{}.txt start".format(self.fid))
        total_start = time.time()
        t_number = 0
        with open(file_name, 'r') as fr:
            lines = fr.readlines()
            index = 0
            while index < len(lines):
                line = lines[index]
                try:
                    args = line.split(',')
                    nlines = []
                    if args[0] == 'N':
                        for ni in range(index+1, index+1+int(args[4])):
                            nlines.append(lines[ni])
                        index += int(args[4])
                    global success
                    success = False
                    latency = driver(line, nlines, conn, self.file_path_o)
                    if success:
                        self.latencies.append(latency)
                        t_number += 1
                except ValueError as e:
                    print("got error: %s", e)
                    conn.close()
                    conn = psycopg2.connect(dsn=dsn)
                index += 1

        total_end = time.time()
        # Close communication with the database.
        conn.close()
        total_latency = total_end - total_start
        throughput = t_number / total_latency
        average_latency = np.average(self.latencies)
        median_latency = np.median(self.latencies)
        latency_95th = np.percentile(self.latencies, 95)
        latency_99th = np.percentile(self.latencies, 99)
        self.client_reports[self.fid] = ClientReport(t_number, total_latency, throughput, average_latency,
                                                     median_latency, latency_95th, latency_99th)
        print("{}. t_number: {}, total_latency: {}, throughput: {}, "
              "average_latency: {}, median_latency: {}, latency_95th: {}, latency_99th: {}". \
              format(self.fid, t_number, total_latency, throughput, average_latency,
                     median_latency, latency_95th, latency_99th))


# one server run 8 clients
class Server:
    def __init__(self, file_path_i, ipaddress, port, cid, file_path_o):
        self.file_path_i = file_path_i
        self.file_path_o = file_path_o
        self.ipaddress = ipaddress
        self.port = port
        self.cid = int(cid)

        self.treads = []
        self.client_reports = {}

    def run(self):
        for i in range(self.cid * 8 + 0, self.cid * 8 + 8):
            self.treads.append(ClientThread(self.file_path_i, self.ipaddress, self.port,
                                            str(i), self.client_reports, self.file_path_o))

        for t in self.treads:
            t.start()
        for t in self.treads:
            t.join()

        with open(self.file_path_o + 'clients.csv', 'a+', newline='') as file:
            for k in self.client_reports.keys():
                writer = csv.writer(file)
                writer.writerow([k, self.client_reports[k].a, self.client_reports[k].b, self.client_reports[k].c,
                                 self.client_reports[k].d, self.client_reports[k].e, self.client_reports[k].f,
                                 self.client_reports[k].g])


def parse_cmdline():
    parser = ArgumentParser(description=__doc__)

    parser.add_argument('-address', type=str, help='ip address', default='localhost')
    parser.add_argument('-port', type=str, help='port', default='26257')
    # parser.add_argument('--port', type=str, help='port', default='26277')
    parser.add_argument('-sid', type=str, help='server id', default=0)
    parser.add_argument('-xfpath', type=str, help='input file A or B',
                        default='/Users/Administrator/Desktop/cs5424db/project_files/xact_files_A/')
    # parser.add_argument('-xfpath', type=str, help='input file A or B',
    #                     default='/Users/Administrator/Desktop/cs5424db/')
    # parser.add_argument('--xact_file', type=str, help='input file A or B',
    #                     default='/temp/cs5424m/project_files/xact_files_A/')
    parser.add_argument('-rfpath', type=str, help='output report direction',
                        default='/Users/Administrator/PycharmProjects/CS5424-neliy/cockroach/')
    # parser.add_argument('--report_file', type=str, help='output report direction',
    #                     default='/temp/cs5424m/project_files/')
    opt = parser.parse_args()

    return opt


def main():
    opt = parse_cmdline()

    Server(opt.xfpath, opt.address, opt.port, opt.sid, opt.rfpath).run()


if __name__ == "__main__":
    main()
