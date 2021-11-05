#!/usr/bin/env python

import NewOrderA
import Delivery
import Payment
import OrderStatus
import StockLevel
import PopularItem
import RelatedCustomerA
import TopBalance
import psycopg2
import time
import threading
import numpy as np
import csv
from argparse import ArgumentParser
import random
import logging

success = False


def driver(line, lines, conn, po, max_retries=3):
    line = line.strip()
    args = line.split(',')
    command = args[0]
    for retry in range(0, max_retries):
        start = time.time()
        try:
            if command == 'N':
                with open(po + 'NewOrderOutPut.txt', 'a+') as fo:
                    new_order = NewOrderA.NewOrder(conn, args[1], args[2], args[3], args[4], fo)
                    if retry == 0:
                        new_order.new_order_input(lines, args[4])
                    new_order.new_order_handler()
            elif command == 'P':
                with open(po + 'PaymentOutPut.txt', 'a+') as fo:
                    payment = Payment.Payment(conn, args[1], args[2], args[3], args[4], fo)
                    payment.payment_handler()
            elif command == 'D':
                delivery = Delivery.Delivery(conn, args[1], args[2])
                delivery.delivery_handler()
            elif command == 'O':
                with open(po + 'OrderStatusOutPut.txt', 'a+') as fo:
                    order_status = OrderStatus.OrderStatus(conn, args[1], args[2], args[3], fo)
                    order_status.order_status_handler()
            elif command == 'S':
                with open(po + 'StockLevelOutPut.txt', 'a+') as fo:
                    stock_level = StockLevel.StockLevel(conn, args[1], args[2], args[3], args[4], fo)
                    stock_level.stock_level_handler()
            elif command == 'I':
                with open(po + 'PopularItemOutPut.txt', 'a+') as fo:
                    popular_item = PopularItem.PopularItem(conn, args[1], args[2], args[3], fo)
                    popular_item.popularItem_handler()
            elif command == 'T':
                with open(po + 'TopBalanceOutPut.txt', 'a+') as fo:
                    top_balance = TopBalance.TopBalance(conn, fo)
                    top_balance.topBalance_handler()
            elif command == 'R':
                with open(po + 'RelatedCustomerOutPut.txt', 'a+') as fo:
                    related_customer = RelatedCustomerA.RelatedCustomer(conn, args[1], args[2], args[3], fo)
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
            logging.debug("got error: %s", e)
            conn.rollback()
            sleep_ms = (2 ** retry) * 0.1 * (random.random() + 0.5)
            logging.debug("Sleeping %s seconds", sleep_ms)
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
class ClientThread:
    def __init__(self, file_path_i, ipaddress, port, fid, file_path_o):
        self.file_path_i = file_path_i
        self.file_path_o = file_path_o
        self.ipaddress = ipaddress
        self.port = port
        self.fid = fid
        self.latencies = []

        self.Latency = {'N': [], 'P': [], 'D': [], 'O': [], 'S': [], 'I': [], 'T': [], 'R': []}

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
                    line = line.strip()
                    args = line.split(',')
                    nlines = []
                    if args[0] == 'N':
                        for ni in range(index + 1, index + 1 + int(args[4])):
                            nlines.append(lines[ni])
                        index += int(args[4])
                    global success
                    success = False
                    latency = driver(line, nlines, conn, self.file_path_o)
                    if success:
                        self.latencies.append(latency)
                        t_number += 1
                        # print("{}. Transaction {} takes {} ms".format(str(index), args[0], str(latency)))
                        self.Latency[args[0]].append(latency)
                except ValueError as e:
                    print("got error: %s", e)
                    conn.close()
                    conn = psycopg2.connect(dsn=dsn)
                index += 1
                if t_number % 10 == 0:
                    now_end = time.time()
                    print("{}. Transaction {} {} at {} s".format(self.fid, str(t_number),
                                                                 args[0], str(now_end - total_start)))

                # if t_number >= 100:
                #     break
        total_end = time.time()
        # Close communication with the database.
        conn.close()
        total_latency = total_end - total_start
        throughput = t_number / total_latency
        average_latency = np.average(self.latencies)
        median_latency = np.median(self.latencies)
        latency_95th = np.percentile(self.latencies, 95)
        latency_99th = np.percentile(self.latencies, 99)

        print("{}. t_number: {}, total_latency: {}, throughput: {}, "
              "average_latency: {}, median_latency: {}, latency_95th: {}, latency_99th: {}". \
              format(self.fid, t_number, total_latency, throughput, average_latency,
                     median_latency, latency_95th, latency_99th))

        with open(self.file_path_o + 'clients.csv', 'a+', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([self.fid, t_number, total_latency, throughput, average_latency,
                             median_latency, latency_95th, latency_99th])

        print("N average: {}\n"
              "P average: {}\n"
              "D average: {}\n"
              "O average: {}\n"
              "S average: {}\n"
              "I average: {}\n"
              "T average: {}\n"
              "R average: {}\n".format(
            np.average(self.Latency['N']),
            np.average(self.Latency['P']),
            np.average(self.Latency['D']),
            np.average(self.Latency['O']),
            np.average(self.Latency['S']),
            np.average(self.Latency['I']),
            np.average(self.Latency['T']),
            np.average(self.Latency['R']),
        ))


def parse_cmdline():
    parser = ArgumentParser(description=__doc__)

    parser.add_argument('-address', type=str, help='ip address', default='localhost')
    parser.add_argument('-port', type=str, help='port', default='26257')
    parser.add_argument('-cid', type=str, help='server id', default='0')
    parser.add_argument('-xfpath', type=str, help='input file A or B',
                        default='/Users/Administrator/Desktop/cs5424db/project_files/my_xact/')
    parser.add_argument('-rfpath', type=str, help='output report direction',
                        default='/Users/Administrator/PycharmProjects/CS5424-neliy/cockroach/output/')
    opt = parser.parse_args()
    return opt


def main():
    opt = parse_cmdline()
    # python main-singleA.py -address='' -port='' -cid='' -xfpath='' -rfpath=''
    ClientThread(opt.xfpath, opt.address, opt.port, opt.cid, opt.rfpath).run()


if __name__ == "__main__":
    main()
