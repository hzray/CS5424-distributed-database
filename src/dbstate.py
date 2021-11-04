import decimal

import cassandra
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT


def main():
    cluster = Cluster(['127.0.0.1'], 6042,
                      execution_profiles={
                          EXEC_PROFILE_DEFAULT: ExecutionProfile(consistency_level=cassandra.ConsistencyLevel.ALL,
                                                                 request_timeout=1000.0)})
    session = cluster.connect()

    query = "select sum(w_ytd) from cs5424.warehouse"
    sum_w_ytd = session.execute(query, []).one().system_sum_w_ytd
    query = "select sum(w_ytd_change) from cs5424.warehouse_counter"
    sum_w_ytd += session.execute(query, []).one().system_sum_w_ytd_change / decimal.Decimal(100)
    print("sum(W_YTD) = {}".format(sum_w_ytd))

    query = "select sum(d_ytd) from cs5424.district"
    sum_d_ytd = session.execute(query, []).one().system_sum_d_ytd
    query = "select sum(d_ytd_change) from cs5424.district_counter"
    sum_d_ytd += session.execute(query, []).one().system_sum_d_ytd_change / decimal.Decimal(100)
    print("sum(D_YTD) = {}".format(sum_d_ytd))

    query = "select sum(d_base_o_id) from cs5424.district"
    sum_next_o_id = session.execute(query, []).one().system_sum_d_base_o_id
    query = "select sum(d_o_id_change) from cs5424.district_counter"
    sum_next_o_id += session.execute(query, []).one().system_sum_d_o_id_change
    print("sum(D_NEXT_O_ID) = {}".format(sum_next_o_id))

    query = "select sum(c_balance) from cs5424.customer"
    sum_c_balance = session.execute(query, []).one().system_sum_c_balance
    query = "select sum(c_balance_change) from cs5424.customer_counter"
    sum_c_balance += session.execute(query, []).one().system_sum_c_balance_change / decimal.Decimal(100)
    print("sum(C_BALANCE) = {}".format(sum_c_balance))

    query = "select sum(c_ytd_payment) from cs5424.customer"
    sum_c_ytd_payment = session.execute(query, []).one().system_sum_c_ytd_payment
    query = "select sum(c_ytd_payment_change) from cs5424.customer_counter"
    sum_c_ytd_payment += session.execute(query, []).one().system_sum_c_ytd_payment_change
    print("sum(C_YTD_PAYMENT) = {}".format(sum_c_ytd_payment))

    query = "select sum(c_payment_cnt) from cs5424.customer"
    sum_c_payment_cnt = session.execute(query, []).one().system_sum_c_payment_cnt
    query = "select sum(c_payment_cnt_change) from cs5424.customer_counter"
    sum_c_payment_cnt += session.execute(query, []).one().system_sum_c_payment_cnt_change
    print("sum(C_PAYMENT_CNT) = {}".format(sum_c_payment_cnt))

    query = "select sum(c_delivery_cnt) from cs5424.customer"
    sum_c_delivery_cnt = session.execute(query, []).one().system_sum_c_delivery_cnt
    query = "select sum(c_delivery_count_change) from cs5424.customer_counter"
    sum_c_delivery_cnt += session.execute(query, []).one().system_sum_c_delivery_cnt_change
    print("sum(C_DELIVERY_CNT) = {}".format(sum_c_delivery_cnt))

    query = "select max(o_id) from cs5424.orders"
    max_o_id = session.execute(query, []).one().system_max_o_id
    print("max(O_ID) = {}".format(max_o_id))

    query = "select sum(o_ol_cnt) from cs5424.orders"
    sum_o_ol_cnt = session.execute(query, []).one().system_sum_o_ol_cnt
    print("sum(O_OL_CNT) = {}".format(sum_o_ol_cnt))

    query = "select sum(ol_amount) from cs5424.order_line"
    result = session.execute(query, []).one()
    print("sum(OL_AMOUNT) = {}".format(result.system_sum_ol_amount))

    query = "select sum(ol_quantity) from cs5424.order_line"
    sum_ol_quantity = session.execute(query, []).one().system_sum_ol_quantity
    print("sum(OL_QUANTITY) = {}".format(sum_ol_quantity))

    query = "select sum(s_quantity) from cs5424.stock"
    sum_s_quantity = session.execute(query, []).one().system_sum_s_quantity
    print("sum(S_QUANTITY) = {}".format(sum_s_quantity))

    query = "select sum(s_ytd) from cs5424.stock"
    sum_s_ytd = session.execute(query, []).one().system_sum_s_ytd
    query = "select sum(s_ytd_change) from cs5424.stock_counter"
    sum_s_ytd += session.execute(query, []).one().system_sum_s_ytd_change
    print("sum(S_YTD) = {}".format(sum_s_ytd))

    query = "select sum(s_order_cnt) from cs5424.stock"
    sum_s_order_cnt = session.execute(query, []).one().system_sum_s_order_cnt
    query = "select sum(s_order_cnt_change) from cs5424.stock_counter"
    sum_s_order_cnt += session.execute(query, []).one().system_sum_s_order_cnt_change
    print("sum(S_ORDER_CNT) = {}".format(sum_s_order_cnt))

    query = "select sum(s_remote_cnt) from cs5424.stock"
    sum_s_remote_cnt = session.execute(query, []).one().system_sum_s_remote_cnt
    query = "select sum(s_remote_cnt_change) from cs5424.stock_counter"
    sum_s_remote_cnt += session.execute(query, []).one().system_sum_s_remote_cnt_change
    print("sum(S_YTD) = {}".format(sum_s_remote_cnt))

if __name__ == "__main__":
    main()

