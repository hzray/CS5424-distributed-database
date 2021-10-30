import cassandra
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT


def main():
    cluster = Cluster(['127.0.0.1'], 6042,
                      execution_profiles={
                          EXEC_PROFILE_DEFAULT: ExecutionProfile(consistency_level=cassandra.ConsistencyLevel.ALL,
                                                                 request_timeout=1000.0)})
    session = cluster.connect()

    query = "select sum(w_ytd) from cs5424.warehouse"
    result = session.execute(query, []).one()
    print("sum(W_YTD) = {}".format(result.system_sum_w_ytd))

    query = "select sum(d_ytd) from cs5424.district"
    result = session.execute(query, []).one()
    print("sum(D_YTD) = {}".format(result.system_sum_d_ytd))

    query = "select sum(d_next_o_id) from cs5424.district"
    result = session.execute(query, []).one()
    print("sum(D_NEXT_O_ID) = {}".format(result.system_sum_d_next_o_id))

    query = "select sum(c_balance) from cs5424.customer"
    result = session.execute(query, []).one()
    print("sum(C_BALANCE) = {}".format(result.system_sum_c_balance))

    query = "select sum(c_ytd_payment) from cs5424.customer"
    result = session.execute(query, []).one()
    print("sum(C_YTD_PAYMENT) = {}".format(result.system_sum_c_ytd_payment))

    query = "select sum(c_payment_cnt) from cs5424.customer"
    result = session.execute(query, []).one()
    print("sum(C_PAYMENT_CNT) = {}".format(result.system_sum_c_payment_cnt))

    query = "select sum(c_delivery_cnt) from cs5424.customer"
    result = session.execute(query, []).one()
    print("sum(C_DELIVERY_CNT) = {}".format(result.system_sum_c_delivery_cnt))

    query = "select max(o_id) from cs5424.orders"
    result = session.execute(query, []).one()
    print("max(O_ID) = {}".format(result.system_max_o_id))

    query = "select sum(o_ol_cnt) from cs5424.orders"
    result = session.execute(query, []).one()
    print("sum(O_OL_CNT) = {}".format(result.system_sum_o_ol_cnt))

    query = "select sum(ol_amount) from cs5424.order_line"
    result = session.execute(query, []).one()
    print("sum(OL_AMOUNT) = {}".format(result.system_sum_ol_amount))

    query = "select sum(ol_quantity) from cs5424.order_line"
    result = session.execute(query, []).one()
    print("sum(OL_QUANTITY) = {}".format(result.system_sum_ol_quantity))

    query = "select sum(s_quantity) from cs5424.stock"
    result = session.execute(query, []).one()
    print("sum(S_QUANTITY) = {}".format(result.system_sum_s_quantity))

    query = "select sum(s_ytd) from cs5424.stock"
    result = session.execute(query, []).one()
    print("sum(S_YTD) = {}".format(result.system_sum_s_ytd))

    query = "select sum(s_order_cnt) from cs5424.stock"
    result = session.execute(query, []).one()
    print("sum(S_ORDER_CNT) = {}".format(result.system_sum_s_order_cnt))

    query = "select sum(s_remote_cnt) from cs5424.stock"
    result = session.execute(query, []).one()
    print("sum(S_REMOTE_CNT) = {}".format(result.system_sum_s_remote_cnt))

if __name__ == "__main__":
    main()

