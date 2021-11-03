update_district_o_id_counter = "UPDATE CS5424.district_counter SET d_o_id_change = d_o_id_change + 1 " \
                               "WHERE d_w_id = ? AND d_id = ?"

update_stock_quantity = "UPDATE CS5424.stock SET s_quantity = ? WHERE s_w_id = ? AND s_i_id = ?"

update_stock_ytd_and_order_cnt = "UPDATE CS5424.stock_counter SET s_ytd_change = s_ytd_change + ?, " \
                        "s_order_cnt_change = s_order_cnt_change + 1 where s_w_id = ? and s_i_id = ?"

update_stock_remote_cnt_change = "UPDATE CS5424.stock_counter SET s_remote_cnt_change = s_remote_cnt_change + 1 " \
                          "WHERE s_w_id = ? and s_i_id = ?"

update_warehouse_ytd_change = "UPDATE CS5424.warehouse_counter SET w_ytd_change = w_ytd_change + ? where w_id = ?"

update_district_ytd_change = "UPDATE CS5424.district_counter SET d_ytd_change = d_ytd_change + ? where d_w_id = ? AND d_id = ?"

update_customer_payment_counters = "UPDATE CS5424.customer_counter SET c_balance_change = c_balance_change - ?, " \
                           "c_ytd_payment_change = c_ytd_payment_change + ?, " \
                           "c_payment_cnt_change = c_payment_cnt_change + 1 " \
                           "where c_w_id = ? AND c_d_id = ? AND c_id = ?"

update_customer_delivery_counters = "UPDATE CS5424.customer_counter SET c_balance_change = c_balance_change + ?, " \
                                    "c_delivery_cnt_change = c_delivery_cnt_change + 1 " \
                                    "WHERE c_w_id = ? and c_d_id = ? and c_id = ?"

update_order_carrier_id = "UPDATE CS5424.orders SET o_carrier_id = ? WHERE o_w_id = ? and o_d_id = ? and o_id = ?"

update_order_line_delivery_d = "UPDATE CS5424.order_line SET ol_delivery_d = ? " \
                               "WHERE ol_w_id = ? and ol_d_id = ? and ol_o_id = ? and ol_number = ?"

insert_order_line = """
                    INSERT INTO 
                    CS5424.order_line(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, 
                                ol_supply_w_id, ol_quantity, ol_dist_info)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """

insert_order = "INSERT INTO " \
               "CS5424.orders(o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) " \
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

insert_customer_order_items = "INSERT INTO CS5424.customer_order_items(coi_w_id, coi_d_id, coi_c_id, coi_o_id, " \
                              "coi_i_id) VALUES(?, ?, ?, ?, ?)"

insert_customer_order = "INSERT INTO CS5424.customer_order(co_w_id, co_d_id, co_c_id, co_o_id, " \
                        "co_i_ids) VALUES(?, ?, ?, ?, ?)"

select_district = "SELECT * FROM CS5424.district WHERE d_w_id = ? AND d_id = ?"

select_item = "SELECT * FROM CS5424.item WHERE i_id = ?"

select_stock = "SELECT * FROM CS5424.stock WHERE s_w_id = ? AND s_i_id = ?"

select_warehouse = "SELECT * FROM CS5424.warehouse where w_id = ?"

select_customer = "SELECT * FROM CS5424.customer WHERE c_w_id = ? and c_d_id = ? and c_id = ?"

select_order_with_carrier = "SELECT * FROM CS5424.orders WHERE o_w_id = ? and o_d_id = ? and o_carrier_id = ?"

select_order_line = "SELECT * FROM CS5424.order_line WHERE ol_w_id = ? and ol_d_id = ? and ol_o_id = ?"

select_order_with_customer = "SELECT * FROM CS5424.orders_customer WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?"

select_order_line_in_range = "SELECT * FROM CS5424.order_line where ol_w_id = ? and ol_d_id = ? and ol_o_id >= ? and ol_o_id < ?"

select_order_in_range = "SELECT * FROM CS5424.orders WHERE o_w_id = ? AND o_d_id = ? AND o_id >= ? AND o_id < ?"

select_customer_sort_by_balance = "SELECT * FROM CS5424.customer_balance WHERE c_w_id = ? AND c_d_id = ? LIMIT ?"

select_customer_order_items = "SELECT * FROM CS5424.customer_order_items WHERE coi_w_id = ? AND coi_d_id = ? AND coi_i_id in ?"

select_customer_by_district = "SELECT * FROM CS5424.customer WHERE c_w_id = ? AND c_d_id = ?"

select_customer_order = "SELECT * FROM CS5424.customer_order WHERE co_w_id = ? AND co_d_id = ? AND co_c_id = ?"

select_related_customer = "SELECT * FROM CS5424.related_customer where w_id = ? AND d_id = ? AND c_id = ?"

select_district_o_id_change = "SELECT * FROM CS5424.district_counter where d_w_id = ? AND d_id = ?"

insert_related_customer = "INSERT INTO CS5424.related_customer(w_id, d_id, c_id, r_w_id, " \
                        "r_d_id, r_c_id) VALUES(?, ?, ?, ?, ?, ?)"

