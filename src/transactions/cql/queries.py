update_district_next_o_id = "UPDATE CS5424.district SET d_next_o_id = ? WHERE d_w_id = ? AND d_id = ? IF d_next_o_id " \
                            "= ? "

update_stock = "UPDATE CS5424.stock SET s_quantity = ?, s_ytd =  ?, s_order_cnt = ?, s_remote_cnt = ? " \
               "WHERE s_w_id = ? AND s_i_id = ?"

update_warehouse_ytd = "UPDATE CS5424.warehouse SET w_ytd = ? where w_id = ?"

update_district_ytd = "UPDATE CS5424.district SET d_ytd = ? where d_w_id = ? AND d_id = ?"

update_customer_payment = "UPDATE CS5424.customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? " \
                          "where c_w_id = ? AND c_d_id = ? AND c_id = ? if c_balance = ?"

update_customer_delivery = "UPDATE CS5424.customer SET c_balance = ?, c_delivery_cnt = ? " \
                           "WHERE c_w_id = ? and c_d_id = ? and c_id = ? if c_balance = ?"

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