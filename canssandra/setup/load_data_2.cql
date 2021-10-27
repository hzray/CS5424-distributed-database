COPY CS5424.CUSTOMER_ORDER_ITEMS (coi_w_id, coi_d_id, coi_o_id, coi_i_id, coi_c_id) FROM '~/Desktop/project_files_4/data_files/customer_order_items.csv' WITH HEADER=TRUE;

COPY CS5424.CUSTOMER_ORDER (co_w_id, co_d_id, co_o_id, co_c_id, co_i_ids) FROM '~/Desktop/project_files_4/data_files/customer_order.csv' WITH HEADER=TRUE;