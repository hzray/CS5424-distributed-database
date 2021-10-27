COPY CS5424.STOCK (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07,S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) FROM '~/Desktop/project_files_4/data_files/stock.csv' WITH HEADER=TRUE;

COPY CS5424.CUSTOMER_ORDER_ITEMS (coi_w_id, coi_d_id, coi_o_id, coi_i_id, coi_c_id) FROM '~/Desktop/project_files_4/data_files/customer_order_items.csv' WITH HEADER=TRUE;

COPY CS5424.CUSTOMER_ORDER (co_w_id, co_d_id, co_o_id, co_c_id, co_i_ids) FROM '~/Desktop/project_files_4/data_files/customer_order.csv' WITH HEADER=TRUE;