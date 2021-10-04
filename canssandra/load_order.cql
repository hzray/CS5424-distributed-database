DROP TABLE IF EXISTS CS5424.customer_order_items;

CREATE TABLE IF NOT EXISTS CS5424.customer_order_items (
  col_w_id int,
  col_d_id int,
  col_c_id int,
  col_o_id int,
  col_i_ids list<int>,
  PRIMARY KEY ((col_w_id, col_d_id), col_c_id, col_o_id)
);

COPY CS5424.CUSTOMER_ORDER_ITEMS (COL_W_ID, COL_D_ID, COL_O_ID, COL_I_IDS, COL_C_ID) FROM '~/Desktop/project_files_cas/data_files_A/customer_order_items.csv' WITH HEADER=TRUE;