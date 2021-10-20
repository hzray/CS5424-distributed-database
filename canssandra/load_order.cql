DROP TABLE IF EXISTS CS5424.customer_order_items;

CREATE TABLE IF NOT EXISTS CS5424.customer_order_items (
  coi_w_id int,
  coi_d_id int,
  coi_c_id int,
  coi_o_id int,
  coi_i_ids list<int>,
  PRIMARY KEY ((coi_w_id, coi_d_id), coi_c_id, coi_o_id)
);

COPY CS5424.CUSTOMER_ORDER_ITEMS (coi_W_ID, coi_D_ID, coi_O_ID, coi_I_IDS, coi_C_ID) FROM '~/Desktop/project_files_4/data_files/customer_order_items.csv' WITH HEADER=TRUE;