DROP KEYSPACE IF EXISTS CS5424RAW;

CREATE KEYSPACE IF NOT EXISTS CS5424RAW
  WITH REPLICATION = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 3
  };



CREATE TABLE IF NOT EXISTS CS5424RAW.customer_order_items (
  coi_w_id int,
  coi_d_id int,
  coi_c_id int,
  coi_o_id int,
  coi_i_id int,
  PRIMARY KEY ((coi_w_id, coi_d_id), coi_i_id, coi_c_id, coi_o_id)
)
WITH CLUSTERING ORDER BY (coi_i_id ASC, coi_c_id ASC, coi_d_id ASC);

CREATE TABLE IF NOT EXISTS CS5424RAW.customer_order (
  co_w_id int,
  co_d_id int,
  co_c_id int,
  co_o_id int,
  co_i_ids int,
  PRIMARY KEY ((co_w_id, co_d_id), co_c_id, co_o_id)
)
WITH CLUSTERING ORDER BY (co_c_id ASC, co_o_id ASC);

COPY CS5424.CUSTOMER_ORDER_ITEMS (coi_w_id, coi_d_id, coi_o_id, coi_i_id, coi_c_id) FROM '/temp/project_files/project_files_cassandra/data_files/customer_order_items.csv' WITH HEADER=TRUE;

COPY CS5424.CUSTOMER_ORDER (co_w_id, co_d_id, co_c_id, co_o_id, co_i_ids) FROM '/temp/project_files/project_files_cassandra/data_files/customer_order.csv' WITH HEADER=TRUE;
