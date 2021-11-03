DROP KEYSPACE IF EXISTS CS5424;

CREATE KEYSPACE IF NOT EXISTS CS5424
  WITH REPLICATION = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 3
  };


CREATE TABLE IF NOT EXISTS CS5424.warehouse (
  W_ID int,
  W_NAME text,
  W_STREET_1 text,
  W_STREET_2 text,
  W_CITY text,
  W_STATE text,
  W_ZIP text,
  W_TAX decimal,
  W_YTD decimal,
  PRIMARY KEY (W_ID)
);

CREATE TABLE IF NOT EXISTS CS5424.district (
  D_W_ID int,
  D_ID int,
  D_NAME text,
  D_STREET_1 text,
  D_STREET_2 text,
  D_CITY text,
  D_STATE text,
  D_ZIP text,
  D_TAX decimal,
  D_YTD decimal,
  D_NEXT_O_ID int,
  PRIMARY KEY (D_W_ID, D_ID)
)
WITH CLUSTERING ORDER BY (D_ID ASC);

CREATE TABLE IF NOT EXISTS CS5424.customer (
  C_W_ID int,
  C_D_ID int,
  C_ID int,
  C_FIRST text,
  C_MIDDLE text,
  C_LAST text,
  C_STREET_1 text,
  C_STREET_2 text,
  C_CITY text,
  C_STATE text,
  C_ZIP text,
  C_PHONE text,
  C_SINCE timestamp,
  C_CREDIT text,
  C_CREDIT_LIM decimal,
  C_DISCOUNT decimal,
  C_BALANCE decimal,
  C_YTD_PAYMENT decimal,
  C_PAYMENT_CNT int,
  C_DELIVERY_CNT int,
  C_DATA text,
  PRIMARY KEY ((C_W_ID, C_D_ID), C_ID)
)
WITH CLUSTERING ORDER BY (C_ID ASC);

CREATE TABLE IF NOT EXISTS CS5424.item (
  I_ID int,
  I_NAME text,
  I_PRICE decimal,
  I_IM_ID int,
  I_DATA text,
  PRIMARY KEY (I_ID)
);

CREATE TABLE IF NOT EXISTS CS5424.orders (
  O_W_ID int,
  O_D_ID int,
  O_ID int,
  O_C_ID int,
  O_CARRIER_ID int,
  O_OL_CNT int,
  O_ALL_LOCAL int,
  O_ENTRY_D timestamp,
  PRIMARY KEY ((O_W_ID, O_D_ID), O_ID)
)
WITH CLUSTERING ORDER BY (O_ID ASC);

CREATE INDEX IF NOT EXISTS carrier_index
  ON CS5424.orders (O_CARRIER_ID);

CREATE TABLE IF NOT EXISTS CS5424.order_line (
  OL_W_ID int,
  OL_D_ID int,
  OL_O_ID int,
  OL_NUMBER int,
  OL_I_ID int,
  OL_DELIVERY_D timestamp,
  OL_AMOUNT decimal,
  OL_SUPPLY_W_ID int,
  OL_QUANTITY int,
  OL_DIST_INFO text,
  PRIMARY KEY ((OL_W_ID, OL_D_ID), OL_O_ID, OL_NUMBER)
)
WITH CLUSTERING ORDER BY (OL_O_ID ASC, OL_NUMBER ASC);

CREATE TABLE IF NOT EXISTS CS5424.stock (
  S_W_ID int,
  S_I_ID int,
  S_QUANTITY int,
  S_YTD int,
  S_ORDER_CNT int,
  S_REMOTE_CNT int,
  S_DIST_01 text,
  S_DIST_02 text,
  S_DIST_03 text,
  S_DIST_04 text,
  S_DIST_05 text,
  S_DIST_06 text,
  S_DIST_07 text,
  S_DIST_08 text,
  S_DIST_09 text,
  S_DIST_10 text,
  S_DATA text,
  PRIMARY KEY (S_W_ID, S_I_ID)
)
WITH CLUSTERING ORDER BY (S_I_ID ASC);

CREATE TABLE IF NOT EXISTS CS5424.customer_order_items (
  coi_w_id int,
  coi_d_id int,
  coi_c_id int,
  coi_o_id int,
  coi_i_id int,
  PRIMARY KEY ((coi_w_id, coi_d_id), coi_c_id, coi_o_id, coi_i_id)
)
WITH CLUSTERING ORDER BY (coi_c_id ASC, coi_o_id ASC, coi_i_id ASC);

CREATE INDEX IF NOT EXISTS item_index
  ON CS5424.customer_order_items (coi_i_id);

CREATE TABLE IF NOT EXISTS CS5424.customer_order (
    co_w_id int,
    co_d_id int,
    co_c_id int,
    co_o_id int,
    co_i_ids list<int>,
    PRIMARY KEY ((co_w_id, co_d_id), co_c_id, co_o_id)
)
WITH CLUSTERING ORDER BY (co_c_id ASC, co_o_id ASC);

CREATE TABLE IF NOT EXISTS CS5424.related_customer (
  w_id int,
  d_id int,
  c_id int,
  r_w_id int,
  r_d_id int,
  r_c_id int,
  PRIMARY KEY ((w_id, d_id), c_id)
)
WITH CLUSTERING ORDER BY (c_id ASC);


CREATE MATERIALIZED VIEW IF NOT EXISTS CS5424.customer_balance
  AS SELECT C_W_ID, C_D_ID, C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST
  FROM CS5424.customer
  WHERE C_W_ID IS NOT NULL AND C_D_ID IS NOT NULL AND C_ID IS NOT NULL AND C_BALANCE IS NOT NULL AND C_FIRST IS NOT NULL AND C_MIDDLE IS NOT NULL AND C_LAST IS NOT NULL
  PRIMARY KEY ((C_W_ID, C_D_ID), C_BALANCE, C_ID)
  WITH CLUSTERING ORDER BY (C_BALANCE DESC, C_ID ASC);

CREATE MATERIALIZED VIEW IF NOT EXISTS CS5424.orders_customer
  AS SELECT O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_ENTRY_D
  FROM CS5424.orders
  WHERE O_W_ID IS NOT NULL AND O_D_ID IS NOT NULL AND O_C_ID IS NOT NULL AND O_ID IS NOT NULL AND O_CARRIER_ID IS NOT NULL AND O_ENTRY_D IS NOT NULL
  PRIMARY KEY ((O_W_ID, O_D_ID, O_C_ID), O_ID)
  WITH CLUSTERING ORDER BY (O_ID DESC);


COPY CS5424.WAREHOUSE (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) FROM '/temp/project_files/project_files_cassandra/data_files/warehouse.csv' WITH HEADER=TRUE;

COPY CS5424.DISTRICT (D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) FROM '/temp/project_files/project_files_cassandra/data_files/district.csv' WITH HEADER=TRUE;

COPY CS5424.CUSTOMER (C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA) FROM '/temp/project_files/project_files_cassandra/data_files/customer.csv' WITH HEADER=TRUE;

COPY CS5424.ORDERS (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) FROM '/temp/project_files/project_files_cassandra/data_files/order.csv' WITH HEADER=TRUE;

COPY CS5424.ITEM (I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA) FROM '/temp/project_files/project_files_cassandra/data_files/item.csv' WITH HEADER=TRUE;

COPY CS5424.ORDER_LINE (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) FROM '/temp/project_files/project_files_cassandra/data_files/order-line.csv' WITH HEADER=TRUE;

COPY CS5424.STOCK (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07,S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) FROM '/temp/project_files/project_files_cassandra/data_files/stock.csv' WITH HEADER=TRUE;

COPY CS5424.CUSTOMER_ORDER_ITEMS (coi_w_id, coi_d_id, coi_o_id, coi_i_id, coi_c_id) FROM '/temp/project_files/project_files_cassandra/data_files/customer_order_items.csv' WITH HEADER=TRUE;

COPY CS5424.CUSTOMER_ORDER (co_w_id, co_d_id, co_o_id, co_c_id, co_i_ids) FROM '/temp/project_files/project_files_cassandra/data_files/customer_order.csv' WITH HEADER=TRUE;

COPY CS5424.RELATED_CUSTOMER (w_id, d_id, c_id, r_w_id, r_d_id, r_c_id) FROM '/temp/project_files/project_files_cassandra/data_files/related_customer.csv' WITH HEADER=TRUE;
