SET sql_safe_updates = FALSE;

USE defaultdb;
DROP DATABASE IF EXISTS CS5424 CASCADE;
CREATE DATABASE  CS5424;

USE CS5424;

CREATE TABLE CS5424.warehouse (
  W_ID INT,
  W_NAME STRING,
  W_STREET_1 STRING,
  W_STREET_2 STRING,
  W_CITY STRING,
  W_STATE STRING,
  W_ZIP STRING,
  W_TAX DECIMAL,
  W_YTD DECIMAL,
  PRIMARY KEY (W_ID),
  INDEX (W_ID) STORING (W_NAME, W_YTD)
)
;

CREATE TABLE CS5424.district (
  D_W_ID INT,
  D_ID INT,
  D_NAME STRING,
  D_STREET_1 STRING,
  D_STREET_2 STRING,
  D_CITY STRING,
  D_STATE STRING,
  D_ZIP STRING,
  D_TAX DECIMAL,
  D_YTD DECIMAL,
  D_NEXT_O_ID INT,
  PRIMARY KEY (D_W_ID, D_ID),
  INDEX (D_W_ID, D_ID) STORING (D_NEXT_O_ID, D_NAME, D_YTD),
  CONSTRAINT fk_warehouse FOREIGN KEY (D_W_ID) REFERENCES warehouse
)
;

CREATE TABLE CS5424.customer (
  C_W_ID INT,
  C_D_ID INT,
  C_ID INT,
  C_FIRST STRING,
  C_MIDDLE STRING,
  C_LAST STRING,
  C_STREET_1 STRING,
  C_STREET_2 STRING,
  C_CITY STRING,
  C_STATE STRING,
  C_ZIP STRING,
  C_PHONE STRING,
  C_SINCE TIMESTAMP,
  C_CREDIT STRING,
  C_CREDIT_LIM DECIMAL,
  C_DISCOUNT DECIMAL,
  C_BALANCE DECIMAL,
  C_YTD_PAYMENT DECIMAL,
  C_PAYMENT_CNT INT,
  C_DELIVERY_CNT INT,
  C_DATA STRING,
  PRIMARY KEY (C_W_ID, C_D_ID, C_ID),
  INDEX (C_W_ID, C_D_ID, C_ID) STORING (C_DISCOUNT, C_LAST, C_CREDIT, C_FIRST, C_MIDDLE, C_BALANCE),
  INDEX (C_BALANCE),
  CONSTRAINT fk_district FOREIGN KEY (C_W_ID, C_D_ID) REFERENCES district
)
;


CREATE TABLE CS5424.item (
  I_ID INT,
  I_NAME STRING,
  I_PRICE DECIMAL,
  I_IM_ID INT,
  I_DATA STRING,
  PRIMARY KEY (I_ID),
  INDEX (I_ID) STORING (I_NAME, I_PRICE)
)
;

CREATE TABLE CS5424.orders (
  O_W_ID INT,
  O_D_ID INT,
  O_ID INT,
  O_C_ID INT,
  O_CARRIER_ID INT,
  O_OL_CNT INT,
  O_ALL_LOCAL INT,
  O_ENTRY_D TIMESTAMP,
  PRIMARY KEY (O_W_ID, O_D_ID, O_ID),
--   INDEX (O_W_ID, O_D_ID, O_C_ID, O_ID, O_CARRIER_ID) STORING (O_ENTRY_D),
  INDEX (O_W_ID, O_D_ID, O_ID, O_CARRIER_ID),
  INDEX (O_W_ID, O_D_ID, O_ID) STORING (O_ENTRY_D, O_C_ID),
  CONSTRAINT fk_district FOREIGN KEY (O_W_ID, O_D_ID) REFERENCES district
)
;

CREATE TABLE CS5424.order_line (
  OL_W_ID INT,
  OL_D_ID INT,
  OL_O_ID INT,
  OL_NUMBER INT,
  OL_I_ID INT,
  OL_DELIVERY_D TIMESTAMP,
  OL_AMOUNT DECIMAL,
  OL_SUPPLY_W_ID INT,
  OL_QUANTITY INT,
  OL_DIST_INFO STRING,
  PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER),
  INDEX (OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID),
  INDEX (OL_W_ID, OL_D_ID, OL_O_ID) STORING (OL_AMOUNT),
  INDEX (OL_W_ID, OL_D_ID, OL_QUANTITY) STORING (OL_I_ID),
  CONSTRAINT fk_order FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES orders,
  CONSTRAINT fk_item FOREIGN KEY (OL_I_ID) REFERENCES item
)
;

CREATE TABLE CS5424.stock (
  S_W_ID INT,
  S_I_ID INT,
  S_QUANTITY INT,
  S_YTD INT,
  S_ORDER_CNT INT,
  S_REMOTE_CNT INT,
  S_DIST_01 STRING,
  S_DIST_02 STRING,
  S_DIST_03 STRING,
  S_DIST_04 STRING,
  S_DIST_05 STRING,
  S_DIST_06 STRING,
  S_DIST_07 STRING,
  S_DIST_08 STRING,
  S_DIST_09 STRING,
  S_DIST_10 STRING,
  S_DATA STRING,
  PRIMARY KEY (S_W_ID, S_I_ID),
  INDEX (S_W_ID,S_I_ID, S_QUANTITY),
  CONSTRAINT fk_item FOREIGN KEY (S_I_ID) REFERENCES item,
  CONSTRAINT fk_warehouse FOREIGN KEY (S_W_ID) REFERENCES warehouse
)
;

CREATE TABLE CS5424.related_customer (
    W_ID INT,
    D_ID INT,
    C_ID INT,
    R_W_ID INT,
    R_D_ID INT,
    R_C_ID INT,
    INDEX (W_ID, D_ID, C_ID)
)
;

IMPORT INTO CS5424.related_customer (
    W_ID,
    D_ID,
    C_ID,
    R_W_ID,
    R_D_ID,
    R_C_ID
)
CSV DATA ('userfile:///related_customer.csv')
;

IMPORT INTO CS5424.warehouse (
  W_ID,
  W_NAME,
  W_STREET_1,
  W_STREET_2,
  W_CITY,
  W_STATE,
  W_ZIP,
  W_TAX,
  W_YTD
)
CSV DATA ('userfile:///warehouse.csv')
;

IMPORT INTO CS5424.district (
  D_W_ID,
  D_ID,
  D_NAME,
  D_STREET_1,
  D_STREET_2,
  D_CITY,
  D_STATE,
  D_ZIP,
  D_TAX,
  D_YTD,
  D_NEXT_O_ID
)
CSV DATA ('userfile:///district.csv')
;

IMPORT INTO CS5424.customer (
  C_W_ID,
  C_D_ID,
  C_ID,
  C_FIRST,
  C_MIDDLE,
  C_LAST,
  C_STREET_1,
  C_STREET_2,
  C_CITY,
  C_STATE,
  C_ZIP,
  C_PHONE,
  C_SINCE,
  C_CREDIT,
  C_CREDIT_LIM,
  C_DISCOUNT,
  C_BALANCE,
  C_YTD_PAYMENT,
  C_PAYMENT_CNT,
  C_DELIVERY_CNT,
  C_DATA
)
CSV DATA ('userfile:///customer.csv')
;

IMPORT INTO CS5424.item (
  I_ID,
  I_NAME,
  I_PRICE,
  I_IM_ID,
  I_DATA
)
CSV DATA ('userfile:///item.csv')
;

IMPORT INTO CS5424.orders (
  O_W_ID,
  O_D_ID,
  O_ID,
  O_C_ID,
  O_CARRIER_ID,
  O_OL_CNT,
  O_ALL_LOCAL,
  O_ENTRY_D
)
CSV DATA ('userfile:///order.csv') WITH nullif = 'null'
;

IMPORT INTO CS5424.order_line (
  OL_W_ID,
  OL_D_ID,
  OL_O_ID,
  OL_NUMBER,
  OL_I_ID,
  OL_DELIVERY_D,
  OL_AMOUNT,
  OL_SUPPLY_W_ID,
  OL_QUANTITY,
  OL_DIST_INFO
)
CSV DATA ('userfile:///order-line.csv') WITH nullif = 'null'
;

IMPORT INTO CS5424.stock (
  S_W_ID,
  S_I_ID,
  S_QUANTITY,
  S_YTD,
  S_ORDER_CNT,
  S_REMOTE_CNT,
  S_DIST_01,
  S_DIST_02,
  S_DIST_03,
  S_DIST_04,
  S_DIST_05,
  S_DIST_06,
  S_DIST_07,
  S_DIST_08,
  S_DIST_09,
  S_DIST_10,
  S_DATA
)
CSV DATA ('userfile:///stock.csv') WITH nullif = '0.0'
;

