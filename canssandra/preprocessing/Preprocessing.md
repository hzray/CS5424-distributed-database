# Preprocessing

Preprocessing of the data file including the following tasks

- Impute missing data into standard format
  - specific for cassandra because cassandra does not support NULL keyword 

- Create new data table `customer_order_items`

- Create new data table `customer_order`



## Imputing 

- Impute missing integer value to `-1`
  - those table have missing integer values including
    - `orders.o_carrier_id`
- Impute missing text value to `unknown`
  - those table have missing string values including
    - `customer.c_state`
- Impute missing timestamp value to `1970-01-01 00:00:00`
  - those table have missing timestamp values including
    - `order_line.ol_delivery_d`



## customer_order_items

This table has fields

- `coi_w_id` 	
- `coi_d_id`
- `coi_c_id`
- `coi_o_id`
- `coi_i_id`



`customer_order_items` is created by joining `order_line` , ` order` and `customer`, in the provided `order_line` table there is no customer-id provided, this table will help to process the related_customer transaction.



## customer_order

This table has fields

- `co_w_id`
- `co_d_id`
- `co_c_id`
- `co_o_id`
- `co_i_ids`

`customer_order` can be viewed as a grouped version of `customer_order_items` where items from the same order are grouped together. `co_i_ids` store a list of item-ids of a specific order.



 