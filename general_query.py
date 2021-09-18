def get_district(session, w_id, d_id):
    row = session.execute("""
                          SELECT * FROM CS5424.district
                          WHERE d_w_id=%s AND d_id=%s
                          """,
                          [w_id, d_id]).one()
    return row


def get_stock(session, w_id, i_id):
    row = session.execute(
        """
        SELECT * FROM CS5424.stock 
        WHERE s_w_id=%s AND s_i_id=%s
        """,
        [w_id, i_id]
    ).one()

    return row


def get_item(session, i_id):
    row = session.execute(
        """
        SELECT * FROM CS5424.item WHERE i_id=%s
        """,
        [i_id]).one()
    return row


def get_warehouse(session, w_id):
    row = session.execute(
            """
            SELECT * FROM CS5424.warehouse 
            WHERE w_id = %s
            """,
            [w_id]).one()
    return row


def get_customer(session, w_id, d_id, c_id):
    row = session.execute("""
                    SELECT * FROM CS5424.customer 
                    WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
                    """,
                          [w_id, d_id, c_id]).one()
    return row


def insert_order(session, o_id, w_id, d_id, c_id, n_items, all_local, time, carrier_id):
    session.execute(
        """
        INSERT INTO 
        CS5424.orders(o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [w_id, d_id, o_id, c_id, carrier_id, n_items, all_local, time]
    )


def insert_order_line(session, o_id, d_id, w_id, i, i_id, sup_id, qty, item_amount, dist_info, delivery):
    session.execute(
        """
        INSERT INTO 
        CS5424.order_line(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount,
                          ol_supply_w_id, ol_quantity, ol_dist_info)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [w_id, d_id, o_id, i, i_id, delivery, item_amount, sup_id, qty, dist_info])
