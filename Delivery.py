# class DeliveryHandler:
#     def __init__(self, cql_session, w_id, carrier_id):
#         self.session = cql_session
#         self.w_id = w_id
#         self.carrier_id = carrier_id
#
#
#     def get_smallest_order_number(self, w_id, d_id):
#         self.session.execute(
#             """
#             SELECT * FROM CS5424.orders WHERE o_w_id = %s and o_d_id = %s and o_carrier_id = %s
#             """,
#             [w_id, d_id, 'unknown'])
#
