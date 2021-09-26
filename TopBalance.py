# https://stackoverflow.com/a/35708544
# Cassandra achieves performance by using the clustering keys to
# sort your data on-disk, thereby only returning ordered rows in a single read (no random reads).
# This is why you must take a query-based modeling approach (often duplicating your data into multiple query tables)
# with Cassandra. Know your queries ahead of time, and build your tables to serve them.

