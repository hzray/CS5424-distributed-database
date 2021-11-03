#!/usr/bin/bash

cd /temp/apache-cassandra-4.0.1/bin/

./cqlsh localhost 6042 -f /home/stuproj/cs4224m/cs5424_cassandra/src/setup.cql

cd /home/stuproj/cs4224m/cs5424_cassandra/benchmarking/node1/