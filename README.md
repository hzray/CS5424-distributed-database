# Cassandra

#### Module Installing

The following external Python modules are required

1. cassandra-driver
2. numpy
3. pandas

### Cassandra Configuration

##### Add Cassandra to PATH environment variable

~~~basj
export PATH=$PATH:/temp/apache-cassandra-4.0.1/bin
~~~

##### Configure `cassandra.yaml`

`cassandra.yaml` on five nodes should have following configuration:

- Set cluster name:

  ~~~yaml
  cluster_name: 'groupM'
  ~~~

- Set seed address

  ~~~yaml
  seed_provider:
      - class_name: org.apache.cassandra.locator.SimpleSeedProvider
        parameters:
            # You can use any node addresses of your cluster as seeds
            # Ex: "<ip1>,<ip2>,<ip3>"
            # Here we use xcnc50.comp.nus.edu.sg and xcnc51.comp.nus.edu.sg as seeds
            - seeds: "192.168.51.13,192.168.51.14"
  ~~~

- Set listening address

  ~~~yaml
  listen_address: # leaving it blank leaves it up to InetAddress.getLocalHost().
  ~~~

- Set port number

  ~~~yaml
  # port for the CQL native transport to listen for clients on
  # For security reasons, you should not expose this port to the internet.  Firewall it if needed.
  native_transport_port: 6042
  ~~~

- Set timeouts

  ~~~yaml
  read_request_timeout_in_ms: 5000000
  range_request_timeout_in_ms: 10000000
  write_request_timeout_in_ms: 2000000
  counter_write_request_timeout_in_ms: 500000
  cas_contention_timeout_in_ms: 1000000
  truncate_request_timeout_in_ms: 600000
  request_timeout_in_ms: 1000000
  ~~~

##### Start Cassandra Server

~~~bash
cassandra
~~~

### Preprocessing 

Data preprocessing will take about three hours (mianly becuase of the creation of ***related_customer*** dataset) , we highly recommend to directly use the processed files saved in `/temp/project_files/project_files_cassandra/data_files/`

~~~bash
python3 preprocess.py
~~~

### Cassandra Setup and Data Loading

~~~bash
cqlsh localhost 6042 --request-timeout=100000 -f /home/stuproj/cs4224m/cs5424_cassandra/src/setup.cql
~~~

This will build data model and load data file into database.

### Start Transaction Experiemnt

Put folders `/benchmarking/node1` to `/benchmarking/node5` into corresponding server (e.g. `xcnc50.comp.nus.edu.sg` to `xcnc54.comp.nus.edu.sg`). Inside which folders lies `exp12.sh`, we have to run these 5 `exp12.sh` at the same time on 5 servers by the same command:

~~~bash
exp12.sh [workload] [consistency_lvl]
~~~

- `workload` = `A` or `B`
- `consistency_lvl` = `QUORUM` or `ROWA`

<!--
Test single transaction:

~~~bash
python3 main.py [workload] [consistency_lvl] [client_id] < xact.txt
~~~

- `workload` = `A` or `B`
- `consistency_lvl` = `QUORUM` or `ROWA`
- `client_id` in range[0, 39]
-->

When need to test another workload, please run `/benchmarking/resetup.sh` on node1.

### Measurement Result

First save the dbstate by running `/src/dbstate.py`：

~~~bash
python3 dbstate.py > dbstate.txt
~~~

Then run `/src/stackClientMeasurement.py`. You may need to check all the input and output path before generating the result at line 7 - line 11 in `stackClientMeasurement.py`.

It will generated *clients.csv*, *throughput.csv* and *dbstate.csv*.

The generated csv files are saved at  `/measurement/workloadX/`





