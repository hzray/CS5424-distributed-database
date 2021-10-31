# CS5424 - cockroach
## command
### start a local cluster(secure)
> https://www.cockroachlabs.com/docs/v21.1/secure-a-cluster
1. generate certificates
    ```shell
    $ mkdir certs my-safe-directory
    $ cockroach cert create-ca --certs-dir=certs --ca-key=my-safe-directory/ca.key
    $ cockroach cert create-node localhost $(hostname) --certs-dir=certs --ca-key=my-safe-directory/ca.key
    $ cockroach cert create-client root --certs-dir=certs --ca-key=my-safe-directory/ca.key
    ```
2. start a cluster
    ```shell
    $ cockroach start --certs-dir=certs --store=node1 --listen-addr=localhost:26257 --http-addr=localhost:8080 --join=localhost:26257,localhost:26258,localhost:26259 --background
    $ cockroach init --certs-dir=certs --host=localhost:26257
    $ grep 'node starting' node1/logs/cockroach.log -A 11
    ```
3. use the built-in SQL client
    ```shell
    $ cockroach sql --certs-dir=certs --host=localhost:26257
    $ cat xxx.sql | cockroach sql --certs-dir=certs --host=localhost:26257
    ```
4. stop the cluster
    ```shell
    $ cockroach quit --certs-dir=certs --host=localhost:26257
    $ rm -rf node1 node2 node3 node4 node5 certs my-safe-directory
    ```
### start a local cluster (insecure)
1. start a cluster
```shell
cockroach start \
--insecure \
--store=node1 \
--listen-addr=localhost:26257 \
--http-addr=localhost:8080 \
--join=localhost:26257,localhost:26258,localhost:26259 \
--background
```
2. init cluster
```shell
cockroach init --insecure --host=localhost:26257
```
3. 
### migrate from csv
> https://www.cockroachlabs.com/docs/v21.1/migrate-from-csv.html
1. host the files where the cluster can access them -- Use Userfile for Bulk Operations
    > https://www.cockroachlabs.com/docs/v21.1/use-userfile-for-bulk-operations
    ```shell
    $ cockroach userfile upload /Users/maxroach/Desktop/test-data.csv /test-data.csv --certs-dir=certs
    $ cockroach userfile list '*.csv' --certs-dir=certs
    $ cockroach userfile delete test-data.csv --certs-dir=certs
    ```
    quick imports:
    ```shell
    $ cockroach import db mysqldump /Users/maxroach/Desktop/test-db.sql --certs-dir=certs
    ```
2. import the csv
   ```sql
   CREATE TABLE customers (
      id INT,
      dob DATE,
      first_name STRING,
      last_name STRING,
      joined DATE
   );
   IMPORT INTO customers (id, dob, first_name, last_name, joined)
   CSV DATA ('userfile:///test-data.csv');
   ```
   

## install cockroach on Linux

```shell
curl https://binaries.cockroachdb.com/cockroach-v21.1.11.linux-amd64.tgz | tar -xz
sudo cp -i cockroach-v21.1.11.linux-amd64/cockroach /usr/local/bin/
mkdir -p /usr/local/lib/cockroach
cp -i cockroach-v21.1.11.linux-amd64/lib/libgeos.so /usr/local/lib/cockroach/
cp -i cockroach-v21.1.11.linux-amd64/lib/libgeos_c.so /usr/local/lib/cockroach/
```
```shell
curl https://binaries.cockroachdb.com/cockroach-v21.1.11.linux-amd64.tgz | tar -xz
mkdir -p cockroach
cp -i cockroach-v21.1.11.linux-amd64/lib/libgeos.so cockroach/
cp -i cockroach-v21.1.11.linux-amd64/lib/libgeos_c.so cockroach/
```

## 