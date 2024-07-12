# Purpose 

Clickward is a CLI tool for config generation and setup of replicated
clickhouse clusters. It's primary goal is to allow rapid standup of clusters
for experimentation and testing.

Users new to clickhouse and clickhouse keeper, such as myself, can use this
tool to stand up clusters and play around. The configuration is supposed to be
close to what we will end up using in Omicron, and the tooling will be ported to
reconfigurator, sled-agent, and any other relevant places. For now, though, this
is the basis of experimentation. In order to share tooling between `clickward`
and omicron, it's more than likely that the current repo will expand to include
a library crate that can be used in omicron. Doing things this way  will allow
us to use clickward directly over time as we experiment with new clickhouse
features and settings without the overhead of omicron build times or knowledge
of its internals. Iterations in clickward are very fast, and that is a key
feature that we would not want to lose.

# Prerequisistes

You need clickhouse installed. Most of us already have an omicron install, so
the easiest way to get this working is to install the omicron prereqs and then
`pushd <OMICRON_DIR>; source env.sh; popd`.

This will make the omicron installed binaries available for use with clickward.

# Getting Started

First, a user should generate configuration for a cluster of keepers and
clickhouse servers. This uses localhost for listen ports and is intended to
be repeatable.

The following command generates clickhouse-keeper cluster with 3 nodes, and two
clickhouse server nodes. Every deployment lives under the `path` used on the
command line in a directory called `deployment`.

```
cargo run gen-config --path . --num-keepers 3 --num-replicas 2
```

The next step is to start running the nodes. Use the same path as where you
generated the config.

```
cargo run deploy --path .
```

At this point your cluster should be running. Wow, wasn't that fast :D

Now, you'll want to go ahead and connect to one of the clickhouse servers using
it's client. All replicas start at `22000` + `id`, where id is an integer. This
setting is hardcoded as `CLICKHOUSE_BASE_TCP_PORT` in the code and is currently
not configurable.

Let's connect to the first of the two clickhouse servers.

```
clickhouse client --port 22001
```

Now, let's create a database.

```sql
CREATE DATABASE IF NOT EXISTS db1 ON CLUSTER test_cluster
```

Let's also create a replicated table. All replication occurs at the [table level](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication).

```sql
CREATE TABLE IF NOT EXISTS db1.table1 ON CLUSTER test_cluster (
    `id` UInt64,
    `column1` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/table1', '{replica}')
ORDER BY id
```

Now, let's insert some data. 

```sql
 INSERT INTO db1.table1 (id, column1) VALUES (1, 'abc');
```

Now, let's see what's there.

```sql
select * from db1.table1
```

Now, let's connect to our other clickhouse server and ensure the data is
replicated. Remember, that you'll need the clickhouse binaries in your path if
you use a new terminal for this.

The data is automatically replicated because we created the table when the two
nodes were already known to each other. This is not true later when we add new
nodes to the cluster.

```
clickhouse client --port 22002
```

```
select * from db1.table1
```

If all goes well you should see the replicated data on both servers.

You can experiment all you want with these static clusters.


## Dynamic cluster reconfiguration

`clickward` allows growing and shrinking your keeper cluster. The commands
are self explanatory. More detail can be found about how this works in the
[clickhouse-keeper documentation](https://clickhouse.com/docs/en/guides/sre/keeper/clickhouse-keeper).

`clickward` also allows adding and removing clickhouse servers. These servers
are standalone, but replicated tables will be replicated there as long as
they are created on the new servers. When you add a server, even though the
configuration files at all nodes get updated, clickhouse doesn't automatically
replicate everything to this new server. This actually has the benefit of
allowing you to manually limit replicas to certain subsets of your nodes. Manual
sharding in effect. We'll go over adding a new replica now. First though, let's
go through the first part of using [system tables](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables).

Let's look at our current cluster from an existing clickhouse server. We need to login again first.

```
clickhouse client --port 22001
```

```
select * from system.replicas format Vertical
```

This shows you a lot of information about our current replicas. You'll want to
look at this again later.

Now let's add a new clickhouse server.

```
cargo run add-server --path .
```

We need to now login to this server and create the database and table locally.
It will actually check keeper and see that the replicas exist and the data will
be there.

```
clickhouse client --port 22003
```


```sql
CREATE DATABASE IF NOT EXISTS db1 ON CLUSTER test_cluster
```

```sql
CREATE TABLE IF NOT EXISTS db1.table1 ON CLUSTER test_cluster (
    `id` UInt64,
    `column1` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/table1', '{replica}')
ORDER BY id
```

Now, we should see that the data is replicated

```sql
select * from db1.table1
```

Now go ahead and take a look at the system replicas table again. You should see
three active nodes. You can do this on any of the three servers.

```
select * from system.replicas format Vertical
```

You may also wish to experiment with removing replicas. We aren't going to fully
document that now, although note that if you remove a replica via `clickward`
it will remain in the `system.replicas` table, but be marked inactive after a
few seconds. You can drop this from an existing node via `system drop replica
'<id>'`, where `<id>` is the identity of the removed node.

Example: `system drop replica '1'`

## Inserting some larger data

We're going to follow the [advanced tutorial](https://clickhouse.com/docs/en/tutorial) from the clickhouse docs. However, our build for helios
does not include the `s3` function, so we will have to download the dataset manually.

### Download a dataset of ~1 million rows

```
curl -O 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_1.gz'
```

### Create our table in our existing db and cluster by connecting to either node

```sql
CREATE TABLE IF NOT EXISTS db1.trips ON CLUSTER test_cluster(
    `trip_id` UInt32,
    `vendor_id` Enum8('1' = 1, '2' = 2, '3' = 3, '4' = 4, 'CMT' = 5, 'VTS' = 6, 'DDS' = 7, 'B02512' = 10, 'B02598' = 11, 'B02617' = 12, 'B02682' = 13, 'B02764' = 14, '' = 15),
    `pickup_date` Date,
    `pickup_datetime` DateTime,
    `dropoff_date` Date,
    `dropoff_datetime` DateTime,
    `store_and_fwd_flag` UInt8,
    `rate_code_id` UInt8,
    `pickup_longitude` Float64,
    `pickup_latitude` Float64,
    `dropoff_longitude` Float64,
    `dropoff_latitude` Float64,
    `passenger_count` UInt8,
    `trip_distance` Float64,
    `fare_amount` Float32,
    `extra` Float32,
    `mta_tax` Float32,
    `tip_amount` Float32,
    `tolls_amount` Float32,
    `ehail_fee` Float32,
    `improvement_surcharge` Float32,
    `total_amount` Float32,
    `payment_type` Enum8('UNK' = 0, 'CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4),
    `trip_type` UInt8,
    `pickup` FixedString(25),
    `dropoff` FixedString(25),
    `cab_type` Enum8('yellow' = 1, 'green' = 2, 'uber' = 3),
    `pickup_nyct2010_gid` Int8,
    `pickup_ctlabel` Float32,
    `pickup_borocode` Int8,
    `pickup_ct2010` String,
    `pickup_boroct2010` String,
    `pickup_cdeligibil` String,
    `pickup_ntacode` FixedString(4),
    `pickup_ntaname` String,
    `pickup_puma` UInt16,
    `dropoff_nyct2010_gid` UInt8,
    `dropoff_ctlabel` Float32,
    `dropoff_borocode` UInt8,
    `dropoff_ct2010` String,
    `dropoff_boroct2010` String,
    `dropoff_cdeligibil` String,
    `dropoff_ntacode` FixedString(4),
    `dropoff_ntaname` String,
    `dropoff_puma` UInt16
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/trips', '{replica}')
PARTITION BY toYYYYMM(pickup_date)
ORDER BY pickup_datetime;
```

### Insert our dataset

```sql
INSERT INTO db1.trips FROM INFILE 'trips_1.gz' COMPRESSION 'gzip' FORMAT TabSeparatedWithNames
```

### Perform some queries

These queries should work on either node 1 or 2.

```sql
SELECT count() FROM trips
```

```sql
SELECT DISTINCT(pickup_ntaname) FROM trips
```

```sql
SELECT round(avg(tip_amount), 2) FROM trips
```

```sql
SELECT
    passenger_count,
    ceil(avg(total_amount),2) AS average_total_amount
FROM trips
GROUP BY passenger_count
```

```sql
SELECT
    pickup_date,
    pickup_ntaname,
    SUM(1) AS number_of_trips
FROM trips
GROUP BY pickup_date, pickup_ntaname
ORDER BY pickup_date ASC
```

# Hard Reset

If you wan to start over, just delete your deployment configurations and kill the processes.

```
rm -rf <deployment_dir>
pkill clickhouse
```
