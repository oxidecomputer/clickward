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
CREATE TABLE db1.table1 ON CLUSTER test_cluster (
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
CREATE TABLE db1.table1 ON CLUSTER test_cluster (
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
