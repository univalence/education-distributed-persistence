# Cassandra

Cassandra is NoSQL database. It is said to be a wide-column store,
meaning that you have tables with thousands or millions of columns.
Cassandra also can be deployed on a cluster of machines, to partition
and replicate data. It can also be deployed on different datacenters
to ensure resiliency and high availability.

On the data side, the data are of tabular kind (like in relational
databases). Data comes with a **primary key**, for indexing purpose.
The primary key is composed of a **partition key** and a **clustering
key**. The partition key is used to send data with this same key on a
dedicated machine. The clustering key is to sort data with the same
partition key.

On the write side, Casssandra is optimized to write data on its
storages. Data are distributed according to the partition key, and
replicated on other machines of the cluster.

On the query side, Cassandra works well with full scan and query on
primary keys (especially on the partition key). But it will not work
with queries on other kind of columns. You can force Cassandra to
perform such queries, but you may have timeout error.

The data is stored in the Cassandra table `stock` in the keyspace
`store`. A _keyspace_ is a namespace that group tables. You can access
Cassandra database and run CQL queries (ie. CQL is a dialect of SQL for
Cassandra) with `cqlsh`.

#### => Exercise

* Open Docker Desktop
* In the container tab, click on a cassandra node
* Click on Terminal tab
* Execute the command `cqlsh`
* Try those queries with `cqlsh`

```sql
SELECT * FROM store.stock LIMIT 10;
SELECT JSON * FROM store.stock LIMIT 10;
SELECT * FROM store.stock WHERE id = '1' LIMIT 10;
SELECT * FROM store.stock WHERE qtt > 5 LIMIT 10;
```

From the last query, you should get an error message. From details in
this message, try to force Cassandra to evaluate the last query. 
