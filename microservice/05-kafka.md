# Apache Kafka

Apache Kafka is a stream processing platform. It works on a cluster of
machines and uses the same technics as NoSQL databases.

Stream processing consists in viewing data as potential infinite flow
of information to process constantly. You might think here of messages
coming from a social network, of the evolution of the price of some
financial products, of weather forecast... This kind of data
information as no end, and the values change continuously.

It might sound strange to use NoSQL technics, from which we would
rather have a static perception of data, to create a stream processing
platform, from which we would rather have a dynamic perception of data.
But, in fact, those technics fit perfectly with stream processing and
comes with additional handy features: especially the storage of data
on a drive, data partitioning and replication, load balancing...

## Concepts in Kafka

In Kafka, you will use **topics** to transmit data from an application
to another. Topics act like a FIFO data structure (First In First
Out). The application that sends data to a topic is named the
**producer**, and the application that collects data from a topics is
named the **consumer**.

Data that flows through a topic are named **messages** and are
composed of:
* a key
* a value
* headers (or metadata, optional)
* other technical metadata used by Kafka (timestamp, offset, flags,
  checksum...)

A topic is divided into **partitions**. There might have at least 1
partition for a topic, and you can have, for example, 16, 20, 64
partitions for a topic. Each partition is allocated to a Kafka broker,
which is a node on the Kafka cluster. And each partition are
replicated to other brokers.

When a producer sends a message to a topic, a hash is computed from
the key. Then, the message is sent to the partition corresponding to
the hash value, and replicated accordingly. You can have more than one
producer that sends data to the same topic.

A consumer needs to **subscribe** to a topic to **poll** data from it.
You might have many consumers polling the same topic. Consumers can
be grouped under one **group ID** or more. In fact, the group ID should
be understood as your application ID in the Kafka scope. All the
consumers, that share the same group ID are, in fact the different
instances of the same application. A **consumer group** (ie. a set of
consumers sharing the same group ID) are manage by one broker of the
Kafka cluster. This broker is named **coordinator** of the consumer
group.

After subscription, consumers are allocated to one or more partitions.
If you have less consumers than partitions, you will have more than one
partition by consumer. If you have as many consumers than partitions,
you will have exactly one partition by consumer. And if you have more
consumers than partitions, the additional consumers will not do
anything.

Every message that arrives at _a partition of a topic_ are indexed with
by an **offset**. An offset is like an index in a list. The first
message ever sent on a topic starts with the index 0. The next one has
the index 1, then 2, then 3, 4... On the consumer side, each consumer
group manages a set of current offset to consume by partition, located
in the corrdinator node. Once a message is consumed by a consumer, the
consumer updates the offset of the consumer group for the corresponding
partition. This update is sent to the coordinator.

#### => Exercise

Kafka comes with tools to explore the state of topics and consumer
groups (ie. consumer sharing the same group ID).

* Display all the topics available

```shell
$ ./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

* Display details about one topic

```shell
$ ./bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --topic stock-info --describe
```

How many partitions and replicas is there in this topic?

* Display the list of consumer groups

```shell
$ ./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --list
```

* Display details about a consumer group

```shell
$ ./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group process --describe
```

Launch this command many times to follow the evolution of consumer
group statistics.

* Display messages from a topic

```shell
$ ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic stock-info
```

(Hit <Ctrl+C> to stop it)
