package io.univalence.dataeng._06_kafka

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ListTopicsOptions, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.serialization.Serdes

import io.univalence.dataeng.internal.exercise_tools._
import io.univalence.dataeng.internal.kafka_utils.{
  consumerGroupToEarliest,
  displayConsumerGroupOffset,
  displayRecord,
  render
}
import io.univalence.dataeng.internal.utils.{retry, using}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import java.time.Duration

/**
 * =Kafka=
 *
 * Apache Kafka is a different kind of service, compared to Cassandra or
 * Elasticsearch. First, it is not a database, but a streaming platform.
 * The main goal of Kafka is first to ensure that messages sent from
 * some applications are received to other applications, without losing
 * a single message. It is not to store and retrieve data, even if Kafka
 * has such a functionality.
 *
 * In this context, we cannot conceive the exercise like the ones with
 * Cassandra and Elasticsearch, or any kind of databases, where we have
 * a single application communicating with such a database. Here, in the
 * exercices below, we need to have at least 2 separated applications:
 * one that produces messages and one that consumes those messages.
 *
 * But first, as message transmission goes through objects, named
 * topics, we have to defined them.
 */
object _01_manage_kafka {
  def main(args: Array[String]): Unit =
    section("Manage Kafka") {

      val properties =
        Map[String, AnyRef](
          /**
           * Bootstrap servers are Kafka servers that are used to
           * discover all the rest of Kafka cluster topology. This
           * property is list of `[host]:[port]` pairs and/or URL-like
           * values separated by a comma (","). Just one node is
           * sufficient to discover the rest of the Kafka cluster. But,
           * it is also a good idea to have more hosts in the list,
           * especially if one of them have a failure.
           */
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
        ).asJava

      using(AdminClient.create(properties)) { admin =>
        exercise_ignore("Check the Kafka cluster") {
          val cluster = admin.describeCluster()

          comment(s"Cluster ID: ${cluster.clusterId().get()}")
          val nodes = cluster.nodes().get().asScala
          for ((node, i) <- nodes.zipWithIndex)
            comment(s"Node#${i + 1}: id=${node.id()} - host=${node.host()}:${node.port()}")

          comment(s"How many nodes has your cluster?")
          check(nodes.size == ??)
        }

        exercise_ignore("Create a topic") {

          /** We want to create a topic with this name: */
          val topicName = "message"

          /**
           * It is a good idea to fix the number of replica to 3. But if
           * you do not have enough node to ensure replication on
           * separate nodes, its better to fix the number of replica on
           * the number of available nodes.
           */
          val nodeCount = Math.min(3, admin.describeCluster().nodes().get().size())

          /** Here, we prepare the topic creation. */
          val newTopic =
            new NewTopic(
              // this is the topic name
              topicName,
              // this is the number of partitions to set in the topic
              4,
              // this the number of replica
              nodeCount.toShort
            )

          /**
           * ''Note: about partition number.''
           *
           * Through the partition number, you represent the capacity of
           * parallelization of your system. It the number of partition
           * is 1, you will not be able to parallelize your processes.
           * If the number of partition is 2, you can have 2 consumers
           * working in parallel on the topic. So the partition number
           * represents your maximum capacity of parallelization.
           *
           * It is good to have enough partitions, even if you start
           * with just one consumer. You can increase the number of
           * partitions later, but modifying this number is not always
           * compatible with all system based on Kafka (eg. Kafka
           * Streams).
           *
           * It your have a not so heavy process, you can match the
           * number of partitions with the number of CPU cores. In case
           * of heavy processes, where performance is needed, you go up
           * to 32, 64, or 128 to the extents (higher number of
           * partitions is not that efficient). Prefer to use a power of
           * 2 to choose this number, so it makes resource allocation
           * planing easier to think about.
           */

          /**
           * Topic creation really happens here. As you can see, you can
           * create a list of topic in one instruction.
           */
          admin.createTopics(List(newTopic).asJava).all()

          /**
           * Let's check if the topic has been created. To do that, we
           * get the list of topics and find in this list the one we
           * have created.
           *
           * We have to let Kafka create the topic, as it may take some
           * time.
           */
          val Some(topics) =
            retry(retryCount = 5, delayMs = 1000) {
              admin.listTopics(new ListTopicsOptions).names().get().asScala
            }(topics => topics.contains(topicName))

          check(topics.contains(topicName))
        }

        exercise_ignore("Check created topic") {

          /**
           * You can get different information from a topic. Especially,
           * the list of partitions and which brokers manage it.
           */
          val description = admin.describeTopics(List("message").asJava).allTopicNames().get().get("message")
          val partitions: Map[Int, TopicPartitionInfo] =
            description
              .partitions()
              .asScala
              .map(tp => tp.partition() -> tp)
              .toMap

          comment("structure of topic message")
          partitions.values.toList.sortBy(_.partition()).foreach { tp =>
            val replicas = tp.replicas().asScala.map(_.id()).mkString(",")
            val isr      = tp.isr().asScala.map(_.id()).mkString(",")
            println(s"  partition ${tp.partition()}: leader=${tp.leader().id()} - replicas:$replicas - isr:$isr")
          }

          comment("Which is the node ID of the leader for the partition 1?")
          check(partitions(1).leader().id() == ??)
          comment("Which are node IDs of the replicas for partition 2?")
          check(partitions(2).replicas().asScala.map(_.id()) == List(??))
        }
      }
    }
}

/**
 * =Kafka Producer=
 *
 * We will first create a simple Kafka producer.
 */
object _02_producer {
  def main(args: Array[String]): Unit =
    section("Send messages with Kafka") {
      val topicName = "message"

      /** A Kafka producer needs to be configured. */
      val properties =
        Map[String, AnyRef](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
        ).asJava

      /**
       * A Kafka producer is an interface to send data to a topic. The
       * data are key-value pairs. They are typed. And you need to
       * provide serializers for the keys and for the values.
       */
      using(
        new KafkaProducer[String, String](
          properties,
          Serdes.String().serializer(),
          Serdes.String().serializer()
        )
      ) { producer =>
        exercise_ignore("Send a message") {

          /**
           * To send a message, you need to create a record. A record
           * should at least contain:
           *   - the '''name''' of the topic,
           *   - a '''key''' (you can omit the key by using `null`
           *     value, but in most the use cases, this is not
           *     recommended)
           *   - a '''value'''
           *
           * It is possible to set some more metadata, like the
           * partition number in the topic, the timestamp of the
           * message, a set of headers (ie. key-value pairs) that will
           * be used for your own purpose.
           */
          val record =
            new ProducerRecord[String, String](
              topicName,
              "Mary",
              "hello"
            )

          comment(s"sending $record")

          /**
           * Sends the record to Kafka. This call is asynchrone. To
           * synchronize it, we use the `get` method.
           */
          val metadata: RecordMetadata = producer.send(record).get

          /**
           * The metadata contains the topic and the partition on which
           * the message has been sent. It also have the offset of
           * message in the partition.
           *
           * The offset can be seen as an index in a list. It starts at
           * 0. And you run this program again, you will see that this
           * offset increase.
           */
          comment(s"Message sent: ${render(metadata)}")
          check(metadata.topic() == topicName && metadata.hasOffset)

          comment(s"In which partition the message has been sent?")
          check(metadata.partition() == ??)
          comment(s"To which offset the message has been put? (/!\\ you have to predict the next offset)")
          check(metadata.offset() == ??)

          /**
           * TODO try to execute this exercise again (even multiple
           * times). What happens to the offset?
           */
        }

        exercise_ignore("Send another message") {

          /**
           * TODO send another message, but this time by using `Jon` as
           * key
           */
          val record: ProducerRecord[String, String] = ???
          val metadata: RecordMetadata               = ???

          comment(s"Message sent: ${render(metadata)}")

          check(metadata.topic() == topicName && metadata.hasOffset)

          comment(s"In which partition the message has been sent?")
          check(metadata.partition() == ??)
          comment(s"To which offset the message has been put? (/!\\ you have to predict the next offset)")
          check(metadata.offset() == ??)
        }
      }
    }
}

/**
 * =Kafka consumer=
 *
 * In the previous program, we have put data in a topic with a Kafka
 * producer. Now, we will extract those data with a consumer in another
 * program. We will see that producer and consumer does not need to work
 * synchronously, because of the Kafka ability to hold data before they
 * are collected by the consumer.
 *
 * The Kafka consumer is the program that gets data from one topic or
 * more.
 */
object _03_consumer {

  val topicName = "message"
  val groupId   = "consumer-group-1"

  def main(args: Array[String]): Unit =
    section("Collect messages with Kafka") {

      /**
       * This is an internal method to force a consumer group to start a
       * the beginning of its topics.
       */
      consumerGroupToEarliest(bootstrap = "localhost:9092", group = groupId)

      val properties =
        Map[String, AnyRef](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",

          /**
           * Every consumer needs to be part of a named group, that is
           * managed at the broker level. In fact, the group is supposed
           * to be your application an each consumer is an instance of
           * that application that runs in parallel.
           *
           * Also, at the broker side, one broker is elected as the
           * group coordinator. It manages the offsets of the group for
           * each partition of each topic the group consumes. It also
           * checks that each consumer are available, and redistributed
           * remaining data (rebalance) if the number of consumers in
           * the group as changed.
           *
           * WARNING: it may happen that the exercises below fails, due
           * to the need to get the whole content of the topic to work.
           * Just change the group ID (eg. use `"consumer-group-2"`,
           * instead of `"consumer-group-1"`), and it might work again.
           */
          ConsumerConfig.GROUP_ID_CONFIG -> groupId,

          /**
           * If the consumer group is new, this parameter says where in
           * the topic it will start:
           *   - `latest`: the group will start at the end of the topic
           *     (highest offsets), and the consumers will only process
           *     new messages.
           *   - `earliest`: the group will start at the beginning of
           *     the topic (smallest offsets), and the consumers will
           *     have to process old message again.
           */
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
        ).asJava

      using(
        new KafkaConsumer[String, String](
          properties,
          Serdes.String().deserializer(),
          Serdes.String().deserializer()
        )
      ) { consumer =>
        exercise("Collect messages") {
          comment(s"Current offsets of the consumer group $groupId before collecting messages")
          displayConsumerGroupOffset(bootstrap = "localhost:9092", group = groupId)

          comment(s"Collecting messages from $topicName...")

          /**
           * The first thing to do with a consumer is to ask it to
           * subscribe to topics.
           */
          val topics = List(topicName).asJava
          consumer.subscribe(topics)

          val collectedRecords: ArrayBuffer[ConsumerRecord[String, String]] = ArrayBuffer.empty

          /**
           * Getting messages from topics you subscribed to, implies to
           * call the `poll` method.
           *
           * `poll` call is blocking and will wait for new messages
           * collect. If no messages are available (during a given
           * time), `poll` returns an empty content. If messages are
           * available, `poll` returns a collection of `ConsumerRecord`.
           */
          val records: Iterable[ConsumerRecord[String, String]] =
            consumer
              .poll(
                Duration.ofSeconds(5)
              )
              .asScala

          collectedRecords ++= records
          for (record <- records)
            println(displayRecord(record))

          var done = records.isEmpty
          while (!done) {
            val records = consumer.poll(Duration.ofSeconds(5)).asScala

            collectedRecords ++= records
            for (record <- records)
              println(displayRecord(record))

            done = records.isEmpty
          }

          comment("No more messages...")

          comment(s"New offsets of the consumer group $groupId after collecting messages")
          displayConsumerGroupOffset(bootstrap = "localhost:9092", group = groupId)

          println()

          comment("How many messages did you collect?")
          check(collectedRecords.size == ??)

          val jonPartitions = collectedRecords.filter(_.key() == "Jon").map(_.partition()).toSet
          comment("In how many partitions are the messages with key Jon?")
          check(jonPartitions.size == ??)

          val maryPartitions = collectedRecords.filter(_.key() == "Mary").map(_.partition()).toSet
          comment("In how many partitions are the messages with key Mary?")
          check(maryPartitions.size == ??)

          comment("Are Mary messages in a different partition than Jon messages? (true/false)")
          check((maryPartitions == jonPartitions) == ??)
        }
      }
    }
}
