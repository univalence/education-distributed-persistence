package io.univalence.dataeng._06_kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes

import scala.jdk.CollectionConverters._
import scala.util.Using

/** We will first create a simple Kafka producer. */
object _01_producer {
  def main(args: Array[String]): Unit = {

    /** A Kafka producer needs to be configured. */
    val properties =
      Map[String, AnyRef](
        /**
         * Bootstrap servers are Kafka servers that are used to discover
         * all the rest of Kafka cluster topology. This property is list
         * of `[host]:[port]` pairs and/or URL-like values separated by
         * a comma (",").
         *
         * In this exercise, we only have one server on your machine.
         */
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "PLAINTEXT://localhost:9092"
      ).asJava

    /**
     * A Kafka producer is an interface to send data to a topic. The
     * data are key-value pairs. They are typed. And you need to provide
     * serializers for the keys and for the values.
     */
    val producer =
      new KafkaProducer[String, String](
        properties,
        Serdes.String().serializer(),
        Serdes.String().serializer()
      )

    Using(producer) { producer =>
      /**
       * To send a message, you need to create a record. A record should
       * at least contain the name of the topic, a key and a value (you
       * can omit the key, but in most the use case, this is not
       * recommended).
       *
       * It is possible to set some more metadata:
       *   - the partition number in the topic
       *   - the timestamp of the message
       *   - a set of headers (ie. key-value pairs) that will be used
       *     for your own purpose
       */
      val record =
        new ProducerRecord[String, String](
          "message",
          "Mary",
          "hello"
        )

      println(s"sending $record")

      /**
       * Sends the record to Kafka. This call is asynchrone. To
       * synchronize it, we use the `get` method.
       */
      val metadata = producer.send(record).get

      println(s"message sent!")

      /**
       * The metadata contains the topic and the partition on which the
       * message has been sent. It also have the offset of message in
       * the partition.
       *
       * The offset can be seen as an index in a list. It starts at 0.
       * And you run this program again, you will see that this offset
       * increase.
       */
      println(s"metadata of the sending: $metadata")
    }.get
  }
}

object _02_consumer {
  def main(args: Array[String]): Unit = {
    val properties =
      Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "PLAINTEXT://localhost:9092",
        ConsumerConfig.GROUP_ID_CONFIG          -> "consumer-group-1"
      ).asJava

    val consumer =
      new KafkaConsumer[String, String](
        properties,
        Serdes.String().deserializer(),
        Serdes.String().deserializer()
      )

    Using(consumer) { consumer =>
      consumer.subscribe(List("message").asJava)
      while (true) {
        val records = consumer.poll(java.time.Duration.ofSeconds(5)).asScala
        for (record <- records)
          println(record)
      }
    }.get
  }
}

object _03_admin {
  def main(args: Array[String]): Unit = {}
}
