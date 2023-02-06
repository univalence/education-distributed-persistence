package io.univalence.dataeng._06_kafka._02_services

import com.google.gson.{JsonElement, JsonParser}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes

import io.univalence.dataeng._06_kafka.Database
import io.univalence.dataeng.internal.exercise_tools.{comment, section}
import io.univalence.dataeng.internal.utils.using

import java.time.Duration

/**
 * This object is where you have to modify and run your code.
 *
 * This object represent a full service that converts data from the
 * topic "message" to put the result in the topic "transformed-message".
 *
 * Your goal is to modify the content of the
 * [[_03_your_service.services]].
 *
 * The service has an in-memory database, to use only if it is
 * necessary. In this database, you can create tables, and use CRUD
 * operations on those tables. The content of the database is accessible
 * through a Rest API on port 18080 with the path `data\<table_name>`.
 */
object _03_your_service {

  import spark.Spark._

  import io.univalence.dataeng.internal.elasticsearch_utils.Json

  import scala.jdk.CollectionConverters._

  val groupId = "service"
  val apiPort = 18080

  /**
   * We create a local in-memory database, to use only if it is
   * necessary. See [[Database]] for more documentation.
   */
  val database = new Database

  /**
   * This a type alias in Scala. It says that the type `Service` is
   * equivalent to the type of function that takes in parameter a key as
   * a String and a value as a parsed JSON document, and that returns
   * (`=>`) a transformed key as a String and a transformed value as a
   * String.
   */
  type Service = (String, JsonElement) => (String, String)

  /** This main launches the service. */
  def main(args: Array[String]): Unit =
    section("Your service") {
      // setup API Rest
      port(apiPort)
      get("/data/", internal.getTables(database, "/data"))
      get("/data/:table", internal.getData(database))

      // TODO change the string parameter below for another exercise
      pipeline(services("exercise-5"))
    }

  val services: Map[String, Service] =
    Map(
      "exercise-1" -> { (key, value) =>
        /**
         * Boilerplate
         *
         * TODO Just extract the `text` field
         *
         * The data are JSON that contain a field `text` at upper level.
         *
         * Note: a key-value pair can be represented in Scala with the
         * syntax `key_part -> value_part`.
         */
        val result: String = ???

        key -> result
      },
      "exercise-2" -> { (key, value) =>
        /**
         * To upper case
         *
         * TODO Extract the `text` field and convert it to uppercase.
         *
         * Use the method `String.toUpperCase`.
         */
        val result: String = ???

        key -> result
      },
      "exercise-3" -> { (key, value) =>
        /**
         * Filter
         *
         * TODO Only emit messages in English (lang=en)
         *
         * If you return `null` instead of a string, the message will
         * not be sent.
         */
        // Extract the `lang` field
        val lang: String = ???
        // Extract the `text` field
        val text: String = ???

        if (lang == ???)
          ???
        else
          ???
      },
      "exercise-4" -> { (key, value) =>
        /**
         * Count
         *
         * TODO count the number of tweets in each lang.
         */

        /** Create a table where the values are integers. */
        val countTable = database.getOrCreateTable[Int]("count")

        // Extract the `lang` field
        val lang: String = ???

        /**
         * Use the `getWithDefault` method to initialize and/or update
         * the count of tweets for the current lang.
         */
        val newValue: Int = countTable.getWithDefault(lang, 0) + 1

        countTable.put(lang, newValue)

        lang -> newValue.toString
      },
      "exercise-5" -> { (key, value) =>
        // TODO send the longest tweets that contains `NoSQL` for each user.

        val nosqlTable = database.getOrCreateTable[String]("nosql-max")

        // Extract the `text` field
        val text: String = ???

        // check that the text contains the good keyword
        if (text.toLowerCase.contains(???)) {
          // add and/or update nosqlTqble with the longest tweet

          key -> nosqlTable.get(key)
        } else {
          key -> null
        }
      }
    )

  /** Build the service pipeline integrating your service pipeline. */
  def pipeline(service: Service): Unit = {
    val consumerProperties =
      Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> _01_init.bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG          -> groupId,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
      ).asJava

    val producerProperties =
      Map[String, AnyRef](
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> _01_init.bootstrapServers
      ).asJava

    using(
      new KafkaConsumer[String, String](
        consumerProperties,
        Serdes.String().deserializer(),
        Serdes.String().deserializer()
      )
    ) { consumer =>
      consumer.subscribe(List(_01_init.messageTopic).asJava)
      comment(s"consumer has subscribed to ${_01_init.messageTopic}")

      using(
        new KafkaProducer[String, String](
          producerProperties,
          Serdes.String().serializer(),
          Serdes.String().serializer()
        )
      ) { producer =>
        comment("Waiting for new messages to collect forever (hit <Ctrl+C> to stop)")
        while (true) {
          val consumerRecords = consumer.poll(Duration.ofSeconds(5)).asScala

          for (consumerRecord <- consumerRecords)
            processRecord(consumerRecord, service, producer)
        }
      }
    }
  }

  def processRecord(
      consumerRecord: ConsumerRecord[String, String],
      service: Service,
      producer: KafkaProducer[String, String]
  ): Unit = {
    comment(s"Processing message key=${consumerRecord.key()} value=")
    println(consumerRecord.value().replaceAll("\n", "\\\\n"))
    val (newKey, newValue) = service(consumerRecord.key(), JsonParser.parseString(consumerRecord.value()))
    comment(s">>> New value:")
    println(Option(newValue).map(_.replaceAll("\n", "\\\\n")).getOrElse("<null>"))

    Option(newValue).foreach { value =>
      sendValue(producer, _01_init.transformedMessageTopic, newKey, value)
      comment(">>> New value sent")
    }
  }

  def sendValue(producer: KafkaProducer[String, String], topic: String, key: String, value: String): Unit = {
    val producerRecord = new ProducerRecord[String, String](topic, key, value)

    producer.send(producerRecord).get()
  }
}
