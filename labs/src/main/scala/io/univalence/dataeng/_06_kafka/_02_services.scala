package io.univalence.dataeng._06_kafka

import com.google.gson.{Gson, JsonArray, JsonElement, JsonObject, JsonParser, JsonPrimitive}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes
import spark.Route

import io.univalence.dataeng.internal.exercise_tools.{comment, section}
import io.univalence.dataeng.internal.kafka_utils._
import io.univalence.dataeng.internal.utils.using

import scala.io.Source
import scala.jdk.CollectionConverters._

import java.io.FileInputStream
import java.time.Duration
import java.util.zip.GZIPInputStream

/**
 * In this file, you will have to run 3 services.
 *
 * A service is kind of application that runs forever.
 *
 * A service might be the frontend of an e-commerce website (eg.
 * voyages-sncf)...
 *
 * {{{
 *   [Producer] -- message -> [Your Service (consumer / producer)] -- transformed-message -> [End consumer]
 * }}}
 *
 * First, execute [[_01_init.main]] only once to create necessary
 * topics.
 *
 * Modify [[_03_processing_your_service.services]] one at a time. Then
 * modify [[_03_processing_your_service.main]] to select one of the
 * exercise from [[_03_processing_your_service.services]].
 *
 * Then:
 *   1. Execute [[_02_processing_end_consumer.main]]
 *   1. Execute [[_03_processing_your_service.main]]
 *   1. Execute [[_04_processing_producer.main]]
 *
 * If something is not satisfying or if you have finished an exercise,
 * then stop all executed main and fix the problem or go to the next
 * exercise.
 *
 * Repeat until all the exercise are done.
 */
object _01_init {

  val messageTopic            = "tweets"
  val transformedMessageTopic = "transformed-tweets"
  val bootstrapServers        = "localhost:9092"

  def main(args: Array[String]): Unit =
    section("Create topics") {
      createTopics(Set(messageTopic, transformedMessageTopic))(bootstrapServers)
    }

}

object _02_processing_end_consumer {

  val groupId = "end-consumer"

  def main(args: Array[String]): Unit =
    section("End consumer") {
      val properties =
        Map[String, AnyRef](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> _01_init.bootstrapServers,
          ConsumerConfig.GROUP_ID_CONFIG          -> groupId,
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
        ).asJava

      using(
        new KafkaConsumer[String, String](
          properties,
          Serdes.String().deserializer(),
          Serdes.String().deserializer()
        )
      ) { consumer =>
        consumer.subscribe(List(_01_init.transformedMessageTopic).asJava)
        comment(s"consumer has subscribed to ${_01_init.transformedMessageTopic}")

        comment("Waiting for new messages to collect forever (hit <Ctrl+C> to stop)")
        while (true) {
          val records = consumer.poll(Duration.ofSeconds(5)).asScala

          records.foreach(record => println(displayRecord(record)))
        }
      }
    }

}

object _03_processing_your_service {

  import spark.{Request, Response}
  import spark.Spark._

  import io.univalence.dataeng.internal.elasticsearch_utils.Json

  val groupId = "service"

  val database = new Database

  /**
   * This a type alias in Scala. It says that the type `Service` is
   * equivalent to the type of function that takes in parameter a key as
   * a String and a value as a parsed JSON document, and that returns
   * (`=>`) a transformed value as a String.
   */
  type Service = (String, JsonElement) => (String, String)

  def main(args: Array[String]): Unit =
    section("Your service") {
      port(18080)
      get("/data/:table", getData)

      pipeline(services("exercise-5"))
    }

  val services: Map[String, Service] =
    Map(
      "exercise-1" -> { (key, value) =>
        /**
         * Boilerplate
         *
         * TODO Just extract the `text` field
         */
        key -> (value / "text").getAsString
      },
      "exercise-2" -> { (key, value) =>
        /**
         * To upper case
         *
         * TODO Extract the `text` field and convert it to uppercase.
         *
         * Use the method `String.toUpperCase`.
         */
        key -> (value / "text").getAsString.toUpperCase
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
        val lang = (value / "lang").getAsString
        val text = (value / "text").getAsString

        if (lang == ???)
          ???
        else
          ???
      },
      "exercise-4" -> { (key, value) =>
        val countTable = database.getOrCreateTable[Int]("count")
        println(countTable)

        val lang = (value / "lang").getAsString

        if (!countTable.hasKey(lang))
          countTable.put(lang, 1)
        else {
          val previousValue = countTable.get(lang)
          countTable.put(lang, previousValue + 1)
        }

        lang -> countTable.get(lang).toString
      },
      "exercise-5" -> { (key, value) =>
        /** TODO send the longest tweets that contains `NoSQL` */
        val nosqlTable = database.getOrCreateTable[String]("nosql-max")
        val text       = (value / "text").getAsString

        if (text.toLowerCase.contains("nosql")) {
          if (!nosqlTable.hasKey(key)) {
            nosqlTable.put(key, text)
          } else {
            val previousText = nosqlTable.get(key)
            if (previousText.length < text.length) {
              nosqlTable.put(key, text)
            }
          }

          key -> nosqlTable.get(key)
        } else {
          key -> null
        }
      }
    )

  val getData: Route = { (request: Request, response: Response) =>
    val tableName = request.params("table")

    val json =
      database
        .getOrCreateTable[Any](tableName)
        .getAll
        .foldLeft(new JsonObject) { case (json, (key, value)) =>
          json.add(key, new JsonPrimitive(value.toString))
          json
        }

    response.`type`("application/json")
    new Gson().toJson(json)
  }

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

          for (consumerRecord <- consumerRecords) {
            comment(s"Processing message key=${consumerRecord.key()} value=")
            println(consumerRecord.value().replaceAll("\n", "\\\\n"))
            val (newKey, newValue) = service(consumerRecord.key(), JsonParser.parseString(consumerRecord.value()))
            comment(s">>> New value:")
            println(Option(newValue).map(_.replaceAll("\n", "\\\\n")).getOrElse("<null>"))

            Option(newValue).foreach { value =>
              sendValue(producer, newKey, value)
              comment(">>> New value sent")
            }
          }
        }
      }
    }
  }

  def sendValue(producer: KafkaProducer[String, String], key: String, value: String): Unit = {
    val producerRecord =
      new ProducerRecord[String, String](
        _01_init.transformedMessageTopic,
        key,
        value
      )

    producer.send(producerRecord).get()
  }
}

object _04_processing_producer {
  import io.univalence.dataeng.internal.elasticsearch_utils.Json

  def main(args: Array[String]): Unit =
    section("Producer") {
      val properties =
        Map[String, AnyRef](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> _01_init.bootstrapServers
        ).asJava

      using(
        new KafkaProducer[String, String](
          properties,
          Serdes.String().serializer(),
          Serdes.String().serializer()
        )
      ) { producer =>
        using(
          Source.fromInputStream(
            new GZIPInputStream(
              new FileInputStream(
                "data/twitter/tweets.json.gz"
              )
            )
          )
        ) { file =>
          for (line <- file.getLines()) {
            Thread.sleep(2000)
            val tweet = JsonParser.parseString(line)
            val user  = (tweet / "user" / "screen_name").getAsString
            val text  = (tweet / "text").getAsString.replaceAll("\"", "\\\\\"")
            val lang  = (tweet / "lang").getAsString

            val value = s"""{"user": "$user", "text": "$text", "lang": "$lang"}"""

            val record =
              new ProducerRecord[String, String](
                _01_init.messageTopic,
                user,
                value
              )

            producer.send(record).get()
            println(s"Send key=$user value=${text.replaceAll("\n", "\\\\n")}")
          }
        }
      }
    }
}

class Database {

  class Table[A](name: String) {
    val data: scala.collection.mutable.Map[String, A] = scala.collection.mutable.Map.empty

    def put(key: String, value: A): Unit = data.update(key, value)

    def get(key: String): A = data(key)

    def getAll: Map[String, A] = Map.from(data)

    def delete(key: String): Unit = data.remove(key)

    def hasKey(key: String): Boolean = data.keySet.contains(key)
  }

  val data: scala.collection.mutable.Map[String, Table[Any]] = scala.collection.mutable.Map.empty

  def getOrCreateTable[A](name: String): Table[A] = {
    if (!data.contains(name))
      data.update(name, new Table(name))

    data(name).asInstanceOf[Table[A]]
  }

  def getAll: Map[String, Map[String, Any]] = Map.from(data.map { case (name, table) => name -> table.getAll })

}
