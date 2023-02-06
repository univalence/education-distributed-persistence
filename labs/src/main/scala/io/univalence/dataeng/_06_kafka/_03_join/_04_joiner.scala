package io.univalence.dataeng._06_kafka._03_join

import com.google.gson.{Gson, JsonObject, JsonParser}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes
import spark.Spark._

import io.univalence.dataeng._06_kafka._03_join._04_joiner.database
import io.univalence.dataeng._06_kafka.Database
import io.univalence.dataeng.internal.utils.using

import java.time.{Duration, Instant}

object _04_joiner {
  import io.univalence.dataeng.internal.elasticsearch_utils.Json

  import scala.jdk.CollectionConverters._

  val groupId = "joiner"

  val apiPort = 18080

  val database = new Database

  val leftInputTopic  = _01_init.venueTopic
  val rightInputTopic = _01_init.checkinTopic
  val outputTopic     = _01_init.venueCheckinTopic

  val internalTopic  = "join-shuffle"
  val leftStoreName  = "left-store"
  val rightStoreName = "right-store"

  val leftStore: database.Table[String]  = database.getOrCreateTable[String](leftStoreName)
  val rightStore: database.Table[String] = database.getOrCreateTable[String](rightStoreName)

  def main(args: Array[String]): Unit = {
    port(apiPort)
    get("/data/", internal.getTables(database, "/data"))
    get("/data/:table", internal.getData(database))

    val consumerProperties =
      Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
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
      consumer.subscribe(
        List(
          leftInputTopic,
          rightInputTopic,
          internalTopic
        ).asJava
      )

      using(
        new KafkaProducer[String, String](
          producerProperties,
          Serdes.String().serializer(),
          Serdes.String().serializer()
        )
      ) { producer =>
        while (true) {
          val consumerRecords = consumer.poll(Duration.ofSeconds(5)).asScala

          for (consumerRecord <- consumerRecords) {
            val producerRecord: Option[ProducerRecord[String, String]] =
              consumerRecord.topic() match {
                case `leftInputTopic` =>
                  Some(
                    new ProducerRecord[String, String](
                      internalTopic,
                      consumerRecord.key(),
                      s"""{"side":"left","data":${consumerRecord.value()}}"""
                    )
                  )
                case `rightInputTopic` =>
                  Some(
                    new ProducerRecord[String, String](
                      internalTopic,
                      consumerRecord.key(),
                      s"""{"side":"right","data":${consumerRecord.value()}}"""
                    )
                  )
                case `internalTopic` =>
                  val value = JsonParser.parseString(consumerRecord.value())

                  val side    = (value / "side").getAsString
                  val key     = consumerRecord.key()
                  val data    = value / "data"
                  val dataStr = new Gson().toJson(data)

                  side match {
                    case "left" =>
                      updateStore1AndJoinStore2(leftStore, rightStore, key, dataStr, outputTopic)
                    case "right" =>
                      updateStore1AndJoinStore2(rightStore, leftStore, key, dataStr, outputTopic)
                    case otherSide =>
                      throw new IllegalStateException(s"unknown side $otherSide")
                  }

                case otherTopic =>
                  throw new IllegalStateException(s"unknown topic $otherTopic")
              }

            producerRecord.foreach(record => producer.send(record).get())
          }
        }
      }
    }
  }

  def updateStore1AndJoinStore2(
      store1: database.Table[String],
      store2: database.Table[String],
      key: String,
      data: String,
      outputTopic: String
  ): Option[ProducerRecord[String, String]] = {
    store1.put(key, data)
    if (store2.hasKey(key)) {
      val store2Data = store2.get(key)
      Some(
        new ProducerRecord[String, String](
          outputTopic,
          key,
          join(data, store2Data)
        )
      )
    } else {
      None
    }
  }

  def join(leftData: String, rightData: String): String = {
    val leftJson  = JsonParser.parseString(leftData)
    val rightJson = JsonParser.parseString(rightData)

    val result = join(Venue.fromJson(leftJson), Checkin.fromJson(rightJson))

    result.toJson
  }
  def join(venue: Venue, checkin: Checkin): VenueCheckin =
    VenueCheckin(
      userID    = checkin.userID,
      venueId   = venue.id,
      latitude  = venue.latitude,
      longitude = venue.longitude,
      venueType = venue.venueType,
      country   = venue.country,
      timestamp = checkin.timestamp,
      offset    = checkin.offset
    )

  case class VenueCheckin(
      userID:    String,
      venueId:   String,
      latitude:  Double,
      longitude: Double,
      venueType: String,
      country:   String,
      timestamp: Instant,
      offset:    Int
  ) {
    def toJson: String = {
      val json = new JsonObject
      json.addProperty("userID", userID)
      json.addProperty("venueId", venueId)
      json.addProperty("latitude", latitude)
      json.addProperty("longitude", longitude)
      json.addProperty("venueType", venueType)
      json.addProperty("country", country)
      json.addProperty("timestamp", timestamp.toEpochMilli)
      json.addProperty("offset", offset)

      val gson = new Gson()
      gson.toJson(json)
    }
  }

}
