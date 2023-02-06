package io.univalence.dataeng._06_kafka._02_services

import com.google.gson._
import io.univalence.dataeng.internal.exercise_tools.section
import io.univalence.dataeng.internal.utils.using
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes

import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.jdk.CollectionConverters._


object _04_producer {
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
