package io.univalence.dataeng._06_kafka._03_join

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes

import io.univalence.dataeng.internal.utils.using

object _03_venue_producer {

  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
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
      Venue.loopOverCSVFile("data/foursquare/venues.txt.gz") { venue =>
        val record =
          new ProducerRecord[String, String](
            _01_init.venueTopic,
            venue.id,
            venue.toJson
          )

        println(s"Sending $venue")
        producer.send(record).get()
      }
    }
  }

}
