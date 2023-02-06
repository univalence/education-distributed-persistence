package io.univalence.dataeng._06_kafka._02_services

import io.univalence.dataeng.internal.exercise_tools.{comment, section}
import io.univalence.dataeng.internal.kafka_utils.displayRecord
import io.univalence.dataeng.internal.utils.using
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.Serdes

import java.time.Duration

/** This program launch the end consumer. */
object _02_end_consumer {
  import scala.jdk.CollectionConverters._

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
