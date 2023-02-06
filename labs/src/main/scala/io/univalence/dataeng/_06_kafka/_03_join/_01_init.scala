package io.univalence.dataeng._06_kafka._03_join

import io.univalence.dataeng.internal.exercise_tools.section
import io.univalence.dataeng.internal.kafka_utils.createTopics


object _01_init {

  val venueTopic = "venue"
  val checkinTopic = "checkin"
  val venueCheckinTopic = "venue-checkin"
  val bootstrapServers = "localhost:9092"

  def main(args: Array[String]): Unit =
    section("Create topics") {
      createTopics(Set(venueTopic, checkinTopic, venueCheckinTopic))(bootstrapServers)
    }

}
