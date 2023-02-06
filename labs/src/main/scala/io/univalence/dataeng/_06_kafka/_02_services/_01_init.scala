package io.univalence.dataeng._06_kafka._02_services

import io.univalence.dataeng.internal.exercise_tools.section
import io.univalence.dataeng.internal.kafka_utils.createTopics


object _01_init {

  val messageTopic            = "tweets"
  val transformedMessageTopic = "transformed-tweets"
  val bootstrapServers        = "localhost:9092"

  def main(args: Array[String]): Unit =
    section("Create topics") {
      createTopics(Set(messageTopic, transformedMessageTopic))(bootstrapServers)
    }

}
