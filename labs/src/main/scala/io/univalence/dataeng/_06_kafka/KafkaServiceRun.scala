package io.univalence.dataeng._06_kafka

import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

/**
 * This program will help you to run a Kafka service.
 *
 * It is based on the library ''testcontainers'', which is a Java
 * library that manage the Docker service, and thus images and
 * containers.
 *
 * You just to launch the program and let it run. To stop it, just it
 * `<Ctrl>+C` or click on the stop button.
 *
 * Kafka service is mapped on port 9092 on localhost.
 */
object KafkaServiceRun {

  def main(args: Array[String]): Unit = {
    val kafka: KafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))

    sys.addShutdownHook(kafka.stop())

    kafka.setPortBindings(List("9092:9093").asJava)
    kafka.start()

    println(s"Bootstrap servers: ${kafka.getBootstrapServers}")

    Await.ready(Future.never, scala.concurrent.duration.Duration.Inf)
  }

}
