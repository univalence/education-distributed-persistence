package io.univalence.microservice.demo

import com.datastax.oss.driver.api.core.CqlSession
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import scala.util.{Try, Using}

object InitMain {

  // Cassandra configuration
  val keyspace: String = Configuration.StoreKeyspace
  val table: String    = Configuration.StockTable

  // Kafka configuration
  val topic: String   = Configuration.StockInfoTopic
  val partitions: Int = 8

  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    println("--> Prepare Cassandra")
    Using(CqlSession.builder().build()) { session =>
      println(s"Delete keyspace $keyspace...")

      Try(session.execute(s"DROP KEYSPACE $keyspace")).getOrElse(())

      println(s"Create keyspace store...")
      session.execute(s"""CREATE KEYSPACE IF NOT EXISTS $keyspace
            | WITH REPLICATION = {
            |   'class': 'SimpleStrategy',
            |   'replication_factor': 1
            | }""".stripMargin)

      println(s"Create table store.stock...")
      session.execute(s"""CREATE TABLE IF NOT EXISTS $keyspace.$table (
            |  id TEXT,
            |  ts BIGINT,
            |  qtt INT,
            |
            |  PRIMARY KEY (id)
            |)""".stripMargin)
    }.get

    println("--> Prepare Kafka")
    Using(
      AdminClient.create(
        Map[String, AnyRef](
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> Configuration.KafkaBootstrap
        ).asJava
      )
    ) { admin =>
      val nodes             = admin.describeCluster().nodes().get().asScala
      val replicationFactor = nodes.size.toShort
      println(s"Kafka cluster size: $replicationFactor")

      val topics = admin.listTopics().names().get().asScala

      if (topics.contains(topic)) {
        println(s"Topic $topic already exists")
        println(s"Delete topic $topic...")
        admin.deleteTopics(List(topic).asJava).all().get()
      }

      println(s"Creating topic $topic...")
      val newTopic = new NewTopic(topic, partitions, replicationFactor)
      admin.createTopics(List(newTopic).asJava).all().get()
    }
  }

}
