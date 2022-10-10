package io.univalence.microservice.demo

import com.datastax.oss.driver.api.core.CqlSession
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import io.univalence.microservice.common.args.readArgs

import scala.util.{Try, Using}

import java.net.InetSocketAddress

object InitMain {

  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    val parameters = readArgs(args.toList).toMap

    val cassandraPort  = parameters.get("cassandra.port").flatMap(_.map(_.toInt)).getOrElse(Configuration.CassandraPort)
    val kafkaBootstrap = parameters.get("kafka.servers").flatten.getOrElse(Configuration.KafkaBootstrap)
    val partitions = parameters.get("kafka.partitions").flatMap(_.map(_.toInt)).getOrElse(Configuration.KafkaPartition)
    val stockInfoTopic = parameters.get("topic").flatten.getOrElse(Configuration.StockInfoTopic)

    val keyspace: String = Configuration.StoreKeyspace
    val table: String    = Configuration.StockTable

    println("--> Prepare Cassandra")
    Using(
      CqlSession
        .builder()
        .addContactPoint(new InetSocketAddress(cassandraPort))
        .withLocalDatacenter("datacenter1")
        .build()
    ) { session =>
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
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBootstrap
        ).asJava
      )
    ) { admin =>
      val nodes             = admin.describeCluster().nodes().get().asScala
      val replicationFactor = nodes.size.toShort
      println(s"Kafka cluster size: $replicationFactor")

      val topics = admin.listTopics().names().get().asScala

      if (topics.contains(stockInfoTopic)) {
        println(s"Topic $stockInfoTopic already exists")
        println(s"Delete topic $stockInfoTopic...")
        admin.deleteTopics(List(stockInfoTopic).asJava).all().get()
      }

      println(s"Creating topic $stockInfoTopic...")
      val newTopic = new NewTopic(stockInfoTopic, partitions, replicationFactor)
      admin.createTopics(List(newTopic).asJava).all().get()
    }
  }

}
