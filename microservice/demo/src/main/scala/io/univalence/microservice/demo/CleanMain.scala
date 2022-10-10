package io.univalence.microservice.demo

import com.datastax.oss.driver.api.core.CqlSession
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}

import io.univalence.microservice.common.args._

import scala.util.Using

import java.net.InetSocketAddress

object CleanMain {

  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    val parameters = readArgs(args.toList).toMap

    val cassandraPort  = parameters.get("cassandra.port").flatMap(_.map(_.toInt)).getOrElse(Configuration.CassandraPort)
    val kafkaBootstrap = parameters.get("kafka.servers").flatten.getOrElse(Configuration.KafkaBootstrap)
    val stockInfoTopic = parameters.get("topic").flatten.getOrElse(Configuration.StockInfoTopic)

    println("--> Clean Cassandra")
    Using(
      CqlSession
        .builder()
        .addContactPoint(new InetSocketAddress(cassandraPort))
        .withLocalDatacenter("datacenter1")
        .build()
    ) { session =>
      println(s"Delete keyspace ${Configuration.StoreKeyspace}...")
      session.execute(s"DROP KEYSPACE ${Configuration.StoreKeyspace}")
    }.fold(
      e => println(s"Error: ${e.getMessage}"),
      _ => ()
    )

    println("--> Clean Kafka")
    Using(
      AdminClient.create(
        Map[String, AnyRef](
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBootstrap
        ).asJava
      )
    ) { admin =>
      val topics = admin.listTopics().names().get().asScala

      if (topics.contains(stockInfoTopic)) {
        println(s"Topic $stockInfoTopic exists")
        println(s"Delete topic $stockInfoTopic...")
        admin.deleteTopics(List(stockInfoTopic).asJava).all().get()
      } else {
        println(s"Topic $stockInfoTopic does not exist. Do nothing")
      }
    }
  }

}
