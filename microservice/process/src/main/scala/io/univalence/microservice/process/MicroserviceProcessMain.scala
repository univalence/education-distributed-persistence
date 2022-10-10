package io.univalence.microservice.process

import com.datastax.oss.driver.api.core.CqlSession
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import io.univalence.microservice.common.args.readArgs
import io.univalence.microservice.common.entity.{ProjectedStock, StockInfo, StockInfoJson}
import io.univalence.microservice.common.repository.ProjectedStockRepository
import io.univalence.microservice.common.repository.impl.CassandraProjectedStockRepository

import java.net.InetSocketAddress

object MicroserviceProcessMain {

  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    val parameters: Map[String, Option[String]] = readArgs(args.toList).toMap
    val kafkaBootstrap = parameters.get("kafka.servers").flatten.getOrElse(Configuration.KafkaBootstrap)
    val cassandraPort  = parameters.get("cassandra.port").flatMap(_.map(_.toInt)).getOrElse(Configuration.CassandraPort)
    val kafkaGroup        = parameters.get("kafka.group").flatten.getOrElse(Configuration.KafkaGroup)
    val stockInfoTopic    = parameters.get("topic").flatten.getOrElse(Configuration.StockInfoTopic)

    val session =
      CqlSession
        .builder()
        .addContactPoint(new InetSocketAddress(cassandraPort))
        .withLocalDatacenter("datacenter1")
        .build()

    val repository: ProjectedStockRepository =
      new CassandraProjectedStockRepository(
        session,
        Configuration.CassandraKeyspace,
        Configuration.CassandraTable
      )

    // TODO create a consumer and subscribe to Kafka topic

    val consumer =
      new KafkaConsumer[String, String](
        Map[String, AnyRef](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBootstrap,
          ConsumerConfig.GROUP_ID_CONFIG          -> kafkaGroup,
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
        ).asJava,
        new StringDeserializer,
        new StringDeserializer
      )

    consumer.subscribe(List(stockInfoTopic).asJava)

    while (true) {
      val stockInfos: List[StockInfo] = nextStockInfos(consumer)

      val newStocks: List[ProjectedStock] =
        stockInfos.map { stock =>
          val projectedStock =
            repository
              .findById(stock.id)
              .getOrElse(emptyProjectedStockFor(stock.id))

          aggregateWithStock(stock, projectedStock)
        }

      repository.saveAll(newStocks)
    }
  }

  def aggregateWithStock(
      stockInfo: StockInfo,
      projectedStock: ProjectedStock
  ): ProjectedStock =
    if (stockInfo.stockType == StockInfo.STOCK)
      ProjectedStock(
        id        = stockInfo.id,
        timestamp = stockInfo.timestamp,
        quantity  = stockInfo.quantity
      )
    else
      projectedStock.copy(quantity = projectedStock.quantity + stockInfo.quantity)

  def emptyProjectedStockFor(id: String): ProjectedStock =
    ProjectedStock(
      id        = id,
      timestamp = 0L,
      quantity  = 0
    )

  def nextStockInfos(consumer: KafkaConsumer[String, String]): List[StockInfo] = {
    val records: Iterable[ConsumerRecord[String, String]] = consumer.poll(java.time.Duration.ofSeconds(5)).asScala

    records.map { record =>
      println(s"Got record: $record")
      val doc = record.value()
      StockInfoJson.deserialize(doc)
    }.toList
  }

}
