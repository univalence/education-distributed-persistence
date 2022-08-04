package io.univalence.microservice.process

import com.datastax.oss.driver.api.core.CqlSession
import io.univalence.microservice.common.entity.{ProjectedStock, StockInfo, StockInfoJson}
import io.univalence.microservice.common.repository.ProjectedStockRepository
import io.univalence.microservice.common.repository.impl.CassandraProjectedStockRepository
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

object MicroserviceProcessMain {

  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {

    // TODO instantiate a stock repository from a Cassandra session

    val session = CqlSession.builder().build()

    val repository: ProjectedStockRepository = new CassandraProjectedStockRepository(session)

    // TODO create a consumer and subscribe to Kafka topic

    val consumer =
      new KafkaConsumer[String, String](
        Map[String, AnyRef](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Configuration.KafkaBootstrap,
          ConsumerConfig.GROUP_ID_CONFIG          -> "process-3",
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
        ).asJava,
        new StringDeserializer,
        new StringDeserializer
      )

    consumer.subscribe(List(Configuration.StockInfoTopic).asJava)

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
                        ): ProjectedStock = {
    if (stockInfo.stockType == StockInfo.STOCK)
      ProjectedStock(
        id = stockInfo.id,
        timestamp = stockInfo.timestamp,
        quantity = stockInfo.quantity
      )
    else
      projectedStock.copy(quantity =
        projectedStock.quantity + stockInfo.quantity
      )
  }

  def emptyProjectedStockFor(id: String): ProjectedStock =
    ProjectedStock(
      id = id,
      timestamp = 0L,
      quantity = 0
    )

  def nextStockInfos(consumer: KafkaConsumer[String, String]): List[StockInfo] = {
    val records: Iterable[ConsumerRecord[String, String]] =
      consumer.poll(java.time.Duration.ofSeconds(5)).asScala

    records.map { record =>
      println(s"Got record: $record")
      val doc = record.value()
      StockInfoJson.deserialize(doc)
    }.toList
  }

}
