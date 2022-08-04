package io.univalence.microservice.ingest

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import spark.{Request, Response}
import spark.Spark._

import io.univalence.microservice.common.entity._

import java.time.Instant

object MicroserviceIngestMain {

  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    port(Configuration.IngestHttpPort)

    val producer: KafkaProducer[String, String] =
      new KafkaProducer[String, String](
        Map[String, AnyRef](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> Configuration.KafkaBootstrap
        ).asJava,
        new StringSerializer,
        new StringSerializer
      )

    post(
      "/stocks/:id",
      { (request: Request, response: Response) =>
        val id        = request.params("id")
        val body      = request.body()
        val timestamp = Instant.now().toEpochMilli
        println(s"--> Received@$timestamp: id: $id - data: $body")

        val stock = StockJson.deserialize(body)
        println(s"Deserialized data: $stock")

        val stockInfo: StockInfo = stockToStockInfo(stock, timestamp)

        sendStockInfo(id, stockInfo, producer)

        "ok"
      }
    )

    post(
      "/deltas/:id",
      { (request: Request, response: Response) =>
        val id        = request.params("id")
        val body      = request.body()
        val timestamp = Instant.now().toEpochMilli
        println(s"--> Received@$timestamp: id: $id - data: $body")

        val delta = DeltaStockJson.deserialize(body)
        println(s"Deserialized data: $delta")

        val stockInfo = deltaToStockInfo(delta, timestamp)

        sendStockInfo(id, stockInfo, producer)

        "ok"
      }
    )

  }

  def sendStockInfo(
      id: String,
      stockInfo: StockInfo,
      producer: KafkaProducer[String, String]
  ): Unit = {
    val doc = StockInfoJson.serialize(stockInfo)

    // TODO send stock info into Kafka
    val record: ProducerRecord[String, String] =
      new ProducerRecord[String, String](Configuration.StockInfoTopic, id, doc)

    producer.send(record)
  }

  def stockToStockInfo(stock: Stock, timestamp: Long): StockInfo =
    StockInfo(
      id        = stock.id,
      stockType = StockInfo.STOCK,
      timestamp = timestamp,
      quantity  = stock.quantity
    )

  def deltaToStockInfo(delta: DeltaStock, timestamp: Long): StockInfo =
    StockInfo(
      id        = delta.id,
      stockType = StockInfo.STOCK,
      timestamp = timestamp,
      quantity  = delta.delta
    )

}
