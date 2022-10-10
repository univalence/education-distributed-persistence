package io.univalence.microservice.ingest

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import spark.{Request, Response}
import spark.Spark._

import io.univalence.microservice.common.args._
import io.univalence.microservice.common.entity._

import java.time.Instant

object MicroserviceIngestMain {

  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    val parameters: Map[String, Option[String]] = readArgs(args.toList).toMap
    val servicePort    = parameters.get("port").flatMap(_.map(_.toInt)).getOrElse(Configuration.IngestHttpPort)
    val kafkaBootstrap = parameters.get("kafka.servers").flatten.getOrElse(Configuration.KafkaBootstrap)
    val stockInfoTopic = parameters.get("topic").flatten.getOrElse(Configuration.StockInfoTopic)

    port(servicePort)

    val producer: KafkaProducer[String, String] =
      new KafkaProducer[String, String](
        Map[String, AnyRef](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBootstrap
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

        sendStockInfo(id, stockInfo, producer, stockInfoTopic)

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

        sendStockInfo(id, stockInfo, producer, stockInfoTopic)

        "ok"
      }
    )

  }

  def sendStockInfo(
      id: String,
      stockInfo: StockInfo,
      producer: KafkaProducer[String, String],
      stockInfoTopic: String
  ): Unit = {
    val doc = StockInfoJson.serialize(stockInfo)

    // TODO send stock info into Kafka
    val record: ProducerRecord[String, String] =
      new ProducerRecord[String, String](stockInfoTopic, id, doc)

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
