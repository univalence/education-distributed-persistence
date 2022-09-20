package io.univalence.microservice.ingest

object Configuration {

  val IngestHttpPort: Int = 10001

  val StockInfoTopic: String = "stock-info"
  val KafkaBootstrap: String = "localhost:9092"

}
