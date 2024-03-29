package io.univalence.microservice.demo

object Configuration {

  val CassandraPort: Int    = 9042
  val StoreKeyspace: String = "store"
  val StockTable: String    = "stock"

  val IngestHttpPort: Int = 10001
  val ApiHttpPort: Int    = 8080

  val ServiceHost: String = "localhost"

  val StockInfoTopic: String = "stock-info"
  val KafkaBootstrap: String = "localhost:9092"
  val KafkaPartition: Int    = 8

}
