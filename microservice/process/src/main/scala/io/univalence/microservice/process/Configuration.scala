package io.univalence.microservice.process

object Configuration {

  val StockInfoTopic: String = "stock-info"

  val CassandraPort: Int        = 9042
  val CassandraKeyspace: String = "store"
  val CassandraTable: String    = "stock"

  val KafkaBootstrap: String = "localhost:9092"
  val KafkaGroup: String     = "process"

}
