package io.univalence.microservice.api

object Configuration {

  val ApiHttpPort: Int = 8080

  val CassandraPort: Int        = 9042
  val CassandraKeyspace: String = "store"
  val CassandraTable: String    = "stock"

}
