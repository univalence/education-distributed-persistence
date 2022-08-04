package io.univalence.microservice.api

import com.datastax.oss.driver.api.core.CqlSession
import io.univalence.microservice.common.entity.{ProjectedStock, ProjectedStockJson}
import spark.{Request, Response}
import spark.Spark._
import io.univalence.microservice.common.repository.ProjectedStockRepository
import io.univalence.microservice.common.repository.impl.CassandraProjectedStockRepository

object MicroserviceApiMain {

  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    port(Configuration.ApiHttpPort)
    println(s"Ready on http://localhost:${Configuration.ApiHttpPort}/")

    // TODO instantiate a stock repository from a Cassandra session

    val session = CqlSession.builder().build()

    val repository: ProjectedStockRepository = new CassandraProjectedStockRepository(session)

    get(
      "/api/stocks",
      (request: Request, response: Response) => {
        println(s"--> Requested to find all stocks")
        val stocks: List[ProjectedStock] = repository.findAll().toList
        val doc                          = ProjectedStockJson.gson.toJson(stocks.asJava)

        response.`type`("application/json")
        doc
      }
    )

    get(
      "/api/stocks/:id",
      (request: Request, response: Response) => {
        val id = request.params("id")
        println(s"--> Requested to find stock of #$id")
        val stock: Option[ProjectedStock] = repository.findById(id)

        if (stock.isEmpty) {
          println(s"product#$id not found")
          response.status(404)

          s"Product#$id Not Found"
        } else {
          println(s"product#$id: $stock")
          val doc = ProjectedStockJson.serialize(stock.get)

          response.`type`("application/json")
          doc
        }
      }
    )

    awaitStop()
  }

}
