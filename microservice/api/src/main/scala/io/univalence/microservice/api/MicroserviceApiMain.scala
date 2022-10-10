package io.univalence.microservice.api

import com.datastax.oss.driver.api.core.CqlSession
import spark.{Request, Response}
import spark.Spark._

import io.univalence.microservice.common.args.readArgs
import io.univalence.microservice.common.entity.{ProjectedStock, ProjectedStockJson}
import io.univalence.microservice.common.repository.ProjectedStockRepository
import io.univalence.microservice.common.repository.impl.CassandraProjectedStockRepository

import java.net.InetSocketAddress

object MicroserviceApiMain {

  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    val parameters: Map[String, Option[String]] = readArgs(args.toList).toMap
    val servicePort   = parameters.get("port").flatMap(_.map(_.toInt)).getOrElse(Configuration.ApiHttpPort)
    val cassandraPort = parameters.get("cassandra.port").flatMap(_.map(_.toInt)).getOrElse(Configuration.CassandraPort)

    port(servicePort)
    println(s"Ready on http://localhost:$servicePort/")

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
