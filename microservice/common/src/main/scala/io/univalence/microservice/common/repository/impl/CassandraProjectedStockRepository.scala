package io.univalence.microservice.common.repository.impl

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchType, Row}
import io.univalence.microservice.common.entity.ProjectedStock
import io.univalence.microservice.common.repository.ProjectedStockRepository

class CassandraProjectedStockRepository(session: CqlSession, keyspace: String, table: String)
    extends ProjectedStockRepository {

  import scala.jdk.CollectionConverters._

  override def findById(id: String): Option[ProjectedStock] = {
    val statement =
      session.prepare(s"SELECT id, ts, qtt FROM $keyspace.$table WHERE id = ?")
    val result: Option[Row] = Option(session.execute(statement.bind(id)).one())

    result.map(result =>
      ProjectedStock(
        id = result.getString("id"),
        timestamp = result.getLong("ts"),
        quantity = result.getInt("qtt")
      )
    )
  }

  override def findAll(): Iterator[ProjectedStock] = {
    val statement =
      session.prepare(s"SELECT id, ts, qtt FROM $keyspace.$table")
    val result: List[Row] =
      session.execute(statement.bind()).all().asScala.toList

    result
      .map(result =>
        ProjectedStock(
          id = result.getString("id"),
          timestamp = result.getLong("ts"),
          quantity = result.getInt("qtt")
        )
      )
      .iterator
  }

  override def save(projectedStock: ProjectedStock): Unit = {
    val statement =
      session.prepare(s"INSERT INTO $keyspace.$table (id, ts, qtt) VALUES (?, ?, ?)")
    session.execute(
      statement.bind(
        projectedStock.id,
        projectedStock.timestamp,
        projectedStock.quantity
      )
    )
  }

  override def saveAll(stocks: List[ProjectedStock]): Unit = {
    val statement =
      session.prepare(s"INSERT INTO $keyspace.$table (id, ts, qtt) VALUES (?, ?, ?)")

    val batch =
      BatchStatement
        .newInstance(BatchType.UNLOGGED)
        .addAll(
          stocks
            .map(stock =>
              statement.bind(stock.id, stock.timestamp, stock.quantity)
            )
            .asJava
        )

    session.execute(batch)
  }

}
