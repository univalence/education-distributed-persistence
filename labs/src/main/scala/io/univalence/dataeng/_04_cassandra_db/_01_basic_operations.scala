package io.univalence.dataeng._04_cassandra_db

import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.datastax.oss.driver.api.querybuilder.{QueryBuilder, SchemaBuilder}
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert
import io.univalence.dataeng._04_cassandra_db.CassandraConnector.{cassandraKeyspace, usingCassandra}

import java.time.LocalDate

object _01_basic_operations {

  def main(args: Array[String]): Unit = {
    usingCassandra(session => {
      implicit val implicitSession: CqlSession = session

      def createKeyspace(keyspaceName: String, numOfReplicas: Int): Unit = {
        val keyspaceQuery = SchemaBuilder.createKeyspace(keyspaceName).ifNotExists.withSimpleStrategy(numOfReplicas)
        session.execute(keyspaceQuery.build)
      }

      def useKeyspace(keyspace: String) =
        session.execute("USE " + CqlIdentifier.fromCql(keyspace))

      case class Video(id: Int, title: String, date: LocalDate)

      val tableName: String = "videos"

      def createTable(keyspace: String): Unit = {
        val createTable = SchemaBuilder.createTable(tableName).ifNotExists()
          .withPartitionKey("video_id", DataTypes.INT)
          .withColumn("title", DataTypes.TEXT)
          .withColumn("creation_date", DataTypes.DATE)

        executeStatement(createTable.build, keyspace)
      }

      def executeStatement(statement: SimpleStatement, keyspace: String) = {
        if (keyspace != null)
          statement.setKeyspace(CqlIdentifier.fromCql(keyspace))
        session.execute(statement)
      }

      def insertVideo(video: Video): Long = {
        val insertInto: RegularInsert = QueryBuilder.insertInto(tableName)
          .value("video_id", QueryBuilder.bindMarker)
          .value("title", QueryBuilder.bindMarker)
          .value("creation_date", QueryBuilder.bindMarker)

        val insertStatement: SimpleStatement = insertInto.build
        val preparedStatement: PreparedStatement = session.prepare(insertStatement)
        val statement: BoundStatement = preparedStatement.bind()
          .setInt(0, video.id)
          .setString(1, video.title)
          .setLocalDate(2, video.date)
        session.execute(statement)

        video.id
      }

      createKeyspace(cassandraKeyspace, 1)
      useKeyspace(cassandraKeyspace)
      createTable(cassandraKeyspace)
      val video: Video = Video(1, "My video", LocalDate.now())
      insertVideo(video)





    })
  }

}
