package io.univalence.dataeng._04_cassandra_db

import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, ResultSet, SimpleStatement}
import com.datastax.oss.driver.api.querybuilder.{QueryBuilder, SchemaBuilder}
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert

import io.univalence.dataeng._04_cassandra_db.CassandraConnector._
import io.univalence.dataeng.internal.exercise_tools._

import java.time.LocalDate

/**
 * =Cassandra=
 * We saw in-process databases such as RocksDB and MapDB. However, this
 * kind of databases doesn't scale when we need to deal with tons of
 * data.
 *
 * Cassandra is a column-oriented NoSQL database with eventual
 * consistency (meaning that it is focused on Availability and Partition
 * tolerance, following the CAP theorem).
 *
 * Cassandra is designed to handle a high volume of data by scaling in
 * using multiple nodes with no single point of failure (SPOF). Thus, it
 * is designed as a multi master system and not a master/slave system.
 * It means that some nodes can die without impacting directly the
 * system, so, operators have time replace dead nodes.
 *
 * Cassandra has first been created in Facebook offices and is now an
 * open source project under the Apache foundation, mainly managed by
 * the company Datastax. Cassandra is written in Java.
 *
 * ==Concepts==
 * Cassandra is built around different concepts:
 *   - '''Keyspace''': Configurations containing tables and defining how
 *     many times we should replicate the data for example.
 *   - '''Table''': Defines the typed schema for a collection of
 *     partitions.
 *   - '''Partition''': Defines the mandatory part of the primary key
 *     all rows in Cassandra must have to identify the node in a cluster
 *     where the row is stored.
 *   - '''Row''': Collection of columns identified by a primary key.
 *   - '''Column''': A single column with a type for example (name:
 *     String). It is possible to use composite types like lists, maps,
 *     sets, and any kind of sub-structures.
 *
 * ==Data storage==
 * Cassandra also uses the same type of data storage as RocksDB. It is
 * also based on LSM-tree, which enhances huge amount of writes in the
 * database.
 *
 * ==Cassandra specificity==
 * Cassandra differs a lot from SQL database because it is query first.
 * It means that we design Cassandra table with queries in mind. Indeed
 * in Cassandra you can't join tables and you can query the data only
 * using the partition keys. If you need to query the same kind of data
 * in two different manners, then you denormalize the data and you
 * create two tables with different partition key.
 *
 * Also, even if you can use ORDER BY to order your data, it is advised
 * to use your clustering key to sort your data.
 *
 * ==In this file==
 *   - The code is wrapped around a TestContainer creating the Cassandra
 *     container to interact with.
 *   - You then have an exercise to interact with Cassandra.
 */
object _01_basic_operations {
  val tableName: String = "videos"

  case class Video(id: Int, title: String, date: LocalDate, creator: String)

  def main(args: Array[String]): Unit = {
    def executeStatement(statement: SimpleStatement)(implicit session: CqlSession, keyspace: String): ResultSet = {
      statement.setKeyspace(CqlIdentifier.fromCql(keyspace))
      session.execute(statement)
    }

    /** FIXME explain */
    usingCassandra { session =>
      /** FIXME remove implicits */
      implicit val implicitSession: CqlSession = session
      implicit val implicitKeySpace: String    = cassandraKeyspace

      exercise("Create a keyspace") {

        /**
         * The beginning of a Cassandra journey is creating a keyspace,
         * since we have only one node, we ask the system to replicate
         * the data only once. The keyspace will be used to then create
         * tables.
         */
        val keyspaceQuery =
          SchemaBuilder
            .createKeyspace(cassandraKeyspace)
            .ifNotExists
            .withSimpleStrategy(1)

        session.execute(keyspaceQuery.build)

        /**
         * We can use the newly created keyspace by default. Here, we
         * use CQL. CQL stands for ''Cassandra Query Language''. It is a
         * SQL-like language designed to interact with Cassandra.
         */
        session.execute("USE " + CqlIdentifier.fromCql(cassandraKeyspace))
      }

      exercise("Create a table") {

        /**
         * We want to store video data. Imagine that you are a company
         * such as Youtube with millions of videos. We first need to
         * create a table containing the schema describing an item. In
         * our case, a video has an id, a title and a creation date.
         * Since we want to request the data by ID, we define the ID has
         * our partition key.
         */
        val createTable =
          SchemaBuilder
            .createTable(tableName)
            .ifNotExists()
            .withPartitionKey("video_id", DataTypes.INT)
            .withColumn("title", DataTypes.TEXT)
            .withColumn("creation_date", DataTypes.DATE)

        executeStatement(createTable.build)
      }

      exercise("Insert a video") {

        /** We now want to insert a video in our newly create table */
        val video: Video = Video(1, "My video", LocalDate.now(), "Jon")

        val insertInto: RegularInsert =
          QueryBuilder
            .insertInto(tableName)
            .value("video_id", QueryBuilder.bindMarker)
            .value("title", QueryBuilder.bindMarker)
            .value("creation_date", QueryBuilder.bindMarker)
            .value("creator_name", QueryBuilder.bindMarker)

        val insertStatement: SimpleStatement     = insertInto.build
        val preparedStatement: PreparedStatement = session.prepare(insertStatement)
        val statement: BoundStatement =
          preparedStatement
            .bind()
            .setInt(0, video.id)
            .setString(1, video.title)
            .setLocalDate(2, video.date)
            .setString(3, video.creator)

        session.execute(statement)
      }

      exercise("Retrieve a video") {

        /**
         * We can retrieve our newly inserted video using its partition
         * key.
         */
        val query     = QueryBuilder.selectFrom(tableName).columns("video_id", "title", "creation_date", "creator_name")
        val statement = query.build
        val result: ResultSet = session.execute(statement)
        val first             = result.one()
        val storedVideo =
          Video(
            id      = first.getInt("video_id"),
            title   = first.getString("title"),
            date    = first.getLocalDate("creation_date"),
            creator = first.getString("creator_name")
          )
        println(s"The stored video was $storedVideo.")
      }

      exercise("Design a table to query video by creator name sorted by creation date.") {
        ???
      }

      exercise("Design a table to query video by creation date.") {
        ???
      }
    }
  }
}
