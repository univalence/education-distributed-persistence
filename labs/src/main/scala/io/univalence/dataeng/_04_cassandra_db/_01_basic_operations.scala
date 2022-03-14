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
 * We saw in-process databases such as RocksDB and MapDB. However this
 * kind of databases doesn't scala when we need to deal with tons of
 * data.
 *
 * Cassandra is a column oriented, No SQL database with eventual
 * consistency. (meaning that they chose Availability and Partition
 * tolerance in the CAP theorem).
 *
 * Cassandra is design to handle a high volume of data by scaling using
 * multiple nodes with no single point of failure. Thus, it is design as
 * a multi master system and not a master/slave system. It means that
 * any nodes can die without impacting directly the system.
 *
 * Cassandra has first been created in Facebook offices and is now an
 * open source project under the Apache foundation. Cassandra is written
 * in C++ and comes with a Java wrapper.
 *
 * ==Concepts==
 * Cassandra is build around different concepts:
 *   - Keyspace: Configurations containing tables and defining how many
 *     times we should replicate the data for example.
 *   - Table: Defines the typed schema for a collection of partitions.
 *   - Partition:Defines the mandatory part of the primary key all rows
 *     in Cassandra must have to identify the node in a cluster where
 *     the row is stored.
 *   - Row: Collection of columns identifier by a primary key
 *   - Column: A single column with a type for example (name: String)
 *
 * ==Data storage==
 * Cassandra also uses the same data storage as RocksDB following the
 * LSM-tree for write heavy databases.
 *
 * ==Cassandra specificity==
 * Cassandra differ a lot from SQL database because it is query first.
 * It means that we design Cassandra table with queries in mind. Indeed
 * in Cassandra you can't join tables and you can query the data only
 * using the partition keys. If you need to query the same kind of data
 * in two different manners, they you denormalize the data and you
 * create two tables with different partition key.
 *
 * Also even if you can use ORDER BY to order your data, it is advised
 * to use your clustering key to sort your data.
 *
 * ==In this file==
 *   - The code is wrap around a TestContainer creating the Cassandra
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

    usingCassandra { session =>
      implicit val implicitSession: CqlSession = session
      implicit val implicitKeySpace: String    = cassandraKeyspace

      exercise("Create a keyspace") {

        /**
         * The beginning of a Cassandra journey is creating a keyspace,
         * since we have only one node, we ask the system to replicate
         * the data only once.
         */
        val keyspaceQuery = SchemaBuilder.createKeyspace(cassandraKeyspace).ifNotExists.withSimpleStrategy(1)
        session.execute(keyspaceQuery.build)

        /**
         * We can use the newly create keyspace by default. Here we use
         * CQL, CQL stands for Cassandra Query Language it is a SQL like
         * language to interact with Cassandra.
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
        val video: Video = Video(1, "My video", LocalDate.now())

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
