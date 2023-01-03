package io.univalence.dataeng._04_cassandra_db

import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.datastax.oss.driver.api.core.cql.{ResultSet, Row, SimpleStatement}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder._
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert
import com.datastax.oss.driver.api.querybuilder.relation.Relation._
import com.datastax.oss.driver.api.querybuilder.select.Select

import io.univalence.dataeng._04_cassandra_db.CassandraConnector._
import io.univalence.dataeng.internal.exercise_tools._

import scala.io.Source
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.{Success, Try, Using}

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

/**
 * =Cassandra=
 * We have seen in-process databases such as RocksDB and MapDB. However,
 * this kind of databases doesn't scale when we need to deal with tons
 * of data.
 *
 * Cassandra is a column-oriented NoSQL database with eventual
 * consistency (meaning that it is focused on Availability and Partition
 * tolerance, following the CAP theorem).
 *
 * Cassandra is designed to handle a high volume of data by scaling in
 * using multiple nodes with no single point of failure (SPOF). Thus, it
 * is designed as a multi master system and not a master/slave system.
 * It means that some nodes can die without impacting directly the
 * system. So, operators have time replace dead nodes.
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
 * Cassandra differs a lot from SQL database, because it is
 * [[https://cassandra.apache.org/doc/latest/cassandra/data_modeling/data_modeling_rdbms.html#query-first-design query first]].
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
object _02_temperatures {

  /**
   * In this file, we will explore data in the file
   * `data/climate/city_temperature.csv.gz`. It is a file with
   * temperature of cities around the world.
   *
   * Below is the Scala case class of each temperature record.
   */

  case class Temperature(
      region:             String,
      country:            String,
      state:              Option[String],
      city:               String,
      month:              Int,
      day:                Int,
      year:               Int,
      averageTemperature: Float
  ) {

    /**
     * We provide this function to insert an instance of Temperature
     * inside Cassandra. Because ''state'' is Optional, we have to
     * handle both cases when there is a state or not.
     *
     * By default, every column in Cassandra is optional, so we don't
     * have to insert a state when the state is None.
     */
    def insert(tableName: String)(session: CqlSession): Unit = {
      val baseQuery: RegularInsert =
        insertInto(tableName)
          .value("year", literal(year))
          .value("month", literal(month))
          .value("day", literal(day))
          .value("averageTemperature", literal(averageTemperature))
          .value("region", literal(region))
          .value("country", literal(country))
          .value("city", literal(city))

      val query =
        state match {
          case None    => baseQuery
          case Some(s) => baseQuery.value("state", literal(s))
        }

      val statement: SimpleStatement = query.build
      session.execute(statement)
    }
  }

  object Temperature {
    val TABLE_BY_YEAR    = "climates_by_year"
    val TABLE_BY_COUNTRY = "climates_by_country"

    /**
     * This is a helper function to convert a Cassandra row into a
     * Temperature. Because a row can be anything, we have to handle the
     * case when we have a row that doesn't correspond to a temperature,
     * thus returning an '''Try[Temperature]'''.
     */
    def fromCassandra(row: Row): Try[Temperature] =
      Try(
        Temperature(
          region             = row.getString("region"),
          country            = row.getString("country"),
          state              = Option(row.getString("state")),
          city               = row.getString("city"),
          month              = row.getInt("month"),
          day                = row.getInt("day"),
          year               = row.getInt("year"),
          averageTemperature = row.getFloat("averageTemperature")
        )
      )

    def createTableByYear(session: CqlSession): Unit = {
      val query =
        SchemaBuilder
          .createTable(TABLE_BY_YEAR)
          .ifNotExists()
          .withPartitionKey("year", DataTypes.INT)
          .withClusteringColumn("month", DataTypes.INT)
          .withClusteringColumn("day", DataTypes.INT)
          .withColumn("averageTemperature", DataTypes.FLOAT)
          .withColumn("region", DataTypes.TEXT)
          .withColumn("country", DataTypes.TEXT)
          .withColumn("state", DataTypes.TEXT)
          .withClusteringColumn("city", DataTypes.TEXT)

      val statement: SimpleStatement = query.build
      session.execute(statement)
    }

    // TODO by student
    def createTableByCountry(session: CqlSession): Unit = {
      s"""CREATE TABLE IF NOT EXISTS $TABLE_BY_COUNTRY (
         |  country            text,
         |  year               int,
         |  month              int,
         |  day                int,
         |  city               text,
         |  averageTemperature float,
         |  region             text,
         |  state              text,
         |  
         |  PRIMARY KEY ((country), (year, month, day, city))
         |)""".stripMargin

      val query =
        SchemaBuilder
          .createTable(TABLE_BY_COUNTRY)
          .ifNotExists()
          .withPartitionKey("country", DataTypes.TEXT)
          .withClusteringColumn("year", DataTypes.INT)
          .withClusteringColumn("month", DataTypes.INT)
          .withClusteringColumn("day", DataTypes.INT)
          .withClusteringColumn("city", DataTypes.TEXT)
          .withColumn("averageTemperature", DataTypes.FLOAT)
          .withColumn("region", DataTypes.TEXT)
          .withColumn("state", DataTypes.TEXT)

      val statement: SimpleStatement = query.build
      session.execute(statement)
    }

    private def retrieve(query: Select)(session: CqlSession): List[Temperature] = {
      val statement         = query.build
      val result: ResultSet = session.execute(statement)

      result.all().asScala.toList.map(fromCassandra).collect { case Success(v) => v }
    }

    /**
     * In our first query, we decided to use '''year''' as our partition
     * key, allowing us to extract quickly all the temperatures for a
     * particular year.
     */
    def retrieveByYear(year: Int)(session: CqlSession): List[Temperature] = {
      val query =
        selectFrom(TABLE_BY_YEAR)
          .all()
          .where(column("year").isEqualTo(literal(year)))
      retrieve(query)(session)
    }

    // TODO by student
    def retrieveByCountry(country: String)(session: CqlSession): List[Temperature] = {
      val query =
        selectFrom(TABLE_BY_COUNTRY)
          .all()
          .where(column("country").isEqualTo(literal(country)))
      retrieve(query)(session)
    }
  }

  def insertCityTemperature(tableName: String)(session: CqlSession): Unit = {
    val inputStream = new GZIPInputStream(new FileInputStream("data/climate/city_temperature.csv.gz"))

    /**
     * We provide a dataset with a lot of temperatures stored in a CSV.
     *
     * Here is an example of data contained in the file:
     * {{{
     * Region,Country,State,City,Month,Day,Year,AverageTemperature
     * Asia,Kazakhstan,,Almaty,2,22,2010,34.0
     * Asia,Kazakhstan,,Almaty,2,23,2010,30.7
     * Asia,Kazakhstan,,Almaty,2,24,2010,34.1
     * Asia,Kazakhstan,,Almaty,2,25,2010,32.7
     * }}}
     *
     * We convert them into temperatures and then we insert them into
     * Cassandra.
     */
    Using(Source.fromInputStream(inputStream)) { file =>
      for (line <- file.getLines().toSeq.tail.take(10000)) {
        val columns = line.split(",")
        val temperature =
          Temperature(
            region             = columns(0),
            country            = columns(1),
            state              = if (columns(2).nonEmpty) Some(columns(2)) else None,
            city               = columns(3),
            month              = columns(4).toInt,
            day                = columns(5).toInt,
            year               = columns(6).toInt,
            averageTemperature = columns(7).toFloat
          )
        temperature.insert(tableName)(session)
      }
    }
  }

  def main(args: Array[String]): Unit =
    /**
     * The function '''usingCassandra''' is an utility that:
     *   - creates a Cassandra cluster with one node in docker
     *   - creates a Cassandra session to interact with the cluster
     *   - provides the session
     */
    usingCassandra { session =>
      exercise("Create a keyspace") {

        /**
         * The beginning of a Cassandra journey is creating a keyspace.
         * Since we only have one node, we ask the system to replicate
         * the data only once. The keyspace will be used then to create
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
         * We want to store temperature data. We first need to create a
         * table containing the schema describing an item. In our case,
         * a temperature has many field such as the average temperature
         * or the locations.
         *
         * In our first query, we want to retrieve the temperatures per
         * year. Thus, we decide that '''year''' is our partition key.
         * We may want to sort our data inside the partitions so we had
         * a clustering key composed with month, day, and city.
         *
         * The partition key and the clustering keys form the primary
         * key. It has to be unique inside our cluster.
         */
        Temperature.createTableByYear(session)
      }

      exercise("Insert a temperature") {

        /**
         * We now want to insert a temperature in our newly create
         * table.
         */
        val temperature: Temperature =
          Temperature(
            region             = "Neither",
            country            = "Minecraft",
            state              = None,
            city               = "Crimson Forest",
            month              = 10,
            day                = 20,
            year               = 2020,
            averageTemperature = 37.3f
          )

        temperature.insert(Temperature.TABLE_BY_YEAR)(session)
      }

      exercise("Retrieve a temperature") {

        /**
         * Now that our temperature is stored, we can retrieve it
         * through our helper function.
         */
        val temperatures = Temperature.retrieveByYear(2020)(session)

        check(temperatures.length == 1)
        check(temperatures.head.country == "Minecraft")
      }

      exercise("Insert temperatures from data/climate/city_temperature.csv.gz") {
        insertCityTemperature(Temperature.TABLE_BY_YEAR)(session)
        val temperatures = Temperature.retrieveByYear(2000)(session)

        check(temperatures.length == 366)
      }

      exercise("Design a table to query temperatures of a particular country (Algeria).") {
        Temperature.createTableByCountry(session)
        insertCityTemperature(Temperature.TABLE_BY_COUNTRY)(session)
        val temperatures = Temperature.retrieveByCountry("Algeria")(session)

        check(temperatures.length == 9265)
        check(temperatures.map(_.country).distinct.length == 1)
      }
    }

}
