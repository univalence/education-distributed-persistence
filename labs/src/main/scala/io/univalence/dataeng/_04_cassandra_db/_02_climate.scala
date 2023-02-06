package io.univalence.dataeng._04_cassandra_db

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row

import io.univalence.dataeng.internal.cassandra_utils._
import io.univalence.dataeng.internal.exercise_tools._
import io.univalence.dataeng.internal.utils._

import scala.io.Source
import scala.jdk.CollectionConverters.CollectionHasAsScala

import java.io.FileInputStream
import java.net.InetSocketAddress
import java.util.zip.GZIPInputStream

/**
 * In this file we will use Cassandra on a larger dataset: the evolution
 * of temperatures around the world.
 */
object _02_climate {

  /**
   * The temperature file is `data/climate/city_temperature.csv.gz`.
   * This file looks like:
   *
   * {{{
   * Region,Country,State,City,Month,Day,Year,AverageTemperature
   * Asia,Kazakhstan,,Almaty,2,22,2010,34.0
   * Asia,Kazakhstan,,Almaty,2,23,2010,30.7
   * Asia,Kazakhstan,,Almaty,2,24,2010,34.1
   * Asia,Kazakhstan,,Almaty,2,25,2010,32.7
   * }}}
   *
   * `State` is a field that is only used for the USA.
   * `AverageTemperature` is given in fahrenheit.
   *
   * Those data should be mapped in a memory representation, easier to
   * operate. Below is the Scala case class for temperature records.
   * Note: `Option` type indicates that a value might be present or not. If it is
   */
  case class Temperature(
      region:             String,
      country:            String,
      state:              Option[String],
      city:               String,
      month:              Int,
      day:                Int,
      year:               Int,
      averageTemperature: Double
  )

  def main(args: Array[String]): Unit =
    using(
      CqlSession
        .builder()
        .addContactPoint(new InetSocketAddress("localhost", 9042))
        .withLocalDatacenter("datacenter1")
        .build()
    ) { session =>
      exercise_ignore("Create the keyspace 'climate'") {
        // TODO set the replication factor to the number of node that you have up to 3
        session.execute("""CREATE KEYSPACE IF NOT EXISTS climate WITH replication = {
                          |  'class':              'SimpleStrategy',
                          |  'replication_factor': '???'
                          |}""".stripMargin)
      }

      exercise_ignore("Create the table of temperatures by year") {

        /**
         * We want to store temperature data. We first need to create a
         * table containing the schema describing an item. In our case,
         * a temperature has many field such as the average temperature
         * or the locations.
         *
         * ''We always to of the usage of a table to determine the
         * key.''
         *
         * In the later queries, we want to retrieve the temperatures
         * per year. Thus, we decide that '''year''' is our partition
         * key. We may want to sort our data inside the partitions so we
         * had a clustering key composed with month, day, and city.
         *
         * The partition key and the clustering keys form the primary
         * key. It has to be unique inside our cluster.
         */
        session.execute(
          s"""CREATE TABLE IF NOT EXISTS climate.temperature_by_year (
             |  country            text,
             |  year               int,
             |  month              int,
             |  day                int,
             |  city               text,
             |  averageTemperature double,
             |  region             text,
             |  state              text,
             |  
             |  PRIMARY KEY ((???), ???)
             |)""".stripMargin
        )
      }

      exercise_ignore("Insert a temperature") {

        /**
         * We now want to insert a temperature in our newly create
         * table.
         */
        val temperature =
          """{
            |  "region"             : "Neither",
            |  "country"            : "Minecraft",
            |  "state"              : null,
            |  "city"               : "Crimson Forest",
            |  "month"              : 10,
            |  "day"                : 20,
            |  "year"               : 2020,
            |  "averageTemperature" : 37.3
            |}""".stripMargin

        session.execute(s"""INSERT INTO climate.temperature_by_year JSON '$temperature'""")
      }

      exercise_ignore("Retrieve a temperature") {
        val result =
          session.execute(
            """SELECT *
              |FROM climate.temperature_by_year
              |WHERE year = ???""".stripMargin
          )

        val temperatures = result.all().asScala.toList
        displayRows(temperatures)

        check(temperatures.length == 1)
        check(temperatures.head.getString("country") == "Minecraft")
      }

      exercise_ignore("Insert temperatures from data/climate/city_temperature.csv.gz") {
        val statement =
          session.prepare("""INSERT INTO climate.temperature_by_year
                            | (region,country,state,city,month,day,year,averageTemperature)
                            | VALUES
                            | (:region,:country,:state,:city,:month,:day,:year,:averageTemperature)
                            | """.stripMargin)

        foreachTemperatureIn("data/climate/city_temperature.csv.gz") { temperature =>
          val bound =
            statement
              .bind()
              .setString("region", temperature.region)
              .setString("country", temperature.country)
              .setString("state", temperature.state.orNull)
              .setString("city", temperature.city)
              .setInt("month", temperature.month)
              .setInt("day", temperature.day)
              .setInt("year", temperature.year)
              .setDouble("averageTemperature", temperature.averageTemperature)

          session.execute(bound)
        }

        val result    = session.execute("SELECT year FROM climate.temperature_by_year")
        val rows      = result.all().asScala.toList
        val yearCount = rows.map(_.getInt("year")).distinct.length

        comment("What is the number of years covers by the dataset?")
        check(yearCount == ??)
      }

      exercise_ignore("Design a table to query temperatures of a particular country (Algeria).") {
        val queryStr =
          s"""CREATE TABLE IF NOT EXISTS climate.climates_by_country (
             |  country            text,
             |  year               int,
             |  month              int,
             |  day                int,
             |  city               text,
             |  averageTemperature float,
             |  region             text,
             |  state              text,
             |  
             |  PRIMARY KEY ((???), (???))
             |)""".stripMargin

        val statement =
          session.prepare("""INSERT INTO climate.climates_by_country
                            | (region,country,state,city,month,day,year,averageTemperature)
                            | VALUES
                            | (:region,:country,:state,:city,:month,:day,:year,:averageTemperature)
                            | """.stripMargin)

        foreachTemperatureIn("data/climate/city_temperature.csv.gz") { temperature =>
          val bound =
            statement
              .bind()
              .setString("region", temperature.region)
              .setString("country", temperature.country)
              .setString("state", temperature.state.orNull)
              .setString("city", temperature.city)
              .setInt("month", temperature.month)
              .setInt("day", temperature.day)
              .setInt("year", temperature.year)
              .setDouble("averageTemperature", temperature.averageTemperature)

          session.execute(bound)
        }

        // TODO get the temperature list for Algeria
        val result =
          session.execute(
            """???"""
          )

        val algeriaTemperatures: List[Row] = result.all().asScala.toList

        check(algeriaTemperatures.length == 9265)
        check(algeriaTemperatures.map(_.getString("country")).distinct.length == 1)
      }
    }

  def foreachTemperatureIn(fileName: String)(f: Temperature => Unit): Unit =
    using(Source.fromInputStream(new GZIPInputStream(new FileInputStream(fileName)))) { file =>
      /**
       * We provide a dataset with a lot of temperatures stored in a
       * CSV.
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
      for ((line, lineNumber) <- file.getLines().toSeq.zipWithIndex.tail.take(10000)) {
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
            averageTemperature = columns(7).toDouble
          )

        if (lineNumber % 1000 == 0)
          comment(s"$lineNumber line processed")

        f(temperature)
      }
    }

}
