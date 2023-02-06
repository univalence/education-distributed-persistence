package io.univalence.dataeng._05_elasticsearch

import com.datastax.driver.core.LocalDate

import io.univalence.dataeng.internal.elasticsearch_utils._
import io.univalence.dataeng.internal.utils.using

import scala.io.Source
import scala.util.Try

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

object _03_climate {

  val BaseUrl = "http://localhost:9200"

  case class Temperature(
      region:             String,
      country:            String,
      state:              Option[String],
      city:               String,
      date:               LocalDate,
      averageTemperature: Option[Double]
  ) {
    def toJson: String =
      s"""{
         |  "region":"$region",
         |  "country":"$country",
         |  "state":${state.map(s => s"\"$s\"").getOrElse("null")},
         |  "city":"$city",
         |  "date":"$date",
         |  "averageTemperature":${averageTemperature.map(_.toString).getOrElse("null")}
         |}""".stripMargin.split("\n").toList.map(_.trim).mkString
  }

  def main(args: Array[String]): Unit =
//    {
//      val response =
//        http_PUT(
//          s"$BaseUrl/temperature",
//          s"""{
//             |  "settings": {
//             |    "index": {
//             |      "number_of_shards": 4,
//             |      "number_of_replicas": 1
//             |    }
//             |  }
//             |}""".stripMargin
//        )
//
//      display(response)
//    }

    using(
      Source.fromInputStream(new GZIPInputStream(new FileInputStream("data/climate/city_temperature.csv.gz")))
    ) { file =>
      val temperatures: Iterator[Temperature] =
        (for (line <- file.getLines().drop(1)) yield try {
          val fields     = line.split(",")
          val date       = Try(LocalDate.fromYearMonthDay(fields(6).toInt, fields(4).toInt, fields(5).toInt)).toOption
          val tempValue  = fields(7).toDouble
          val fahrenheit = Option.when(tempValue > -99)(tempValue)

          date.map(d =>
            Temperature(
              region             = fields(0),
              country            = fields(1),
              state              = Option.when(fields(2).trim.nonEmpty)(fields(2)),
              city               = fields(3),
              date               = d,
              averageTemperature = fahrenheit
            )
          )
        } catch {
          case e: Exception =>
            println(line)
            throw e
        }).filter(_.nonEmpty).map(_.get)

      temperatures
        .grouped(1000)
        .foreach { group =>
          val content =
            group
              .map { t =>
                val id        = s"${t.city}#${t.date}"
                val operation = s"""{ "index" : { "_index" : "climate", "_id" : "$id" } }"""
                operation + "\n" + t.toJson
              }
              .mkString("\n") + "\n"

          println(s"insert ${group.size} documents")

          val response = http_POST(s"$BaseUrl/_bulk", content)
          println(response)
        }
    }

}
