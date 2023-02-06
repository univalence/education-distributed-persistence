package io.univalence.dataeng._06_kafka._03_join

import com.google.gson.{JsonElement, JsonParser}

import io.univalence.dataeng.internal.elasticsearch_utils.Json
import io.univalence.dataeng.internal.exercise_tools.time
import io.univalence.dataeng.internal.utils.{hexdump, using}

import scala.io.Source

import java.io.FileInputStream
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.zip.GZIPInputStream

case class Venue(
    id:        String,
    latitude:  Double,
    longitude: Double,
    venueType: String,
    country:   String
) {
  def toJson: String =
    s"""{"id": "$id","latitude":$latitude,"longitude":$longitude,"venueType":"$venueType","country":"$country"}"""
}

object Venue {
  def loopOverCSVFile(filename: String)(f: Venue => Unit) = {
    val in = new GZIPInputStream(new FileInputStream(filename))
    using(Source.fromInputStream(in)) { data =>
      time("Load checkins in database") {
        for (line <- data.getLines()) {
          // building checkin entity
          val data = line.split("\t")

          val venue = Venue(data(0), data(1).toDouble, data(2).toDouble, data(3), data(4))
          f(venue)
        }
      }
    }
  }

  def fromJson(content: String): Venue = fromJson(JsonParser.parseString(content))

  def fromJson(json: JsonElement): Venue =
    Venue(
      id        = (json / "id").getAsString,
      latitude  = (json / "latitude").getAsDouble,
      longitude = (json / "longitude").getAsDouble,
      venueType = (json / "venueType").getAsString,
      country   = (json / "country").getAsString
    )

}

case class Checkin(
    userID:    String,
    venueId:   String,
    timestamp: Instant,
    offset:    Int
) {
  def toJson: String =
    s"""{"userID": "$userID","venueId":"${venueId}","timestamp":${timestamp.toEpochMilli},"offset":$offset}"""
}
object Checkin {

  // timestamp is based on Pacific time
  private val pacificZone: ZoneId = ZoneId.of("America/Los_Angeles")

  def foursquareTimestampToInstant(timestamp: String): Instant = {
    val formatter = DateTimeFormatter.ofPattern("EEE LLL dd HH:mm:ss Z yyyy", Locale.US)
    val dateTime  = LocalDateTime.parse(timestamp, formatter)

    dateTime.toInstant(pacificZone.getRules.getOffset(dateTime))
  }

  def timestampToString(timestamp: Instant): String = LocalDateTime.ofInstant(timestamp, pacificZone).toString

  def loopOverCSVFile(filename: String)(f: Checkin => Unit) = {
    val in = new GZIPInputStream(new FileInputStream(filename))
    using(Source.fromInputStream(in)) { data =>
      time("Load checkins in database") {
        for (line <- data.getLines()) {
          // building checkin entity
          val data      = line.split("\t")
          val timestamp = foursquareTimestampToInstant(data(2))

          val checkin = Checkin(data(0), data(1), timestamp, data(3).toInt)
          f(checkin)
        }
      }
    }
  }

  def fromJson(content: String): Checkin = fromJson(JsonParser.parseString(content))

  def fromJson(json: JsonElement): Checkin =
    Checkin(
      userID    = (json / "userID").getAsString,
      venueId   = (json / "venueId").getAsString,
      timestamp = Instant.ofEpochMilli((json / "timestamp").getAsInt),
      offset    = (json / "offset").getAsInt
    )
}
