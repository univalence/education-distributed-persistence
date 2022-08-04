package io.univalence.dataeng._01_web_service

import com.google.gson.Gson

import io.univalence.dataeng.internal.exercise_tools._

/**
 * ==JSON serialization and deserialization==
 * '''JSON''' (JavaScript Object Notation) is a popular data format for
 * Web applications. The previous widely adopted data format on the Web
 * was XML. JSON came with a lighter syntax and is based on the
 * JavaScript syntax.
 *
 * There are many available libraries to serialize JSON data (ie.
 * mapping data between memory model and JSON format). Java community
 * usually uses Jackson. Scala has many choices. Here, we will use GSON,
 * a library made by Google, that works both with Java and Scala. Its
 * usage is really simple.
 */
object _03_json {

  /** Below is our memory model. It represents a user. */
  case class User(id: String, name: String, age: Int)

  def main(args: Array[String]): Unit = {

    /**
     * We first create a mapper object, which is an instance that can
     * map memory data into JSON and the reverse.
     */
    val mapper = new Gson()

    /** We first convert data into JSON. */
    val userJon: User   = User("123", "Jon", 32)
    val jsonJon: String = mapper.toJson(userJon)

    section("Encode: convert a case class instance into JSON") {
      check(jsonJon == ??)
    }

    /** We now convert JSON into data. */
    section("Decode: convert JSON into case class instance") {
      val memoryData: User = mapper.fromJson(jsonJon, classOf[User])

      check(memoryData == ??)
    }

    /**
     * What happen if a field is missing? Do not hesitate to print the
     * result.
     */
    section("Decode JSON with missing a field") {
      val jsonMary = """{"id": "321","name":"Mary"}"""
      val userMary = mapper.fromJson(jsonMary, classOf[User])

      check(userMary == ??)
    }

    /**
     * What happen if there is a syntax error?
     *
     * TODO Activate the section below and fix the bug.
     */
    exercise_ignore("Decode a JSON with syntax error") {
      val jsonTom = """{"id":"123","name":"Tom","age":}"""
      val userTom = mapper.fromJson(jsonTom, classOf[User])

      check(userTom == User(id = "123", name = "Tom", age = 35))
    }
  }

}
