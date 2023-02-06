package io.univalence.dataeng._06_kafka._03_join

import com.google.gson.{Gson, JsonArray, JsonObject, JsonPrimitive}
import spark.{Request, Response, Route}

import io.univalence.dataeng._06_kafka._02_services._03_your_service.apiPort
import io.univalence.dataeng._06_kafka.Database

object internal {

  def getTables(database: Database, path: String): Route = { (request: Request, response: Response) =>
    val json =
      database.getTableNames.foldLeft(new JsonArray()) { case (json, tablename) =>
        val doc = new JsonObject()
        doc.add("name", new JsonPrimitive(tablename))
        doc.add("url", new JsonPrimitive(s"http://localhost:$apiPort$path/$tablename"))
        json.add(doc)

        json
      }

    response.`type`("application/json")
    new Gson().toJson(json)
  }

  def getData(database: Database): Route = { (request: Request, response: Response) =>
    val tableName = request.params("table")

    val size =
      Option(request.queryParams("size"))
        .map(_.toInt)
        .getOrElse(20)
    val position =
      Option(request.queryParams("position"))
        .map(_.toInt)
        .getOrElse(0)
    val prefix = Option(request.queryParams("prefix"))

    val table = database.getTable[Any](tableName)
    val json: Option[JsonObject] =
      table
        .map { t =>
          val content =
            prefix
              .map(p => t.getPrefix(p))
              .getOrElse(t.getAll)

          content
            .slice(position, position + size)
            .foldLeft(new JsonObject) { case (json, (key, value)) =>
              json.addProperty(key, value.toString)
              json
            }
        }

    val result = new JsonObject
    result.addProperty("table", tableName)
    result.addProperty("position", position)
    result.addProperty("size", size)
    val count = json.map(_.size()).getOrElse(0)
    result.addProperty("count", count)
    prefix.foreach(p => result.addProperty("prefix", p))
    json.foreach(j => result.add("data", j))
    if (count > 0) {
      if (position > 0)
        result.addProperty("previous", s"http://localhost:18080/data/$tableName?size=$size&position=${Math.max(0, position - size)}")
      result.addProperty("next", s"http://localhost:18080/data/$tableName?size=$size&position=${position + size}")
    }

    if (json.isEmpty) {
      response.status(404)
      response.`type`("plain/text")
      s"Table $tableName not found."
    } else {
      response.`type`("application/json")
      new Gson().toJson(result)
    }
  }

}
