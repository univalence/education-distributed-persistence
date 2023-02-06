package io.univalence.dataeng._06_kafka._02_services

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

    val table = database.getTable[Any](tableName)
    val json: Option[JsonObject] =
      table
        .map(
          _.getAll
            .foldLeft(new JsonObject) { case (json, (key, value)) =>
              json.add(key, new JsonPrimitive(value.toString))
              json
            }
        )

    if (json.isEmpty) {
      response.status(404)
      response.`type`("plain/text")
      s"Table $tableName not found."
    } else {
      response.`type`("application/json")
      new Gson().toJson(json)
    }
  }

}
