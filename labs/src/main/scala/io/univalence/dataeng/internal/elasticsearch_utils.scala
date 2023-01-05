package io.univalence.dataeng.internal

import com.google.gson.{GsonBuilder, JsonElement, JsonParser}
import okhttp3.{MediaType, OkHttpClient, Request => OkRequest, RequestBody, Response => OkResponse}

import scala.util.Using

object elasticsearch_utils {

  import scala.jdk.CollectionConverters._

  def display(response: Response): Unit = {
    println(s"${Console.YELLOW}  ${response.code} ${response.message}${Console.RESET}")
    response.headers
      .foreach { case (k, vs) =>
        println(s"${Console.YELLOW}  $k: ${vs.mkString(";")}${Console.RESET}")
      }

    val gson =
      new GsonBuilder()
        .setPrettyPrinting()
        .create()

    println(gson.toJson(response.bodyAsJson))
  }

  def http_GET(url: String): Response =
    execute(
      new OkRequest.Builder()
        .get()
        .url(url)
        .build()
    )

  def http_POST(url: String, content: String): Response =
    execute(
      new OkRequest.Builder()
        .post(RequestBody.create(content, JSON_TYPE))
        .url(url)
        .build()
    )

  def http_PUT(url: String, content: String): Response =
    execute(
      new OkRequest.Builder()
        .put(RequestBody.create(content, JSON_TYPE))
        .url(url)
        .build()
    )

  def http_DELETE(url: String): Response =
    execute(
      new OkRequest.Builder()
        .delete()
        .url(url)
        .build()
    )

  def execute(request: OkRequest): Response = {
    val client = new OkHttpClient()
    println(s"${Console.GREEN}calling $request${Console.RESET}")

    Using(client.newCall(request).execute())(Response.from).get
  }

  val JSON_TYPE: MediaType = MediaType.parse("application/json")

  implicit class Json(json: JsonElement) {
    def /(field: String): JsonElement = json.getAsJsonObject.get(field)

    def apply(n: Int): JsonElement = json.getAsJsonArray.get(n)

    def toList: List[JsonElement] = json.getAsJsonArray.asScala.toList
  }

  case class Response(code: Int, message: String, headers: Map[String, List[String]], body: String) {
    def bodyAsJson: JsonElement = JsonParser.parseString(body)
  }

  object Response {
    def from(r: OkResponse): Response =
      Response(
        code    = r.code(),
        message = r.message(),
        headers = r.headers().toMultimap.asScala.toMap.view.mapValues(_.asScala.toList).toMap,
        body    = r.body().string()
      )
  }

}
