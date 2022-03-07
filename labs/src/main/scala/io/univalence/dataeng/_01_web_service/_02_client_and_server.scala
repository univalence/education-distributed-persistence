package io.univalence.dataeng._01_web_service

import scala.util.Using

/**
 * Now we will focus a little bit more on the client side.
 *
 * Till now, we have use a browser or curl to communicate with a Web
 * service. But it is also possible to make another application to
 * communicate with a Web service.
 *
 * The [[java.net.URL]] provides a low-level interface to act so. But,
 * [[https://square.github.io/okhttp/ OkHttp]] from Square is more
 * interesting library.
 *
 * The client below simply sends a `GET` request for the path `\hello`
 * and displays the body of the response and if status code.
 *
 * But, first launch [[_02_server]] at the end of the file, and then
 * launch this client.
 */
object _02_client_1 {
  import okhttp3._

  def main(args: Array[String]): Unit = {
    val client = new OkHttpClient()

    val builder = new Request.Builder()
    val request =
      builder
        .get()
        .url(s"http://localhost:${_02_server.serverPort}/hello")
        .build()

    Using(client.newCall(request).execute()) { response =>
      println(s"server response status: ${response.code()} ${response.message()}")
      Using(response.body()) { body =>
        println(s"server response body: ${body.string()}")
      }
    }.get
  }

}

/**
 * What differs with this client is that first it sends a POST request,
 * second the data are sent in the request body.
 *
 * ==Media type==
 *
 * When you are using a request body, you have to tell the server how to
 * interpret the data. This done by indicating the media type
 * ([[http://tools.ietf.org/html/rfc2045 RFC2045]]). Here we use
 * `text/plain` to tell the server to interpret data as human readable
 * text. There are other media type like `application/json`, for JSON
 * data.
 */
object _02_client_2 {
  import okhttp3._

  def main(args: Array[String]): Unit = {
    val client = new OkHttpClient()

    val builder = new Request.Builder()

    val requestBody = RequestBody.create("Jon", MediaType.get("text/plain"))

    val request =
      builder
        .post(requestBody)
        .url(s"http://localhost:${_02_server.serverPort}/message")
        .build()

    Using(client.newCall(request).execute()) { response =>
      println(s"server response status: ${response.code()} ${response.message()}")
      Using(response.body()) { body =>
        println(s"server response body: ${body.string()}")
      }
    }.get
  }

}

object _02_server {
  import spark._
  import spark.Spark._

  val serverPort = 8091

  def main(args: Array[String]): Unit = {
    port(serverPort)

    get(
      "/hello",
      { (request: Request, response: Response) =>
        response.`type`("application/json")

        """{"message": "hello"}"""
      }
    )

    post(
      "/message",
      { (request: Request, response: Response) =>
        response.`type`("application/json")

        val content = request.body()
        s"""{"message": "$content"}"""
      }
    )
  }

}
