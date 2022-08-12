package io.univalence.microservice.demo

import okhttp3.{OkHttpClient, Request}

import scala.util.Using

object ReaderMain {

  def main(args: Array[String]): Unit = {
    val id  = "1"
    val url = s"http://localhost:${Configuration.ApiHttpPort}/api/stocks/$id"

    val client = new OkHttpClient.Builder().build()
    val request = new Request.Builder()
      .url(url)
      .get()
      .build()

    Using(client.newCall(request).execute()) { response =>
      if (response.isSuccessful) {
        println(s"Success: data: ${response.body().string()}")
      } else {
        println(
          s"Error: ${response.message()} (${response
            .code()}) - data: ${response.body().string()}"
        )
      }
    }.get
  }

}
