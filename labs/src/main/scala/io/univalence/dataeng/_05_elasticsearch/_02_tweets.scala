package io.univalence.dataeng._05_elasticsearch

import com.google.gson.JsonParser

import io.univalence.dataeng.internal.elasticsearch_utils._
import io.univalence.dataeng.internal.exercise_tools._

import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

/**
 * Now, we will explore a larger dataset with tweets.
 *
 * When you get '''tweets''' from the
 * [[https://developer.twitter.com/en/docs Twitter API]], you get a
 * complex document with many information, including, the tweet ID, a
 * part of the tweet text, the date, the determined language,
 * information about the user, hashtags, retweets...
 *
 * In this exercise, we will explore this dataset.
 */
object _02_tweets {

  def main(args: Array[String]): Unit = {
    val tweetFile  = "data/twitter/tweets.json.gz"
    val tweetIndex = "tweet"
    val baseUrl    = "http://localhost:9200"

    // Initialize tweet index if needed
    if (!indexExists(baseUrl, tweetIndex)) {
      loadIndexWithTweets(tweetFile, baseUrl, tweetIndex)
      Thread.sleep(1000)
    }

    /**
     * By default, the search API only returns 10 documents.
     *
     * If you need more documents, in the query part of the URL, you
     * can:
     *   - use the parameter `from` with a number, to skip the given
     *     number of documents
     *   - use the parameter `size` with a number, to get the given
     *     number of documents
     *
     * Here, we will get the number of document in the tweet index to
     * modify the value of the parameter `size`.
     *
     * Note: avoid using parameters `from` and `size` above 10,000
     * documents.
     */
    val count = (http_GET(s"$baseUrl/$tweetIndex/_count").bodyAsJson / "count").getAsInt

    check(count == 3205)

    exercise_ignore("Get all languages available in the dataset") {
      /**
       * Get all available languages in the tweet sample
       */
      val response =
        http_POST(
          s"$baseUrl/$tweetIndex/_search?size=$count",
          """{
            |  "query": {
            |    "query_string": {
            |      "query": "lang:*"
            |    }
            |  }
            |}""".stripMargin
        )

      check(response.code == 200)
      val documents = (response.bodyAsJson / "hits" / "hits").getAsJsonArray.toList.map(_ / "_source")
      check(documents.size == count)

      val langs =
        documents
          // get just the lang field
          .map(j => (j / "lang").getAsString)
          // remove replicated lang
          .toSet

      // see http://www.iana.org/assignments/language-subtag-registry/language-subtag-registry
      check(
        langs ==
          Set(
            "und", // means undetermined
            "pt",
            "ca",
            "tl",
            "ko",
            "de",
            "tr",
            "en",
            "es",
            "ja",
            "ro",
            "sl",
            "pl",
            "in",
            "ar",
            "fr",
            "el",
            "it",
            "ru",
            "th",
            "fa",
            "zh"
          )
      )
    }

    exercise("???") {
      val response =
        http_POST(s"$baseUrl/$tweetIndex/_search?size=0",
//          """{
//            |  "query": {
//            |    "query_string": {
//            |      "query": "*"
//            |    }
//            |  }
//            |}""".stripMargin
          """{
            |  "size": 0,
            |  "aggs": {
            |    "hashtags": {
            |      "terms": {
            |        "field": "entities.hashtags.text.keyword"
            |      }
            |    }
            |  }
            |}""".stripMargin
        )

      display(response)
    }

    exercise_ignore("agg") {
      val response =
        http_POST(
          s"$baseUrl/$tweetIndex/_search?size=0",
          """{
            |  "aggs": {
            |    "lang_qty": {
            |      "terms": {
            |        "field": "lang.keyword",
            |        "size": 50
            |      }
            |    }
            |  }
            |}""".stripMargin
        )

      display(response)
    }

  }

  private def loadIndexWithTweets(tweetFile: String, baseUrl: String, index: String): Unit = {
    println("Loading tweets...")
    val bulkIndexTweets =
      loadTweetsFrom(tweetFile)
        .map { case (id, tweet) =>
          val operation = s"""{ "index" : { "_index" : "$index", "_id" : "$id" } }"""
          operation + "\n" + tweet
        }

    println(s"Indexing ${bulkIndexTweets.length} tweets...")

    bulkIndexTweets.grouped(1000).foreach { group =>
      val bulkOperations = group.mkString("\n") + "\n"
      val response       = http_POST(s"$baseUrl/_bulk", bulkOperations)
      println(response)
    }
  }

  private def indexExists(baseUrl: String, index: String) = http_GET(s"$baseUrl/$index").code == 200

  def loadTweetsFrom(filename: String): Seq[(Long, String)] =
    Using(Source.fromInputStream(new GZIPInputStream(new FileInputStream(filename)), "UTF-8")) { file =>
      file
        .getLines()
        .zipWithIndex
        .flatMap { case (line, i) =>
          Try {
            JsonParser.parseString(line).getAsJsonObject.get("id").getAsLong
          } match {
            case Success(id) =>
              Iterator(id -> line)
            case Failure(exception) =>
              println(line)
              println(s"Error at line $i: ${exception.getMessage}")
              Iterator.empty
          }
        }
        .toList
    }.get

}
