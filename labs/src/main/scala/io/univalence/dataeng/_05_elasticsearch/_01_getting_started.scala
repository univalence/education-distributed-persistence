package io.univalence.dataeng._05_elasticsearch

import co.elastic.clients.elasticsearch._types.{ElasticsearchException, FieldValue}
import co.elastic.clients.elasticsearch._types.query_dsl.{MatchAllQuery, MatchQuery, Query}
import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.{BulkRequest, CountRequest, SearchRequest, SearchResponse}
import co.elastic.clients.elasticsearch.core.bulk.{BulkOperation, IndexOperation}
import co.elastic.clients.elasticsearch.core.search.HitsMetadata
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.core.JsonParseException

import io.univalence.dataeng._05_elasticsearch.ElasticSearch.{countIndex, search}
import io.univalence.dataeng._05_elasticsearch.ElasticSearchConnector.usingElasticSearch
import io.univalence.dataeng.internal.utils.retry

import scala.io.Source
import scala.util.Using

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

/**
 * =Elasticsearch=
 * In this file, we will create our first line of codes using
 * Elasticsearch.
 *
 * ==Introduction==
 * Elasticsearch [[https://www.elastic.co/elasticsearch/]] is a search
 * engine written in Java by the company Elastic. It generally allows
 * company to add search capability (such as a search box in a website).
 * Its specificity is that Eleasticsearch is scalable and can retrieve
 * data in almost real time even with a huge amount of storing data.
 *
 * ==Elasticsearch with Java==
 * We can communicate to Elasticsearch using its REST API
 * [[https://www.elastic.co/guide/en/elasticsearch/reference/current/rest-apis.html]].
 * However generally people prefer use high level library to interact
 * with it. We will use
 * [[https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/index.html]].
 *
 * ==Useful link==
 *   - https://www.elastic.co/guide/en/elasticsearch/reference/current/rest-apis.html
 */
object _01_getting_started {
  case class Tweet @JsonCreator() (id: String, source: String, text: String) {
    override def toString: String =
      s"""
         |id: $id
         |source: $source
         |
         |$text
         |""".stripMargin
  }

  /** Returns the type of the elastic search exception. */
  def exceptionType(e: ElasticsearchException): String = e.response().error().`type`()

  def main(args: Array[String]): Unit =
    usingElasticSearch { (client, mapper) =>
      /**
       * ==Index==
       * Indexes are like database in a relational database. Indexes
       * should be unique, it throws an exception if the index already
       * exists.
       */
      val index: String = "tweets"

      try client.indices().create((c: CreateIndexRequest.Builder) => c.index(index))
      catch {
        case e: ElasticsearchException =>
          exceptionType(e) match {
            case "resource_already_exists_exception" =>
              println(s"INFO: The index '$index' already exists, we ignore this step.")
            case _ => e.printStackTrace()
          }
      }

      /**
       * ==Inserting documents==
       * Unlike relational databases, Elasticsearch doesn't contain rows
       * but documents. Each document actually is a JSON with metadata
       * such as an id.
       */
      val inputStream = new GZIPInputStream(new FileInputStream("data/twitter/tweets.json.gz"))

      try Using(Source.fromInputStream(inputStream)) { file =>
        val operations: java.util.ArrayList[BulkOperation] = new java.util.ArrayList[BulkOperation]()

        for (line <- file.getLines())
          try {
            val tweet: Tweet = mapper.reader.readValue(line, classOf[Tweet])
            val id           = tweet.id

            val indexOperation: IndexOperation[Tweet] =
              new IndexOperation.Builder()
                .index(index)
                .id(id)
                .document(tweet)
                .build()

            operations.add(new BulkOperation.Builder().index(indexOperation).build())
          } catch {
            /**
             * Some tweets are not well formatted for Jackson since it
             * is for educational purpose, we ignore them.
             */
            case _: JsonParseException => ()
            case e: Exception          => e.printStackTrace()
          }

        val bulkRequest = new BulkRequest.Builder().operations(operations).build()
        client.bulk(bulkRequest)
      } catch {
        case e: Exception => e.printStackTrace()
      }

      /**
       * We need to wait for BulkOperation to be processed by
       * ElasticSearch.
       */
      val tweetsAdded = retry(countIndex(index)(client))(_ > 0L)(5)

      tweetsAdded match {
        case Some(v) => println(s"$v tweets added to elasticsearch\n")
        case None    => println("The index has no value something bad happens, call the teacher 🚨")
      }

      /**
       * ==Searching documents==
       * The strength of Elasticsearch is its searching ability. Indeed,
       * you have many ways to search for a specific set of documents.
       *
       * When a document meet the requirements it is called a "hit". A
       * "hit" has the document data and a score associated to it. The
       * score describe the overall score the document get compared to
       * the predicates.
       *
       * you can find all the possibilities at:
       * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html.
       */

      /** Show basic information about the hits. */
      def printHitsInformation(header: String, hits: HitsMetadata[Tweet]): Unit = {
        println(s"===$header===")
        println(s"The maximum score is ${hits.maxScore()}")
        println(s"The best Tweet candidate is:")

        if (hits.hits().size() == 0) println("There is not hits :/\n")
        else println(hits.hits().get(0).source())
      }

      /**
       * The most simple query is to match all the document from the
       * Database.
       *
       * When you use match all, every documents have a score of 1.
       */
      val matchAll: MatchAllQuery           = new MatchAllQuery.Builder().build()
      val matchAllQuery: Query              = new Query.Builder().matchAll(matchAll).build()
      val matchAllHits: HitsMetadata[Tweet] = search(matchAllQuery, index)(client)
      printHitsInformation("Match All", matchAllHits)

      /**
       * Of course, you can search for specific kind of tweets using
       * different query predicates such as match.
       */
      val fieldValue: FieldValue         = new FieldValue.Builder().stringValue("computer").build()
      val matchSubquery: MatchQuery      = new MatchQuery.Builder().field("text").query(fieldValue).build()
      val matchQuery: Query              = new Query.Builder().`match`(matchSubquery).build()
      val matchHits: HitsMetadata[Tweet] = search(matchQuery, index)(client)
      printHitsInformation("Match", matchHits)
    }
}
