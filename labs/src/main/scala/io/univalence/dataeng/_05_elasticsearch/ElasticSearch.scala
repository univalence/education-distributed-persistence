package io.univalence.dataeng._05_elasticsearch

import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.{CountRequest, SearchRequest, SearchResponse}
import co.elastic.clients.elasticsearch.core.search.HitsMetadata

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.reflect.{classTag, ClassTag}

object ElasticSearch {

  /** Returns the number of documents of an index. */
  def countIndex(name: String)(implicit client: ElasticsearchClient): Long = {
    val countRequest: CountRequest = new CountRequest.Builder().index(name).build()
    client.count(countRequest).count()
  }

  /** Generalizes the query process to retrieves objects of type T. */
  def search[T: ClassTag](query: Query, index: String)(client: ElasticsearchClient): HitsMetadata[T] = {
    val searchRequest: SearchRequest = new SearchRequest.Builder().index(index).query(query).build()
    val searchResponse: SearchResponse[T] =
      client.search(searchRequest, classTag[T].runtimeClass.asInstanceOf[Class[T]])
    searchResponse.hits()
  }

  def hitsToList[T: ClassTag](hits: HitsMetadata[T]): List[T] = hits.hits().asScala.toList.map(_.source())
}
