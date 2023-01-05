package io.univalence.dataeng._05_elasticsearch

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.indices.ExistsRequest

import io.univalence.dataeng._05_elasticsearch._02_old_exercices.Product
import io.univalence.dataeng._05_elasticsearch.ElasticSearch.countIndex
import io.univalence.dataeng.internal.exercise_tools.check
import io.univalence.dataeng.internal.utils.retry

object Assertions {
  def checkIndexExist(index: String)(implicit client: ElasticsearchClient): Unit = {
    val existsRequest: ExistsRequest = new ExistsRequest.Builder().index(index).build()
    val existsResponse               = client.indices().exists(existsRequest)
    val indexExists                  = existsResponse.value()
    check(indexExists)
  }

  def checkProductsAreInserted(index: String, products: List[Product])(implicit client: ElasticsearchClient): Unit = {
    val productsInserted: Long = retry(countIndex(index))(_ > 0)(5).getOrElse(0)
    check(productsInserted == products.length)
  }
}
