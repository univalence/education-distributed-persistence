package io.univalence.dataeng._05_elasticsearch

import co.elastic.clients.elasticsearch._types.{ElasticsearchException, FieldValue}
import co.elastic.clients.elasticsearch._types.query_dsl.{MatchQuery, Query, RangeQuery}
import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.BulkRequest
import co.elastic.clients.elasticsearch.core.bulk.{BulkOperation, IndexOperation}
import co.elastic.clients.elasticsearch.core.search.HitsMetadata
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest
import co.elastic.clients.json.JsonData
import com.fasterxml.jackson.annotation.JsonCreator

import io.univalence.dataeng._05_elasticsearch.Assertions._
import io.univalence.dataeng._05_elasticsearch.ElasticSearch._
import io.univalence.dataeng._05_elasticsearch.ElasticSearchConnector.usingElasticSearch
import io.univalence.dataeng.internal.exercise_tools._

import scala.jdk.CollectionConverters._

/**
 * ==Exercise==
 * You have an e-commerce platform selling cars with millions of users
 * and you want to implement a SearchBox using ElasticSearch to list
 * yours products.
 */
object _02_exercices {
  case class Product @JsonCreator() (id: Int, name: String, brand: String, kind: String, price: Int)

  val products =
    List(
      Product(1, "Peugeot 208", "Peugeot", "gasoline", 17_000),
      Product(2, "Peugeot 308", "Peugeot", "gasoline", 25_450),
      Product(3, "Tesla model S", "Tesla", "electric", 104_990),
      Product(4, "Tesla model 3", "Tesla", "electric", 43_800),
      Product(5, "Tesla model X", "Tesla", "electric", 114_990),
      Product(6, "Tesla model Y", "Tesla", "electric", 59_990),
      Product(7, "Renault ZOE", "Renault", "electric", 32_000),
      Product(8, "Renault CLIO", "Renault", "gasoline", 16_550),
      Product(9, "Renault CAPTUR", "Renault", "gasoline", 22_450)
    )

  val index: String = "products"

  def main(args: Array[String]): Unit =
    usingElasticSearch { (client, _) =>
      implicit val implicitClient: ElasticsearchClient = client

      section("Create an index 'products'") {
        try client.indices().create((c: CreateIndexRequest.Builder) => c.index(index))
        catch { case e: ElasticsearchException => () }

        checkIndexExist(index)
      }

      section("Add the products to elastic search") {
        val operations: List[BulkOperation] =
          products.map { product =>
            val indexOperation =
              new IndexOperation.Builder()
                .index(index)
                .id(product.id.toString)
                .document(product)
                .build()

            new BulkOperation.Builder().index(indexOperation).build()
          }

        val bulkRequest = new BulkRequest.Builder().operations(operations.asJava).build()
        client.bulk(bulkRequest)

        checkProductsAreInserted(index, products)
      }

      section("Search for all electric cars available") {
        def userSearchFor(userInput: String): List[Product] = {
          val fieldValue: FieldValue           = new FieldValue.Builder().stringValue(userInput).build()
          val matchSubquery: MatchQuery        = new MatchQuery.Builder().field("kind").query(fieldValue).build()
          val matchQuery: Query                = new Query.Builder().`match`(matchSubquery).build()
          val matchHits: HitsMetadata[Product] = search(matchQuery, index)
          hitsToList(matchHits)
        }

        val cars = userSearchFor("electric")

        check(cars.forall(_.kind == "electric"))
        check(cars.length == 5)
      }

      section("""
                |Sometimes customers don't know how to write 'peugeot' and write 'peugot' instead, 
                |the SearchBox should returns peugeot's cars even if customer write 'peugot'.""".stripMargin) {
        def userSearchFor(userInput: String): List[Product] = {
          val fieldValue: FieldValue = new FieldValue.Builder().stringValue(userInput).build()
          val matchSubquery: MatchQuery =
            new MatchQuery.Builder().field("brand").fuzziness("2").query(fieldValue).build()
          val matchQuery: Query                = new Query.Builder().`match`(matchSubquery).build()
          val matchHits: HitsMetadata[Product] = search(matchQuery, index)
          hitsToList(matchHits)
        }

        val cars = userSearchFor("peugot")

        check(cars.length == 2)
        check(cars.forall(_.brand == "Peugeot"))
      }

      section("Customers want only cars between 0 and 30 000 euros.") {
        def userSearchForPriceBetween(from: Int, to: Int): List[Product] = {
          val rangeQuery: RangeQuery =
            new RangeQuery.Builder().field("price").gt(JsonData.of(from)).lt(JsonData.of(to)).build()
          val matchQuery: Query                = new Query.Builder().range(rangeQuery).build()
          val matchHits: HitsMetadata[Product] = search(matchQuery, index)
          hitsToList(matchHits)
        }

        val cars = userSearchForPriceBetween(0, 30_000)

        check(cars.length == 4)
        check(cars.forall(_.price < 30_000))
      }
    }
}
