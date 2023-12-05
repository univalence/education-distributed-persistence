package io.univalence.dataeng._07_kvstore

import ujson.Value

import io.univalence.dataeng.internal.exercise_tools._

import scala.collection.MapView
import scala.io.Source
import scala.util.Using

import java.time.LocalDateTime

/**
 * We have to store and manage orders in a coffee shop. Orders are
 * stored in a CSV file. It contains those fields:
 *   - id: the ID of the order,
 *   - client: an ID of the client who has ordered,
 *   - timestamp: the timestamp at which the order has been registered,
 *   - product: the name of the ordered product,
 *   - price: the price of the ordered product.
 *
 * But we would use a KV-store in order to improve the access to those
 * data. The main difficulty there is to decide how to design the key.
 *
 * Locally, bad key design may lead to bad performance and data loss.
 *
 * For example, if you store your orders by ID and you want to get
 * orders at a given date, you will have scan the whole KV-store. But,
 * if you store orders by timestamp, the access wil be direct.
 *
 * Another example, if you store order by product name, you will lost
 * most of your data. This happens because the CSV file contains many
 * lines for products like expresso, café, café crème... So every time
 * an order for an expresso is added in the KV-store, it will overwrite
 * the previously registered order for expresso. To solve this, you will
 * have to use another field with the product name to build the key,
 * like the order ID or the timestamp.
 *
 * In a distributed context, bad key design may lead to bad data
 * balancing and application crash.
 */
object _02_orders {
  def main(args: Array[String]): Unit =
    section("Design of the key") {
      import Orders.Order
      val orders = Orders.load()

      section("Store orders by ID") {

        /**
         * We have orders from a coffee shop and we want to store them
         * by order ID.
         *
         * There are 2 possibilities. For the first one, the key only
         * contains the ID and the value contains all the order
         * information as a JSON object.
         */
        exercise("Store the whole structure by ID") {
          // Create the store
          val orderStore = new InMemoryStore
          // Add orders. The key is the ID of each order
          orders.foreach { order =>
            // each order is converted into a JSON object. The JSON
            // object follows this format:
            // {"id":"123","client":"xyz","timestamp":"2023-01-10T16:23:32","product":"expresso","price":1.23}
            orderStore.put(key = order.id, value = order.toJson)
          }

          comment("How many orders are there in the CSV file?")
          check(orders.size == ??)

          comment("How many orders are there in the store?")
          check(orderStore.scan().get.size == ??)

          // Get an order from its ID
          val orderId = "62469972"
          val record  = orderStore.get(orderId).get
          comment(s"record: $record")

          // Get the product
          val product = record.value.obj("product").str

          comment(s"What is the product name of the order $orderId?")
          check(product == ??)
        }

        /**
         * For the second possibility, the key is composed of the order
         * ID and a field name, separated by a `#`. For example,
         * `123#product` gives access to the field `product` of the
         * order `123`. The value contains thus just the value of the
         * field.
         */
        exercise("Store field by field") {
          val orderStore = new InMemoryStore
          orders.foreach { order =>
            orderStore.put(key = s"${order.id}#id", value        = order.id)
            orderStore.put(key = s"${order.id}#client", value    = order.client)
            orderStore.put(key = s"${order.id}#timestamp", value = order.timestamp.toString())
            orderStore.put(key = s"${order.id}#product", value   = order.product)
            orderStore.put(key = s"${order.id}#price", value     = order.price)
          }

          // Get the product name of the order 58920768
          val orderId = "58920768"
          val record  = orderStore.get(orderId + "#product").get
          comment(s"record: $record")
          val product = record.value.str
          comment(s"What is the product name associated to the order $orderId?")
          check(product == ??)

          // Get all the fields of order 58920768
          val records = orderStore.scanPrefix(orderId).get
          val orderData: Map[String, Value] =
            records.toList.groupMapReduce(
              // the key of the Map is tghe part of the key in the
              // store that represents the field name
              _.key.split("#")(1)
            )(
              // The value associated in the Map is the JSON object
              // in the store
              _.value
            )((_, v) => v)
          // convert the structure into an instance of Order
          val order =
            Order(
              id        = orderData("id").str,
              client    = orderData("client").str,
              timestamp = LocalDateTime.parse(orderData("timestamp").str),
              product   = orderData("product").str,
              price     = orderData("price").num
            )

          comment(s"What is the price associated to the order $orderId?")
          check(order.price == ??)
        }
      }

      section("Store by timestamp") {
        exercise("Get the number of orders at a given date") {
          val orderStore = new InMemoryStore
          orders.foreach(order => orderStore.put(key = order.timestamp.toString(), value = order.toJson))

          val date = "2023-02-01"

          val result = orderStore.scanPrefix(date).get
          comment("How many orders are there at " + date)
          check(result.size == ??)
        }

        exercise("Get the turnover for a given month") {
          val orderStore = new InMemoryStore
          orders.foreach(order => orderStore.put(key = order.timestamp.toString(), value = order.toJson))

          val date = "2023-02"

          val result = orderStore.scanPrefix(date).get
          val prices = result.map(record => record.value.obj("price").num)

          comment("What is the turnover during the month of " + date)
          check(prices.sum == ??)
        }
      }

      section("Compound key") {

        /**
          * In the exercise below, we will see the interest of building
          * a key from different fields in order to avoid to scan the
          * whole KV-store.
          */
        exercise("The most consumed product by a client") {
          val orderStore = new InMemoryStore
          orders.foreach(order =>
            orderStore.put(
              // We need to store orders by client and product.
              // We add the timestamp in the key (it could have been
              // the order ID) in order to disguish each order and in
              // order to ensure that all necessary information are
              // stored.
              key   = s"${order.client}#${order.product}#${order.timestamp}",
              value = order.toJson
            )
          )

          val client = "XztHU0aeUckvR7AC"

          // Get and convert orders
          val records =
            orderStore
              // get all orders for the given client
              .scanPrefix(client)
              .get
              .map { record =>
                // extract the product name from the key
                val fields = record.key.split("#")
                val key    = fields(1)

                // for each key, count 1
                (key, 1)
              }

          val mostConsumedProduct =
            records.toList
              // count each occurrence of each product, summing the 1s
              // associated to the keys
              .groupMapReduce(_._1)(_._2)(_ + _)
              .toList
              // sort by product occurrences from the less bought to
              // the most bought
              .sortBy(_._2)
              // get the most bought product
              .last

          comment(s"What product the client $client consumes the most?")
          check(mostConsumedProduct == (??, ??))
        }

        /**
          * The code below has almost the same code as the one in the
          * exercise above. Except that you have to determine the key
          * in order to get the client who has bought `chocolat chaud`
          * the most.
          */
        exercise("Which client buys 'chocolat chaud' the most?") {
          val orderStore = new InMemoryStore
          orders.foreach(order =>
            orderStore.put(
              // TODO determine the key
              key   = s"???",
              value = order.toJson
            )
          )

          val product = "chocolat chaud"

          // Get and convert orders
          val records =
            orderStore
              .scanPrefix(product)
              .get
              .map { record =>
                val fields = record.key.split("#")
                val key    = fields(1)

                (key, 1)
              }

          val mostConsumedProduct =
            records.toList
              .groupMapReduce(_._1)(_._2)(_ + _)
              .toList
              .sortBy(_._2)
              .last

          comment(s"Which client buys '$product' the most?")
          check(mostConsumedProduct == (??, ??))
        }
      }

      section("Bad key design") {
        exercise("Data loss: store by product") {
          val orderStore = new InMemoryStore
          orders.foreach(order => orderStore.put(key = order.product, value = order.toJson))

          comment("How many orders are there in the CSV file?")
          check(orders.size == ??)

          comment("How many orders are there in the store?")
          check(orderStore.scan().get.size == ??)
        }
      }
    }
}

object Orders {
  val filename = "data/orders.csv"

  def load(): Seq[Order] =
    Using(Source.fromFile(filename)) { file =>
      val orders =
        for (line <- file.getLines().drop(1))
          yield toOrder(line)

      orders.toSeq
    }.get

  case class Order(
      id:        String,
      client:    String,
      timestamp: LocalDateTime,
      product:   String,
      price:     Double
  ) {
    def toJson: ujson.Obj =
      ujson.Obj(
        "id"        -> ujson.Str(id),
        "client"    -> ujson.Str(client),
        "timestamp" -> ujson.Str(timestamp.toString()),
        "product"   -> ujson.Str(product),
        "price"     -> ujson.Num(price)
      )
  }
  object Order {
    def fromJson(json: Value): Order =
      Order(
        id        = json("id").str,
        client    = json("client").str,
        timestamp = LocalDateTime.parse(json("timestamp").str),
        product   = json("product").str,
        price     = json("price").num
      )
  }

  def toOrder(line: String): Order = {
    val fields = line.split(",").toList
    Order(
      id        = fields(0),
      client    = fields(1),
      timestamp = LocalDateTime.parse(fields(2)),
      product   = fields(3),
      price     = fields(4).toDouble
    )
  }
}
