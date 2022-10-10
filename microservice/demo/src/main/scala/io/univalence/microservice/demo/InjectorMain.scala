package io.univalence.microservice.demo

import io.univalence.microservice.common.args.readArgs
import io.univalence.microservice.common.entity.{DeltaStock, DeltaStockJson, Stock, StockJson}
import okhttp3._

import scala.util.{Random, Using}

/** Inject random stock data in the application.
  */
object InjectorMain {

  /** Internal stock state to ensure that each stock quantity will not be less than zero.
    */
  val stockDb: scala.collection.mutable.Map[String, Int] =
    scala.collection.mutable.Map.empty

  def main(args: Array[String]): Unit = {
    val parameters = readArgs(args.toList).toMap

    val ingestPort = parameters.get("ingest.port").flatMap(_.map(_.toInt)).getOrElse(Configuration.IngestHttpPort)

    val httpClient = new OkHttpClient.Builder().build()

    println("--> Initialize stocks")

    initStocks(httpClient, ingestPort)

    println("--> Send stocks and deltas")

    /** Endlessly sends data...
      */
    while (true) {

      /** Ensure to have an average of stock update 30% of the time, and delta 70% of the time.
        */
      if (Random.nextInt(100) < 100) {
        val stock = nextStock

        println(s"Serializing: $stock")
        sendStock(stock.id, StockJson.serialize(stock), httpClient, ingestPort)
      } else {
        val delta = nextDeltaStock

        // ensure that stock for each prduct is positive
        if (stockDb(delta.id) + delta.delta >= 0) {
          println(s"Serializing: $delta")
          sendDelta(delta.id, DeltaStockJson.serialize(delta), httpClient, ingestPort)
          // update stock value
          stockDb(delta.id) = stockDb(delta.id) + delta.delta
        } else {
          println(s"Not enough stock for product#${delta.id}")
        }
      }

      pause()
    }
  }

  /** Initialize the whole application with a basis stock value.
    */
  def initStocks(httpClient: OkHttpClient, ingestPort: Int): Unit = {
    for (id <- Stock.transco.keys) {
      val stock = createStock(id)
      stockDb(id) = stock.quantity

      println(s"Observed stock for product#$id: ${stock.quantity}")

      sendStock(id, StockJson.serialize(stock), httpClient, ingestPort)

      pause()
    }
  }

  /** Some random pause...
    */
  def pause(): Unit = {
    val waitTime = Random.nextInt(1000) + 1000
    Thread.sleep(waitTime)
  }

  def sendDelta(id: String, doc: String, client: OkHttpClient, ingestPort: Int): Unit =
    sendDoc(
      doc,
      s"http://${Configuration.ServiceHost}:$ingestPort/deltas/$id",
      client
    )

  def sendStock(id: String, doc: String, client: OkHttpClient, ingestPort: Int): Unit =
    sendDoc(
      doc,
      s"http://${Configuration.ServiceHost}:$ingestPort/stocks/$id",
      client
    )

  def sendDoc(doc: String, url: String, client: OkHttpClient): Unit = {
    println(s"Sending to $url: $doc")

    val body = RequestBody.create(doc, MediaType.parse("application/json"))
    val request = new Request.Builder()
      .url(url)
      .post(body)
      .build()

    Using(client.newCall(request).execute()) { response =>
      if (response.isSuccessful) {
        println(s"Success: data: $doc")
      } else {
        println(
          s"Error: ${response.message()} (${response.code()}) - data: $doc"
        )
      }
    }.get
  }

  def createStock(id: String): Stock =
    Stock(id = id, quantity = Random.nextInt(400) + 100)

  def createDeltaStock(id: String): DeltaStock =
    DeltaStock(id = id, delta = Random.nextInt(10) - 5)

  def nextStock: Stock = {
    val id = Random.shuffle(Stock.transco.keys.toList).head

    val stock = createStock(id)

    println(s"Observed stock: ${stock.quantity} for product#$id")

    stock
  }

  def nextDeltaStock: DeltaStock = {
    val id         = Random.shuffle(Stock.transco.keys.toList).head
    val deltaStock = createDeltaStock(id)

    if (deltaStock.delta < 0)
      println(s"Someone want to buy ${-deltaStock.delta} of product#$id")
    else
      println(s"Someone deliver ${deltaStock.delta} of product#$id")

    deltaStock
  }

}
