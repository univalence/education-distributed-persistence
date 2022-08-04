package io.univalence.microservice.common.entity

import com.google.gson.Gson

case class StockInfo(id: String, stockType: String, timestamp: Long, quantity: Int)
object StockInfo {
  val STOCK = "STOCK"
  val DELTA = "DELTA"
}
object StockInfoJson {
  val gson = new Gson()

  def serialize(stockInfo: StockInfo): String = gson.toJson(stockInfo)
  def deserialize(data: String): StockInfo =
    gson.fromJson(data, classOf[StockInfo])
}