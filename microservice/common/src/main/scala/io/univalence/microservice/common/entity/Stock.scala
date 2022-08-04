package io.univalence.microservice.common.entity

import com.google.gson.Gson

case class Stock(id: String, quantity: Int)
object Stock {
  val transco =
    Map(
      "1"  -> "tomate",
      "2"  -> "chocolat",
      "3"  -> "banane",
      "4"  -> "ananas",
      "5"  -> "miel",
      "6"  -> "café",
      "7"  -> "thé",
      "8"  -> "sucre",
      "9"  -> "salade",
      "10" -> "broccoli",
      "11" -> "olive",
      "12" -> "crêpe",
      "13" -> "eau",
      "14" -> "jus",
      "15" -> "biscuit",
      "16" -> "orange"
    )
}
object StockJson {
  val gson = new Gson()

  def serialize(stock: Stock): String = gson.toJson(stock)
  def deserialize(doc: String): Stock = gson.fromJson(doc, classOf[Stock])
}
