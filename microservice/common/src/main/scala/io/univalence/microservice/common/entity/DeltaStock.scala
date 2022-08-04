package io.univalence.microservice.common.entity

import com.google.gson.Gson

case class DeltaStock(id: String, delta: Int)
object DeltaStockJson {
  val gson = new Gson()

  def serialize(stock: DeltaStock): String = gson.toJson(stock)
  def deserialize(doc: String): DeltaStock =
    gson.fromJson(doc, classOf[DeltaStock])
}
