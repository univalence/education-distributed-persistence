package io.univalence.microservice.common.entity

import com.google.gson.Gson

case class ProjectedStock(id: String, timestamp: Long, quantity: Int)
object ProjectedStockJson {
  val gson = new Gson()

  def serialize(projectedStock: ProjectedStock): String =
    gson.toJson(projectedStock)
  def deserialize(data: String): ProjectedStock =
    gson.fromJson(data, classOf[ProjectedStock])
}
