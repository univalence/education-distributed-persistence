package io.univalence.microservice.common.repository

import io.univalence.microservice.common.entity.ProjectedStock

/**
 * A repository is a usual design pattern that add a layer to decouple
 */
trait ProjectedStockRepository {

  def findById(id: String): Option[ProjectedStock]

  def findAll(): Iterator[ProjectedStock]

  def save(projectedStock: ProjectedStock): Unit

  def saveAll(stocks: List[ProjectedStock]): Unit

}
