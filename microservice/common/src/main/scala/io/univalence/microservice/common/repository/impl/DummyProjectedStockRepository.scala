package io.univalence.microservice.common.repository.impl

import io.univalence.microservice.common.entity.ProjectedStock
import io.univalence.microservice.common.repository.ProjectedStockRepository

class DummyProjectedStockRepository extends ProjectedStockRepository {

  override def findAll(): Iterator[ProjectedStock] = Iterator.empty

  override def findById(id: String): Option[ProjectedStock] = None

  override def save(projectedStock: ProjectedStock): Unit = ()

  override def saveAll(stocks: List[ProjectedStock]): Unit = ()

}
