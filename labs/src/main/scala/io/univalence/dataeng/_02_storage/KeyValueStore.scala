package io.univalence.dataeng._02_storage

import scala.util.Try

trait KeyValueStore[K, V] {

  /** Add or modify data in the store. */
  def put(key: K, value: V): Unit

  /** Try to find data according to a given key. */
  def get(key: K): Try[V]

  /** Try to remove data with the given key. */
  def delete(key: K): Unit

}
