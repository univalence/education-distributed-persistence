package io.univalence.dataeng._07_kvstore

import ujson.Value

import scala.collection.mutable.TreeMap
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import java.time.LocalDateTime

case class Record(
    key:       String,
    value:     ujson.Value,
    timestamp: LocalDateTime,
    deleted:   Boolean
)

trait Store {
  type Key   = String
  type Value = ujson.Value

  def put(key: Key, value: Value): Try[Unit]

  def put(key: Key, value: String): Try[Unit]                = put(key, ujson.Str(value))
  def put(key: Key, value: Int): Try[Unit]                   = put(key, ujson.Num(value))
  def put(key: Key, value: Double): Try[Unit]                = put(key, ujson.Num(value))
  def put(key: Key, value: Boolean): Try[Unit]               = put(key, ujson.Bool(value))
  def putNull(key: Key): Try[Unit]                           = put(key, ujson.Null)
  def putNull(key: Key, values: Seq[ujson.Value]): Try[Unit] = put(key, ujson.Arr.from(values))

  def get(key: Key): Try[Record]
  def delete(key: Key): Try[Unit]
  def scan(): Try[Iterator[Record]]
  def scanFrom(key: Key): Try[Iterator[Record]]
  def scanPrefix(prefix: String): Try[Iterator[Record]]
}

class InMemoryStore extends Store {

  val data: TreeMap[Key, Record] = TreeMap.empty

  override def put(key: Key, value: Value): Try[Unit] =
    Try {
      val record =
        Record(
          key       = key,
          value     = value,
          timestamp = LocalDateTime.now(),
          deleted   = false
        )

      data(key) = record
    }

  override def get(key: Key): Try[Record] =
    data.get(key) match {
      case None        => Failure(new NoSuchElementException(key))
      case Some(value) =>
        if (value.deleted)
          Failure(new NoSuchElementException(key))
        else
          Success(value)
    }

  override def delete(key: Key): Try[Unit] =
    Try {
      data.remove(key).getOrElse(())
    }

  override def scan(): Try[Iterator[Record]] =
    Try {
      data.valuesIterator
    }

  override def scanFrom(key: Key): Try[Iterator[Record]] =
    Try {
      data.valuesIteratorFrom(key)
    }

  override def scanPrefix(prefix: String): Try[Iterator[Record]] =
    Try {
      data.view.filterKeys(_.startsWith(prefix)).valuesIterator
    }

}
