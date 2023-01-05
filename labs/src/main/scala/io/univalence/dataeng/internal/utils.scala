package io.univalence.dataeng.internal

import scala.annotation.tailrec
import scala.util.Using

object utils {

  def using[A <: AutoCloseable](resource: A)(f: A => Unit): Unit = Using(resource)(f).get

  /** Retries a function until it succeed or else return None. */
  @tailrec
  def retry[T](fn: => T)(condition: T => Boolean)(n: Int = 3): Option[T] =
    if (condition(fn)) {
      Some(fn)
    } else {
      if (n > 1) {
        Thread.sleep(200)
        retry(fn)(condition)(n - 1)
      } else None
    }
}
