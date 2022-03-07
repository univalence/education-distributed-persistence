package io.univalence.dataeng.internal

object utils {

  /** Retries a function until it succeed or else return None. */
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
