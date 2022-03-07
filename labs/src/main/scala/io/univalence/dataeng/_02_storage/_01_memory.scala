package io.univalence.dataeng._02_storage

import scala.util.Try

/**
 * =In-Memory database=
 * We start here with the storage of data in memory, especially for data
 * organized into key-value pair. We will see that different solutions
 * are proposed.
 *
 * ==Key-value==
 * The data we consider is divided into '''records''', where each record
 * contains all information necessary for an individual entity. Usually,
 * a record is divided into named fields. But it can sometimes contains
 * data like audio, video, or any binary information. Those records are
 * uniquely identified with key. In other words, the key helps to
 * retrieve a specific record.
 *
 * A '''key''' used to be an integer (eg. transaction number), a string
 * (eg. bank account, UUID), or more complex structure composed of
 * different fields (eg. to identify a house, you need a street, a
 * number, the city, and the country).
 *
 * ==Memory storage==
 * There are many data structures to store data in memory, that you
 * should already know, like the list, the hash table, the set...
 *
 * ==In this file==
 * We propose 4 implementations of in-memory storage, using simple
 * array, hash table, treemap, and skip list. At the end, you will be
 * ask to do a benchmark.
 *
 * To help you, you will find unit tests in `labs/src/test/scala`.
 */

object _01_array {
  def main(args: Array[String]): Unit = {}

  /**
   * The first approach consists in using an array of key-value pairs.
   *
   * An array is a linear structure to add an element is done in O(1),
   * but the other kinds of operations are done in O(N).
   *
   * TODO complete the implementation below
   */
  class ArrayKeyValueStore[K, V] extends KeyValueStore[K, V] {
    val data = scala.collection.mutable.ArrayBuffer.empty[(K, V)]

    override def put(key: K, value: V): Unit = ???

    override def get(key: K): Try[V] = ???

    override def delete(key: K): Unit = ???
  }
}

object _01_hashtable {
  def main(args: Array[String]): Unit = {}

  /**
   * This approach consists in using a hash table, which indexes stored
   * entities directly with the key, based on its hash to get its offset
   * in an array. If there is an offset collision (ie. when two
   * different keys produce the same hash), the values are added in a
   * list associated to this offset.
   *
   * So, operations in hash table use to be performed between O(1) and
   * O(N).
   *
   * TODO complete the implementation below
   */
  class MapKeyValueStore[K, V] extends KeyValueStore[K, V] {
    val data = scala.collection.mutable.Map.empty[K, V]

    override def put(key: K, value: V): Unit = ???

    override def get(key: K): Try[V] = ???

    override def delete(key: K): Unit = ???
  }
}

object _01_treemap {
  def main(args: Array[String]): Unit = {}

  /**
   * Treemap is a very interesting structure. It acts like a hash table.
   * But there are other additional properties:
   *
   *   - All elements are sorted according to the key.
   *   - Elements are disposed inside a balanced tree (ie. all paths
   *     from the root to a leaf differs of at most one node).
   *
   * When data are sorted by the key, it allows you to query the
   * smallest element and the bigger one. You can also query a subset by
   * range of keys. We will see this kind of query later on, when we
   * will see time series.
   *
   * Operations in a treemap are performed in O(log(N)).
   *
   * There are different known implementation of a treemap. The one use
   * in Java and in Scala is based on Red-Black tree.
   *
   * The Scala implementation needs you to provide an implicit ordering.
   * But most of predefined object in Scala (like `Int`, `Double`, or
   * `String`) comes with such an ordering.
   *
   * TODO complete the implementation below
   */
  class TreeKeyValueStore[K: Ordering, V] extends KeyValueStore[K, V] {
    val data = scala.collection.mutable.TreeMap.empty[K, V]

    override def put(key: K, value: V): Unit = ???

    override def get(key: K): Try[V] = ???

    override def delete(key: K): Unit = ???

    def min: (K, V) = ???

    def max: (K, V) = ???

    def range(from: K, to: K): Iterator[(K, V)] = ???
  }

}

object _01_skiplist {
  import java.util.concurrent.ConcurrentSkipListMap

  def main(args: Array[String]): Unit = {}

  /**
   * Skip list is a probabilist data structure with same properties as a
   * treemap and same complexity in time. The Java implementation of
   * skip list is not as performant as the treemap one. But it has an
   * interesting behavior in concurrent environment: many parallel
   * threads can access the data with no data corruption, whereas with
   * treemap only one thread can access the data at a time.
   *
   * Skip list is often used in NoSQL database to manage data in memory.
   *
   * TODO complete the implementation below
   */
  class SkipListKeyValueStore[K: Ordering, V] extends KeyValueStore[K, V] {
    val data = new ConcurrentSkipListMap[K, V]((k1: K, k2: K) => Ordering[K].compare(k1, k2))

    override def put(key: K, value: V): Unit = ???

    override def get(key: K): Try[V] = ???

    override def delete(key: K): Unit = ???

    def min: (K, V) = ???

    def max: (K, V) = ???

    def range(from: K, to: K): Iterator[(K, V)] = ???
  }

}

/**
 * ==Benchmark==
 * In this repo, we will use the tool JMH (Java Microbenchmark Harness)
 * through a SBT plugin. JMH is a tool made by Oracle and is used as a
 * referential tool to measure performance of JVM program fragments.
 *
 * All the benchmarks are in the directory `benchmark`.
 *
 * To use JMH, you have to run SBT in a terminal and type those commands
 * {{{
 * > project benchmark
 * > Jmh/run -i 5 -wi 3 -f1 -t1 _02_storage._01_memory.*SystemBenchmark.*
 * }}}
 *
 * The options are:
 *   - `-i` number of iterations (JMH retries the benchmark many times)
 *   - `-wi` number of warmup iterations (JMH warms your JVM up with
 *     your benchmark function before measurement)
 *   - `-f` number of fork (ie. number times warmup iterations and
 *     benchmark iterations should be repeated sequentially)
 *   - `-t` number of thread (ie. number times warmup iterations and
 *     benchmark iterations should be repeated in parallel)
 *
 * The benchmarks give you statistics in ops/s (count of invocations per
 * seconds) for each benchmark function. You will see the ''Score'' (ie.
 * average measurement) and the ''Error'' (ie. standard deviation).
 *
 * Note: a specific benchmark file is provided: `SystemBenchmark`. This
 * benchmark aims to give you a estimation of the performance of your
 * computer on integer and string operations.
 */

// TODO launch benchmarks, convert measures in micro- or nano-seconds
// TODO check for the fastest structure
