package io.univalence.dataeng._03_in_process_db

import org.rocksdb._

import io.univalence.dataeng._02_storage.{KeyValueStore, User}
import io.univalence.dataeng._02_storage.pseudobin.pseudobin
import io.univalence.dataeng._02_storage.pseudobin.pseudobin.{Maybe, PseudobinSerde}
import io.univalence.dataeng._02_storage.pseudobin.pseudobin.PseudobinSerde._
import io.univalence.dataeng.internal.exercise_tools._

import scala.io.Source
import scala.util.{Try, Using}

import java.io.{File, FileInputStream}
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.zip.GZIPInputStream

/**
 * =RocksDB=
 * Now, we will declare a store based on RocksDB. RocksDB is an
 * in-process database. It means that instead of launching a full
 * third-party service, as you use to do with databases, like Oracle,
 * PostgreSQL, Cassandra... RocksDB is just a library to manage data in
 * memory and files, that you add in your project dependencies. Its
 * lifecycle follows your application runtime.
 *
 * RocksDB is interesting because it manages data like most NoSQL
 * databases (Cassandra, BigTable, HBase...). It has features of a
 * key-value store, but also features of a time series database.
 *
 * RocksDB has first been created in Facebook offices and is now an open
 * source project. It is a fork of another in-process database named
 * LevelDB and made by Google developers. RocksDB is written in C++ and
 * comes with a Java wrapper.
 *
 * ==Data storage==
 * RocksDB almost follows the LSM-tree (Log-Structured Merge-tree)
 * specifications. An '''LSM-tree''' is an organization of data between
 * the memory and mass storage, which is optimized for write accesses
 * (but less for read accesses), especially with huge volumes. In write
 * access, it is faster than B-Tree and B+Tree algorithm, used with more
 * traditional databases, knowing that such algorithms tend to fragment
 * intensively data with big amounts of data.
 *
 * Thus, RocksDb uses 3 kinds of data storage.
 *
 * {{{
 *     Data --+--> [Mutable Memtable]
 *             |      |
 *   MEMORY   |      +--> [Immutable Memtable]*
 *   =========|=============|=====================================
 *   VOLUME   |             +--> [SSTable & Index & Bloom filter]*
 *             +--> [WAL]
 * }}}
 *
 * ===Memtable===
 * The first storage is '''memtable'''. It is a memory area were data
 * are stored once they are put. RocksDB uses a skip list. Once the skip
 * list is full, it is converted first into an immutable skip list and
 * another mutable skip list is created. And when they are too many
 * immutable skip lists, they are stored in a SSTable.
 *
 * ===WAL===
 * The second kind of storage is the '''WAL''' (for ''Write Ahead
 * Log''). In parallel of the memtable, it also receives data once they
 * are put. The data are stored as is incrementally with no index. The
 * role of this file is only to rebuild the memtables that have not been
 * stored in SSTables, when you restart your application. The content of
 * the WAL is wipe out when the memtables are pushed to the SSTable.
 *
 * ===SSTable===
 * '''SSTable''' (''Sorted String Table'') is a file divided into
 * ''segments'' and each segment contains an __ordered__ subset of the
 * data. If you change a value in a record, RocksDB will rewrite the
 * whole segment at the end of the file.
 *
 * SSTable file comes with an __ordered__ index. Each index entry only
 * refers to the first position of the segment in the SSTable file. So,
 * it only refers the first record in the segment.
 *
 * {{{
 * INDEX        |      SSTABLE
 * -------------|--------------------------------------------
 * data       --|->    [data database delete deserialization]
 * codec      --|->    [codec couchbase encoder]
 * hashtable  --|->    [hashtable hbase immutable latency]
 * }}}
 *
 * So, if you search a specific key, you first have to look up for the
 * key equals or just below your key, to get the segment position. Then,
 * you find the corresponding entry in the segment by using a linear
 * search. In the figure above, if you search for the value `delete`,
 * you will use the key `data` and do the search in the segment
 * associated to `data`. If you search for `decoder`, it will be the
 * same: look up in the index and search in the associated segment. To
 * limit this kind of scenario, we use the Bloom filter, thast we will
 * in the section below.
 *
 * Regularly, you will need to clean the SStables. This automatically
 * done with a compaction job. This job is run on a regular basis. It
 * simply rebuild another SSTable file, from which it removes entries
 * marked as deleted and merges too small segments.
 *
 * The advantage of such structure is that it is fast and the index can
 * be loaded in memory even if you have a huge volume of data.
 *
 * In RocksDB, the index is saved in the same file as the SSTable file.
 * The compaction job is run in a separate thread every ~10 days.
 *
 * ===Bloom filter===
 * To improve data access, most key-value stores use a Bloom filter. A
 * '''Bloom filter''' is a structure with fixed size, that you feed when
 * you add new keys in a store. It can then tell if a key stored in the
 * file with more or less precision.
 *
 * {{{
 * Has this key K been registered?
 *
 *                   +--> YES (maybe)
 *   Bloom Filter --+
 *                   +--> NO  (absolutely)
 * }}}
 *
 * When you search a key, if the Bloom filter indicates that it does not
 * recognize it, then you the key is really not in your database and you
 * can tell that the key does not exist. If the Bloom filter answers
 * that the key is in the database, then the key may be in the database
 * (or not). In this case, you have to look up for the closest index and
 * scan the corresponding segment.
 *
 * The goal of the Bloom filter is to filter the search requests and
 * have some gain in read performances. RocksDB manages a Bloom filter
 * for every SSTable, it is stored in the SSTable file and load in
 * memory.
 *
 * ===Note===
 * RocksDB only sees data as byte arrays. So, you have to provide your
 * own codec to convert data.
 *
 * ==In this file==
 *   - We will start with a class that wraps the RocksDB API with the
 *     key-value store interface seen before.
 *   - You then have an exercise using this interface where you are
 *     asked to follow the evolution RocksDB files.
 *   - We then will leave the key-value store API to explore the time
 *     series capability of RocksDB.
 *   - A last part aims to understand the importance of key designing,
 *     when you have time series.
 */

class RocksDBKeyValueStore[K, V](
    databaseDirectory: File,
    keySerde:          PseudobinSerde[K],
    valueSerde:        PseudobinSerde[V]
) extends KeyValueStore[K, V]
    with AutoCloseable {

  private var database: RocksDB = _
  private var options: Options  = _

  locally {
    databaseDirectory.mkdirs()

    // before starting with RocksDB, we need to load the native library
    RocksDB.loadLibrary()

    /**
     * Note: every single RocksDb object created should be managed in a
     * try-with-resource structure (or Using). RocksDB objects are
     * allocated outside of the Java heap memory and are thus
     * unreachable from the GC. So, you must ensure that the method
     * `.close()` is called (they are [[AutoCloseable]]) on those
     * objects once they are not needed anymore.
     */

    // initialize a RocksDB key-value store
    options  = new Options().setCreateIfMissing(true)
    database = RocksDB.open(options, databaseDirectory.getPath)
  }

  /** Add or modify data in the store. */
  override def put(key: K, value: V): Unit = {
    val keyData   = keySerde.toPseudobin(key).getBytes(StandardCharsets.UTF_8)
    val valueData = valueSerde.toPseudobin(value).getBytes(StandardCharsets.UTF_8)

    database.put(keyData, valueData)
  }

  /** Try to find data according to a given key. */
  override def get(key: K): Try[V] = {
    val keyData = keySerde.toPseudobin(key).getBytes(StandardCharsets.UTF_8)

    for {
      valueData <- Try(database.get(keyData))
      value     <- valueSerde.fromPseudobin(new String(valueData, StandardCharsets.UTF_8))
    } yield value._1
  }

  /** Try to remove data with the given key. */
  override def delete(key: K): Unit = {
    val keyData = keySerde.toPseudobin(key).getBytes(StandardCharsets.UTF_8)

    database.delete(keyData)
  }

  /**
   * Here, we add another operation whose name is flush and which has
   * some signification for RocksDB, we will discover below.
   */
  def flush(): Unit =
    Using(new FlushOptions().setWaitForFlush(true)) { flushOptions =>
      database.flush(flushOptions)
    }.get

  override def close(): Unit = {
    options.close()
    database.close()
  }
}

/**
 * In the program below, we will see some basic usages of RocksDB as a
 * key-value store.
 *
 * You are asked to activate the exercises one by one and follow the
 * evolutions of the files in the database directory.
 */

object _01_rocksdb {

  val databaseDirectory = "data/target/_03_in_process_db/_01_rocksdb/_01_rocksdb"

  def main(args: Array[String]): Unit = {
    val databaseFile = new File(databaseDirectory)
    if (databaseFile.exists()) {
      // the storage already exists, we perform a cleanup
      databaseFile.listFiles().foreach(_.delete())
    }

    Using(new RocksDBKeyValueStore[String, User](databaseFile, STRING, User.serde)) { database =>
      exercise_ignore("Add entities in database") {
        database.put("123", User("123", "Jon", Some(32)))
        database.put("456", User("456", "Mary", None))
        database.put("789", User("789", "Tom", Some(33)))

        // try to get a record
        println(database.get("123"))

        // TODO take a look at the database directory and notice the organization
        /**
         * for this exercise do not hesitate to use commands like those
         * below to explore the inside of the file created by RocksDB:
         *   - less (less <file>)
         *   - hexdump (hexdump -C <file>)
         *
         * If you do not have those tools, there is a utility program in
         * this package name [[HexDumpMain]], that you can use.
         */
      }

      exercise_ignore("Flush effect") {
        database.flush()

        println(database.get("123"))
      }

      exercise_ignore("Update a record") {
        // update record and observe the modification
        database.put("456", User("456", "Mary", Some(25)))

        println(database.get("456"))
      }

      exercise_ignore("Flush the update") {
        database.flush()

        println(database.get("456"))
      }

      exercise_ignore("Delete a record") {
        // delete a record and try to get it
        database.delete("789")
      }

      exercise_ignore("Flush a delete") {
        // delete a record and try to get it
        database.flush()
      }
    }.get
  }
}

/**
 * =Time series=
 * Another interesting feature with RocksDB is its ability to store time
 * series. This ability comes with the capacity offered with skip list
 * and SSTables.
 *
 * '''Time series''' are just data evolving during time. It may
 * represent measures on an application (memory usage, CPU usage,
 * storage usage...) or on a computer (task manager, CPU temperature,
 * network I/Os...). It may also represent health data (heart beat rate
 * in a run, walk steps per day, sleep quality over a month...),
 * evolution of financial products (stocks, options, futures, forex...),
 * meteorological and climatic data...
 *
 * With time series, it is easier to follow the evolutions of indexes,
 * because time series are closed to continuous values (the notion of
 * ''continuous'' fully depends on the business domain). So, we use to
 * observe time series through graphs of different kinds, in a view to
 * have a better perception of series evolution.
 *
 * To query time series, you do not need to be precise. You just have to
 * provide the bounds (lower bound and upper bound) of an interval of
 * keys. In the case of RocksDB, such request results in an __iterator__
 * allowing you to explore ''lazily'' the values, whose key fits in the
 * described interval.
 *
 * ==Below==
 * There are two objects. The first one aims to store the memory heap
 * evolution of the current JVM during time, while filling a hash map.
 * The second one aims to explore those data, by using time series
 * capability of RocksDB.
 */
object _02_time_series_load_data {
  val databaseFile = new File("data/target/_03_in_process_db/_01_rocksdb/_02_time_series")

  def main(args: Array[String]): Unit = {
    if (databaseFile.exists()) {
      databaseFile.listFiles().foreach(_.delete())
    } else {
      databaseFile.mkdirs()
    }

    RocksDB.loadLibrary()

    Using(new Options().setCreateIfMissing(true)) { options =>
      Using(RocksDB.open(options, databaseFile.getPath)) { database =>
        // JMX bean that collect the current memory usage
        val memoryBean = ManagementFactory.getMemoryMXBean

        // this structure is only used to make more apparent momery usage
        var data = Map.empty[Long, Long]

        for (i <- 1 to 1000) {
          if (i % 50 == 0) {
            println(s"$i data loaded...")
          }

          // get current memory heap usage
          val usedHeap = memoryBean.getHeapMemoryUsage.getUsed

          // get current timestamp
          val time = System.currentTimeMillis()

          // store those data in memory
          data = data + (time -> usedHeap)

          // encode data
          val usedHeapData = LONG.toPseudobin(usedHeap).getBytes(StandardCharsets.UTF_8)
          val timeData     = LONG.toPseudobin(time).getBytes(StandardCharsets.UTF_8)

          // store in database
          database.put(timeData, usedHeapData)

          // wait a little to get the next memory heap usage
          Thread.sleep(100)
        }

        Using(new FlushOptions) { flushOptions =>
          database.flush(flushOptions)
        }.get
      }.get
    }.get
  }

}

/**
 * As RocksDB is based on ''skip list'' for memtable and SStable for
 * file storage, every single data stored is ordered according to the
 * key. This means that when you read all the data, data will come in
 * that order, independently of the write order.
 */
object _02_time_series_read_data {

  def main(args: Array[String]): Unit = {
    RocksDB.loadLibrary()

    Using(new Options().setCreateIfMissing(false)) { options =>
      Using(RocksDB.open(options, _02_time_series_load_data.databaseFile.getPath)) { database =>

        exercise_ignore("Get the whole content of the time series") {

          /**
           * At first, we will read all data and check it all appears in
           * order.
           */
          Using(database.newIterator()) { iterator =>
            readIterator(iterator)
          }
        }

        exercise_ignore("Get subset of a time series") {
          // TODO modify the start and end timestamps below
          /**
           * Do this, such that we get the memory usage during the first
           * 10 seconds after the first minutes.
           *
           * Eg. if you dataset starts at 16:20:30, we want the values
           * from 16:21:00 till 16:21:10.
           *
           * Note:
           *
           * The `.newIterator()` method also accepts a parameter which
           * is a [[ReadOptions]]. It is possible to modify
           * `ReadOption`, in a view to limit the dataset covered by the
           * iterator. You thus have to set a lower bound and/or an
           * upper bound.
           */

          val start = toEpochMilli("2022-02-14T16:20:50")
          val end   = toEpochMilli("2022-02-14T16:21:00")

          Using(
            new ReadOptions()
              .setIterateLowerBound(new Slice(LONG.toPseudobin(start)))
              .setIterateUpperBound(new Slice(LONG.toPseudobin(end)))
          ) { readOptions =>
            Using(database.newIterator(readOptions)) { boundedIterator =>
              readIterator(boundedIterator)
            }.get
          }.get
        }

      }.get
    }.get
  }

  /**
   * RocksDB provides an iterator in its API, but it has no connection
   * to the Java's nor Scala's iterator. RocksDB's iterators, once
   * created, are bidirectional. So, you have indicates if you start at
   * the beginning or at the end of dataset.
   *
   * At every step, you have to check if the iterator is valid
   * (equivalent of `.hasNext()` in Java. Then, you can get the byte
   * arrays for the key and the value, process then, and advance the
   * iterator cursor (to the following element with `.next()` or to
   * previous element with `.prev()`).
   *
   * Once used, you have to close the iterator.
   *
   * Note: it can be interesting to create an adaptor to RocksDB's
   * iterator that fits the Java's iterator and that makes it
   * [[AutoCloseable]].
   */
  def readIterator(iterator: RocksIterator): Unit = {
    iterator.seekToFirst()

    while (iterator.isValid) {
      val key   = iterator.key()
      val value = iterator.value()

      val result: Try[String] =
        for {
          time     <- LONG.fromPseudobin(new String(key, StandardCharsets.UTF_8)).map(_._1)
          usedHeap <- LONG.fromPseudobin(new String(value, StandardCharsets.UTF_8)).map(_._1)
        } yield {
          val instant = toLocalDateTime(time)
          s"$instant => $usedHeap"
        }

      println(result)

      iterator.next()
    }
  }

  def toLocalDateTime(milli: Long): LocalDateTime = {
    val instant = Instant.ofEpochMilli(milli)
    LocalDateTime.ofInstant(instant, ZoneId.of("Europe/Paris"))
  }

  def toEpochMilli(localDateTime: String): Long = {
    val dateTime = LocalDateTime.parse(localDateTime)

    dateTime
      .toInstant(
        ZoneId
          .of("Europe/Paris")
          .getRules
          .getOffset(dateTime)
      )
      .toEpochMilli
  }

}

/**
 * =Key design with time series=
 * In this exercise, we want to know the user who has first checked in
 * each venue each day, based on data in the file
 * `data/foursquare/checkins.txt.gz`. In this objective, we need to
 * group the dataset by venueId and ordered them by the timestamp. But
 * we only have one key.
 *
 * ==Building the key==
 * In most NoSQL databases, keys follows two properties:
 *
 *   1. They are unique.
 *   1. They are ordered.
 *
 * In the dataset we want to explore, the ordered is ensured by the
 * timestamp and the uniqueness is ensured by the venueId and the
 * timestamp (two users cannot check in a location at the same time,
 * there is always a winner ;) ).
 *
 * The problem is that we have only one key. To tackle this limitation,
 * we will generate a key for each checkin based on two fields (venueId
 * and timestamp) to emulate a compound key. As RocksDB uses bitwise
 * order, the leftmost part of the key will contain the venueId and the
 * rightmost part will contain the timestamp. Timestamp must appeared in
 * the key as a readable string starting with the year, the month, the
 * day... The format [[https://en.wikipedia.org/wiki/ISO_8601 ISO 8601]]
 * is a perfect match for such usage. By convention, we use to separate
 * the different parts of the key with a character like `#`.
 *
 * ==Below==
 * The first object aims to load data. Pay attention to the building of
 * the key. The second object requests the build database by using a
 * feature of RocksDB using the prefix of keys.
 */
object _03_checkins_load_data {
  // input data
  val dataFilename = new File("data/foursquare/checkins.txt.gz")

  val databaseFile = new File("data/target/_03_in_process_db/_01_rocksdb/_03_checkins")

  def main(args: Array[String]): Unit = {
    if (databaseFile.exists()) {
      databaseFile.listFiles().foreach(_.delete())
    } else {
      databaseFile.mkdirs()
    }

    RocksDB.loadLibrary()

    Using(new Options().setCreateIfMissing(true)) { options =>
      Using(RocksDB.open(options, databaseFile.getPath)) { database =>
        val in = new GZIPInputStream(new FileInputStream(dataFilename))
        Using(Source.fromInputStream(in)) { data =>
          val keyCounts = scala.collection.mutable.Map.empty[String, Int].withDefaultValue(0)
          time("Load checkins in database") {
            for (line <- data.getLines()) {
              // building checkin entity
              val data      = line.split("\t")
              val timestamp = foursquareTimestampToInstant(data(2))
              val checkin   = Checkin(data(0), data(1), timestamp, data(3).toInt)

              keyCounts.update(checkin.venueId, keyCounts(checkin.venueId) + 1)

              // building the key
              val keyData     = buildKey(checkin).getBytes(StandardCharsets.UTF_8)
              val checkinData = Checkin.serde.toPseudobin(checkin).getBytes(StandardCharsets.UTF_8)

              database.put(keyData, checkinData)
            }

            Using(new FlushOptions) { flushOptions =>
              database.flush(flushOptions)
            }.get
          }

          keyCounts.toList
            .sortBy { case (_, count) => -count }
            .take(10)
            .foreach(println)
        }.get
      }.get
    }.get
  }

  /**
   * Key is composed of the venueId and the checkin timestamp, separated
   * by a `#`.
   *
   * Reminder: we must not use Pseudobin serializer here, as this serde
   * tends to prefix data with its size encoded in a string. So, with
   * this serde the key ordering might look weird, and prefix search is
   * almost impossible. The best way to build a key is to encode the key
   * in a string and to convert it into a byte array.
   */
  def buildKey(checkin: Checkin): String = s"${checkin.venueId}#${timestampToString(checkin.timestamp)}"

  // timestamp is based on Pacific time
  private val pacificZone: ZoneId = ZoneId.of("America/Los_Angeles")

  def foursquareTimestampToInstant(timestamp: String): Instant = {
    val formatter = DateTimeFormatter.ofPattern("EEE LLL dd HH:mm:ss Z yyyy")
    val dateTime  = LocalDateTime.parse(timestamp, formatter)

    dateTime.toInstant(pacificZone.getRules.getOffset(dateTime))
  }

  def timestampToString(timestamp: Instant): String = LocalDateTime.ofInstant(timestamp, pacificZone).toString
}

/**
 * Reading part.
 *
 * Prefix search consists in giving a common prefix of the key you are
 * looking for.
 */
object _03_checkins_read_data {

  val databaseFile: File = _03_checkins_load_data.databaseFile

  def main(args: Array[String]): Unit = {
    /*
4adcda60f964a520934421e3,20
4f7b45ce1081951d719558a1,19
49ca9382f964a520bf581fe3,10
4b2d0e1af964a52037cd24e3,9
4be139c2c1732d7f797c5b9a,6
439ec330f964a520102c1fe3,6
4b89fb5df964a5200d5a32e3,5
4c25ea28905a0f4787466260,5
4ae0cb57f964a5208d8221e3,5
4b549b4ef964a52060c227e3,5
     */
    val venue1 = "4b2d0e1af964a52037cd24e3"

    RocksDB.loadLibrary()

    Using(
      new Options()
        .setCreateIfMissing(false)
        .useCappedPrefixExtractor(venue1.length)
    ) { options =>
      Using(RocksDB.open(options, databaseFile.getPath)) { database =>
        Using(
          new ReadOptions()
          //          .setIterateLowerBound(new Slice(s"$venue1#2012-04-03T19:00:00"))
          //          .setIterateUpperBound(new Slice(s"$venue1#2012-04-03T18:50"))
        ) { readOptions =>
          Using(database.newIterator(readOptions)) { iterator =>
            val prefixData = venue1.getBytes(StandardCharsets.UTF_8)

            // we move the iterator to the first entry with searched prefix
            iterator.seek(prefixData)

            // get the first key
            var key = new String(iterator.key(), StandardCharsets.UTF_8)
            // at each iteration, we should check the prefix is still the same
            while (iterator.isValid && key.startsWith(venue1)) {
              val value = new String(iterator.value(), StandardCharsets.UTF_8)

              // change the key presentation for readability concern
              val displayKey = key.split("#").mkString(", ")

              println(s"($displayKey) => $value")

              iterator.next()
              key = new String(iterator.key(), StandardCharsets.UTF_8)
            }
          }.get
        }.get
      }.get
    }.get
  }
}

case class Checkin(userID: String, venueId: String, timestamp: Instant, offset: Int)
object Checkin {
  val serde: PseudobinSerde[Checkin] =
    new PseudobinSerde[Checkin] {
      override def toPseudobin(checkin: Checkin): String =
        (
          STRING.toPseudobin(checkin.userID)
            + STRING.toPseudobin(checkin.venueId)
            + LONG.toPseudobin(checkin.timestamp.toEpochMilli)
            + INT.toPseudobin(checkin.offset)
        )

      override def fromPseudobin(data: pseudobin.Input): Maybe[Checkin] =
        for {
          (userId, input1)    <- STRING.fromPseudobin(data)
          (venueId, input2)   <- STRING.fromPseudobin(input1)
          (timestamp, input3) <- LONG.fromPseudobin(input2)
          (offset, input4)    <- INT.fromPseudobin(input3)
        } yield (
          Checkin(userID = userId, venueId = venueId, timestamp = Instant.ofEpochMilli(timestamp), offset = offset),
          input4
        )
    }
}
