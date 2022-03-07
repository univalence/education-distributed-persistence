package io.univalence.dataeng._02_storage

import io.univalence.dataeng._02_storage.pseudobin.pseudobin.{Input, PseudobinSerde}
import io.univalence.dataeng._02_storage.pseudobin.pseudobin.PseudobinSerde._
import io.univalence.dataeng.internal.exercise_tools._

import scala.annotation.tailrec
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

import java.io.{EOFException, File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}
import java.time.Instant
import java.util.zip.GZIPInputStream

/**
 * We have seen how to serialize data. Now, we will use the
 * serialization to store data in file. Storing data is a complicated
 * operation, even in memory. But in the case of memory, you get help
 * from the JVM to organize data within a list, a hash map, a tree map,
 * etc.
 *
 * To store data in a file, you get no help, except to read and write
 * byte arrays with a sequential manner. Thus, you need a format adapted
 * to data stored in a support with sequential access. This is what we
 * have seen with the serde part. Besides, you will also need to write
 * and read the formatted data from the file, and decide of an
 * organization of those data in it.
 *
 * ==Manage data in file==
 *
 * To make things easier, as we are in presence of a support with
 * sequential access, we will perceived a file as a list: a succession
 * of values written down one after the others by using the
 * serialization tools we have previously seen. But there is an
 * important difference with a list: it is very difficult to modify
 * already written data in file.
 *
 * TODO Why is that difficult to modify already written data in a file?
 *
 * So, the only possibility here is to add the modified data at the end
 * of the file and to consider the previous version as a old one to be
 * forgot.
 *
 * TODO What are the advantages and drawbacks of this approach?
 *
 * TODO Imagine a way to tackle the drawbacks.
 *
 * ==Record==
 *
 * To help organize data in a file, the values will be encapsulated in a
 * specific structure named ''records'', the same way network protocols
 * imply to encapsulate data in specific structures to send them. A
 * record here stores the data, a key associated to the data, plus some
 * additional metadata, like, for example, the timestamp of the record
 * creation, its checksum, its compression codec if any, etc.
 *
 * What has to be understood here is if an ''entity'' is a unit of data
 * that evolves in the time (like user information, an order in a store,
 * or the price of a product), the record represents the snapshot of
 * this entity at a precise instant. And a succession of record
 * represents the story of an entity.
 *
 * ===Record structure===
 * Here, for readability reason, each record is placed in a separated
 * line. A record in the file is prefixed by the size of the record in
 * characters, once serialized. The size includes the line-break
 * character (`\n`) at the end of the record. The size is expressed as
 * an [[INT]]. We have two metadata fields in the record:
 *
 *   - `removed` indicating if the the entity is considered as deleted.
 *   - `timestamp` indicating the moment the record has been created.
 *
 * TODO Why do we need the `removed` flag?
 *
 * Here is a schema of the content of the file.
 *
 * {{{
 *   +------------+-------------------+------------------+-----+-------+----+
 *   | size (INT) | removed (BOOLEAN) | timestamp (LONG) | key | value | \n |
 *   +------------+-------------------+------------------+-----+-------+----+
 *   | size (INT) | removed (BOOLEAN) | timestamp (LONG) | key | value | \n |
 *   +------------+-------------------+------------------+-----+-------+----+
 *   | ...                                                                  |
 *   +------------+-------------------+------------------+-----+-------+----+
 *   | size (INT) | removed (BOOLEAN) | timestamp (LONG) | key | value | \n |
 *   +------------+-------------------+------------------+-----+-------+----+
 * }}}
 */

case class Record[K, V](
    removed:   Boolean,
    timestamp: Instant,
    key:       K,
    value:     V
)

/**
 * For Record, we will need a specific serde, that will use the serde of
 * the key and the serde of the value part.
 */

object Record {

  def serde[K, V](keySerde: PseudobinSerde[K], valueSerde: PseudobinSerde[V]): PseudobinSerde[Record[K, V]] =
    new PseudobinSerde[Record[K, V]] {
      override def toPseudobin(record: Record[K, V]): String =
        (
          BOOLEAN.toPseudobin(record.removed)
            + LONG.toPseudobin(record.timestamp.toEpochMilli)
            + keySerde.toPseudobin(record.key)
            + valueSerde.toPseudobin(record.value)
        )

      override def fromPseudobin(data: Input): Try[(Record[K, V], Input)] =
        for {
          (removed, input1)   <- BOOLEAN.fromPseudobin(data)
          (timestamp, input2) <- LONG.fromPseudobin(input1)
          (key, input3)       <- keySerde.fromPseudobin(input2)
          (value, input4)     <- valueSerde.fromPseudobin(input3)
        } yield (Record(removed, Instant.ofEpochMilli(timestamp), key, value), input4)
    }

}

/**
 * Now that we have represented records, we can use it to manage our
 * data into a database, with key-value semantic.
 */

class FlatFileKeyValueStore[K, V](databaseFile: File, keySerde: PseudobinSerde[K], valueSerde: PseudobinSerde[V])
    extends KeyValueStore[K, V]
    with AutoCloseable {

  // TODO complete the line below with the awaiting serde
  val recordSerde: PseudobinSerde[Record[K, V]] = // ???
    Record.serde(keySerde, valueSerde)

  /**
   * Create (if it does not exists) and open the file with read/write
   * access.
   */
  val fileChannel: FileChannel =
    FileChannel.open(databaseFile.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)

  /** @inheritdoc */
  override def put(key: K, value: V): Unit = {
    // `put` method must first create a record based on parameters
    // TODO complete the line below with the awaiting Record instance
    val record: Record[K, V] = // ???
      Record(
        removed   = false,
        timestamp = Instant.now(),
        key       = key,
        value     = value
      )

    put(record)
  }

  /** @inheritdoc */
  override def get(key: K): Try[V] = {
    // start from the beginning of the file to find the record
    fileChannel.position(0L)

    val record: Try[Record[K, V]] = findRecordInFile(key, None)

    record.map(_.value)
  }

  /**
   * Try to remove data with the given key.
   *
   * Data deletion is based on the flag tombstone in the record. if this
   * flag is set, then we consider that the data is deleted. By default,
   * this flag is unset.
   */
  override def delete(key: K): Unit = {
    // start from the beginning of the file to find the record
    fileChannel.position(0L)

    val record: Try[Record[K, V]] = findRecordInFile(key, None)

    // delete a record consists in to store a new record with the same data and the flag tombstone set
    // TODO complete the line below
    val updatedRecord: Record[K, V] = // ???
      record.get.copy(removed = true)

    // then, the record marked as deleted is added at the end of the file
    put(updatedRecord)
  }

  /** @inheritdoc */
  override def close(): Unit = fileChannel.close()

  // ---- PRIVATE PART ----------------

  private val sizeBuffer: ByteBuffer = ByteBuffer.allocate(INT.size)

  private def put(record: Record[K, V]): Unit = {
    // TODO Generate the record data (do not forget the line-break character "\n").
    val recordData: String = // ???
      recordSerde.toPseudobin(record) + "\n"
    // TODO Generate the size of the record data serialized in a String.
    val sizeData: String = // ???
      INT.toPseudobin(recordData.length)

    // create the record line to store in the file
    val recordDataLine: String = sizeData + recordData

    // convert the record line into a byte array
    val rawContent: Array[Byte] = recordDataLine.getBytes(StandardCharsets.UTF_8)

    val buffer: ByteBuffer = ByteBuffer.wrap(rawContent)

    fileChannel
      // set position to the end of the file
      .position(fileChannel.size())
      // add record data
      .write(buffer)
  }

  /**
   * This method recursively walks through the file to find the last
   * version of an entity with a specific key.
   *
   * As we store data incrementally, we should not stop to the first
   * record with the corresponding key. We have to walk through the
   * whole file to check if there is a more recent version.
   *
   * Note: the method has the [[tailrec]] annotation. This indicates
   * that the method below must be tail-recursive. Scala optimizes
   * tail-recursive method, by converting the recursive method call into
   * a ''while'' loop.
   *
   * @param key
   *   key to look for
   * @param lastRecord
   *   the last version found of a record, if it has been found. Should
   *   be `None` at first call.
   */
  @tailrec
  private def findRecordInFile(key: K, lastRecord: Option[Record[K, V]]): Try[Record[K, V]] = {
    // ensure that the buffer position is zero
    sizeBuffer.rewind()

    // The first step is to simply try to get the next available record in the file
    // TODO use readRecordSize and readRecord to read a record from the file
    val record: Try[Record[K, V]] =
      for {
        size <- // ???
          readRecordSize(fileChannel, sizeBuffer)
        record <- // ???
          readRecord(fileChannel, size)
      } yield record

    // Second step: check record
    record match {
      case Success(r) =>
        /**
         * Here we found a record, but we have to check if it has the
         * corresponding key.
         *
         * It key matches, the collected record becomes the potential
         * value to return, and we continue to analyse the file to check
         * for any more recent version of the found entity.
         *
         * Else, we simply continue the walk through the file (with a
         * recursive call) with the same parameters.
         */

        // TODO Complete the code below according to the desciption above.
        // ???
        // we check the key
        if (r.key == key) {
          // if the key matches, we update the record to return and continue the walk
          findRecordInFile(key, Some(r))
        } else {
          // else, we just do a recursive call with no modification of parameters
          findRecordInFile(key, lastRecord)
        }

      // if we are at the end of the file
      case Failure(_: EOFException) =>
        /**
         * Here, we did not find any more record and we are at the end
         * of the file (causing EOFException). So, it is time to analyse
         * the record collected (ie. `lastRecord`).
         *
         *   - If `lastRecord` is `None`, we did not any record with the
         *     corresponding key and we just return
         *     [[NoSuchElementException]].
         *   - Else we have a record. So, we need to check the `removed`
         *     flag. If the flag is set, the record has been deleted and
         *     we send [[NoSuchElementException]]. If not, we have found
         *     the record.
         */

        // TODO Complete the code below according to the desciption above.
        // ???

        lastRecord
          .map { record =>
            // is the removed flag unset in the found record?
            if (!record.removed)
              Success(record)
            else {
              // if not, the record has not been found
              Failure(new NoSuchElementException(key.toString))
            }
          }
          // in case, no record has been found...
          .getOrElse(Failure(new NoSuchElementException(key.toString)))

      // any other error cases are considered as corruptions in the file
      case Failure(e) =>
        /**
         * For any other kind of failure, we just have to return the
         * failure.
         */

        Failure(e)
    }
  }

  /**
   * Try to read record size from the current position in a file.
   *
   * @param sizeBuffer
   *   the buffer that will contain the size of the record to read
   *   afterwards.
   * @return
   *   The number of byte read or [[EOFException]] if no more bytes are
   *   available.
   */
  private def readRecordSize(fileChannel: FileChannel, sizeBuffer: ByteBuffer): Try[Int] =
    // first, read all necessary bytes from the file
    if (fileChannel.read(sizeBuffer) >= 0) {
      // get record size as string
      val sizeData = new String(sizeBuffer.array(), StandardCharsets.UTF_8)
      // convert and return string into size as INT
      INT.fromPseudobin(sizeData).map(_._1)
    } else {
      // if the read task return a negative value, we are at the end of the file
      // so, there are no more data to read
      Failure(new EOFException)
    }

  /**
   * Try to read a record from the current position in a file.
   *
   * @param size
   *   the size in bytes of the record data.
   * @return
   *   A record or [[EOFException]] if no more bytes are available.
   */
  private def readRecord(fileChannel: FileChannel, size: Int): Try[Record[K, V]] = {
    // create a buffer that will receive the bytes of the record
    val buffer = ByteBuffer.allocate(size)

    if (fileChannel.read(buffer) >= 0) {
      // convert record bytes into a string
      val data = new String(buffer.array(), StandardCharsets.UTF_8)
      // deserialize and return record data
      recordSerde.fromPseudobin(data).map(_._1)
    } else { Failure(new EOFException()) }
  }

}

object _03_flat_file {

  // this the destination file to store data
  val databaseFilename = "data/target/_02_storage/_03_flat_file/data.db"

  def main(args: Array[String]): Unit = {
    val databaseFile = new File(databaseFilename)

    // we ensure that we start from an empty directory
    // so everytime we have to launch this program, we will not be
    // disturbed by the data of the previous execution
    databaseFile.getParentFile.mkdirs()
    if (databaseFile.exists()) {
      Files.delete(databaseFile.toPath)
    }

    // example of usage of the database file
    Using(new FlatFileKeyValueStore[String, User](databaseFile, STRING, User.serde)) { database =>
      // we first add some data
      database.put("123", User("123", "Jon", Some(32)))
      database.put("456", User("456", "Mary", None))
      database.put("789", User("789", "Tom", Some(33)))

      // try to get a record
      println(database.get("123"))

      // update record and observe the modification
      database.put("456", User("456", "Mary", Some(25)))
      println(database.get("456"))

      // try to get an unregistered key
      println(database.get("321"))

      // delete a record and try to get it
      database.delete("789")
      println(database.get("789"))
    }.get
  }

}

object _03_store_checkins {

  val dataFilename     = "data/foursquare/venues.txt.gz"
  val databaseFilename = "data/target/_02_storage/_03_flat_file/venues.db"

  def main(args: Array[String]): Unit = {
    val databaseFile = new File(databaseFilename)

    // ensure that we start from an existing empty directory
    databaseFile.getParentFile.mkdirs()
    if (databaseFile.exists()) {
      Files.delete(databaseFile.toPath)
    }

    /**
     * Read the content of the checkins file, and store it in a database
     * file.
     */
    Using(new FlatFileKeyValueStore(new File(databaseFilename), STRING, Venue.serde)) { database =>
      val in = new GZIPInputStream(new FileInputStream(dataFilename))
      Using(Source.fromInputStream(in)) { data =>
        // TODO in the lines below, complete the and note the times

        time("Writing venue data in file") {
          for (line <- data.getLines()) {
            // TODO convert each line into a Venue instance
            ???

            // TODO store the Venue instance into the database
            ???
          }
        }
      }

      time("Read venue data from file") {
        // TODO query the database with an existing venue ID
        ???
      }
    }
  }

  case class Venue(
      id:        String,
      latitude:  Double,
      longitude: Double,
      venueType: String,
      country:   String
  )

  object Venue {
    val serde: PseudobinSerde[Venue] =
      new PseudobinSerde[Venue] {
        override def toPseudobin(venue: Venue): String =
          (
            STRING.toPseudobin(venue.id)
              + DOUBLE.toPseudobin(venue.latitude)
              + DOUBLE.toPseudobin(venue.longitude)
              + STRING.toPseudobin(venue.venueType)
              + STRING.toPseudobin(venue.country)
          )

        override def fromPseudobin(data: Input): Try[(Venue, Input)] =
          for {
            (id, input1)        <- STRING.fromPseudobin(data)
            (latitude, input2)  <- DOUBLE.fromPseudobin(input1)
            (longitude, input3) <- DOUBLE.fromPseudobin(input2)
            (venueType, input4) <- STRING.fromPseudobin(input3)
            (country, input5)   <- STRING.fromPseudobin(input4)
          } yield (Venue(id, latitude, longitude, venueType, country), input5)
      }
  }

}

/**
 * ==My database file is growing endlessly :/==
 * This an important drawback here. What ever you are doing with such a
 * database (add data, update data, and even remove data), your file
 * will continue to grow.
 *
 * This cause a loss in performance when you want to search data. But we
 * will in the next lab that there is a solution here, with notion of
 * index.
 *
 * But to have a better control of the size of your file, we will see
 * that we use to run a parallel job, named ''compaction job'', that
 * physically removes unnecessary records.
 *
 * ==Why do we have a timestamp in the record?==
 * As you can notice in the [[Record]] case class above, there is a
 * timestamp associated. This timestamp is there not only for debugging
 * purpose. It might be used in a view to apply a TTL policy on data.
 *
 * '''TTL''' stands for ''Time-To-Live''. It is a period at the end of
 * which data will be automatically removed from the database. Like the
 * `removed` flag, the data deletion is only done logically in the find
 * function.
 *
 * With a TTL, you can easily turn your database into a persistent cache
 * and have a better control on the size of your database.
 */
