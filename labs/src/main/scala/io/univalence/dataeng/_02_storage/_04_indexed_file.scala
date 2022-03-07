package io.univalence.dataeng._02_storage

import io.univalence.dataeng._02_storage.pseudobin.pseudobin.{Input, PseudobinSerde}
import io.univalence.dataeng._02_storage.pseudobin.pseudobin.PseudobinSerde._

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
 * We just have seen a way to store data in file. It simply add data at
 * the end of the file and does not modify any previous version of
 * entities or remove them. In fact, deletion and update are seen as
 * logical states.
 *
 * To add data, this approach is really efficient. But it also comes
 * with some drawbacks:
 *
 *   - Even if we remove data from the file, it is only a logical
 *     deletion. To tackle this, we can for example use a ''compaction
 *     job''. A compaction job is a background process that use to run
 *     once a week or two. It creates a new database file where it
 *     copies the data from the previous one, by omitting previous
 *     version of entries and deleted entries. But even here, it will
 *     not be as efficient as we might hope.
 *   - We have to scan all the file to find every single entity. And
 *     this a problem that we can solve.
 *
 * ==Index==
 * To improve access latency to the data, we can use an index. An
 * '''index''' is like an index at the end of a book, indicating the
 * pages where to find the terms you are looking for. In the present
 * case, for any given key appearing in the database file, the index
 * will give you the position in byte in the file. By considering a file
 * where all records are appended at the end of the file, the index can
 * only indicate the position of last available version. So, when you
 * want to access data, you just have to read the index and go directly
 * to the associated position to get the record, without scanning the
 * whole file.
 *
 * But where to store the index data?
 *
 * At runtime, the memory is the best location to store the index, as
 * operation in memory are fast enough. It could be stored in a simple
 * hash table. But to avoid reading the whole data file, in order to
 * rebuild the index data at load-time, it is also necessary to store
 * the index data in file.
 *
 * ==Manage indexed data in files==
 *
 * In comparison to the content of the data file seen in previous lab,
 * there is no modification here, and it works exactly the same way.
 *
 * For the index file, it is a bit different. There is one thing that
 * makes it easier to manage: every index entry as a structure that does
 * not change once serialized.
 *
 * ===Add a new entity===
 *
 * Every time you add a new entity in the database, you create also an
 * entry in the index. The new serialized index is simply add at the end
 * of the file. Once done, we add the index entry in memory. But what
 * happen if you modify an entity in the database?
 *
 * ===Get an entity===
 *
 * To get an entity, we just get the position of the last available
 * record from the given key and the in-memory index. And that is all.
 * We do not need to scan the index file, as it is only used to load in
 * memory index entries faster.
 *
 * ===Update an existing entity===
 *
 * When you do a modification of an entity, you add a new record in the
 * data file. But you must also modify the corresponding index with the
 * position of the added record. An advantage here is that the position
 * is expressed as a ''long integer''. And all long integers are encoded
 * in the exact same number of bytes. So, modifying the record position
 * in an index does not change the size of the index entry once
 * serialized in the index file. Thus, we can have an index file, which
 * size varies with the number entities logically stored and not with
 * the number of records physically stored in the data file.
 *
 * By the way, we have to retrieve the index entry in the file. To avoid
 * a full scan of the index file, we store in the in-memory index
 * position in the file in the in-memory index.
 *
 * To delete an entry is almost the same operation as update an entry,
 * except that we just add a record with the flag `removed` set.
 *
 * ==Index file structure==
 *
 * The index file structure follows the same logic as the data file
 * structure:
 *
 * {{{
 *   +------------+-----+---------------+----+
 *   | size (INT) | key | offset (LONG) | \n |
 *   +------------+-----+---------------+----+
 *   | size (INT) | key | offset (LONG) | \n |
 *   +------------+-----+---------------+----+
 *   | ...                                   |
 *   +------------+-----+---------------+----+
 *   | size (INT) | key | offset (LONG) | \n |
 *   +------------+-----+---------------+----+
 * }}}
 */

/**
 * As we have said, an index entry is just the key and a position of the
 * record in the data file.
 */
case class Index[K](key: K, position: Long)

object Index {

  def serde[K](keySerde: PseudobinSerde[K]): PseudobinSerde[Index[K]] =
    new PseudobinSerde[Index[K]] {

      override def toPseudobin(index: Index[K]): String =
        keySerde.toPseudobin(index.key) + LONG.toPseudobin(index.position)

      override def fromPseudobin(data: Input): Try[(Index[K], Input)] =
        for {
          (key, input1)      <- keySerde.fromPseudobin(data)
          (position, input2) <- LONG.fromPseudobin(input1)
        } yield (Index(key, position), input2)

    }

}

/**
 * This index case class is the one used in memory. It allows to
 * associate each key with two positions. `recordPosition` is the
 * position of the record in the data file. `indexPosition` is the
 * position of the index entry in the index file. This last one is used
 * to update the record position in the index file.
 */
case class IndexOffset(recordPosition: Long, indexPosition: Long)

/**
 * The record case class below is the same has the one is the previous
 * lab. We have just changed the name of the case class into
 * `IndexedRecord` to avoid name collision.
 */

case class IndexedRecord[K, V](removed: Boolean, timestamp: Instant, key: K, value: V)

object IndexedRecord {

  def serde[K, V](keySerde: PseudobinSerde[K], valueSerde: PseudobinSerde[V]): PseudobinSerde[IndexedRecord[K, V]] =
    new PseudobinSerde[IndexedRecord[K, V]] {
      override def toPseudobin(record: IndexedRecord[K, V]): String =
        (BOOLEAN.toPseudobin(record.removed)
          + LONG.toPseudobin(record.timestamp.toEpochMilli)
          + keySerde.toPseudobin(record.key)
          + valueSerde.toPseudobin(record.value))

      override def fromPseudobin(data: Input): Try[(IndexedRecord[K, V], Input)] =
        for {
          (removed, input1)   <- BOOLEAN.fromPseudobin(data)
          (timestamp, input2) <- LONG.fromPseudobin(input1)
          (key, input3)       <- keySerde.fromPseudobin(input2)
          (value, input4)     <- valueSerde.fromPseudobin(input3)
        } yield (IndexedRecord(removed, Instant.ofEpochMilli(timestamp), key, value), input4)
    }

}

/** Now we can create our own indexed database, based on 2 files. */
class IndexedFileKeyValueStore[K, V](
    databaseFile: File,
    indexFile:    File,
    keySerde:     PseudobinSerde[K],
    valueSerde:   PseudobinSerde[V]
) extends KeyValueStore[K, V]
    with AutoCloseable {

  val recordSerde: PseudobinSerde[IndexedRecord[K, V]] = IndexedRecord.serde(keySerde, valueSerde)
  val indexSerde: PseudobinSerde[Index[K]]             = Index.serde(keySerde)

  /**
   * Create (if it does not exists) and open the data file with
   * read/write access.
   */
  val fileChannel: FileChannel =
    FileChannel.open(databaseFile.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)

  /**
   * Create (if it does not exists) and open the index file with
   * read/write access.
   */
  val indexChannel: FileChannel =
    FileChannel.open(indexFile.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)

  /**
   * The index is represented in memory as a hash table associating keys
   * with their corresponding index.
   */
  val indexes: scala.collection.mutable.Map[K, IndexOffset] = scala.collection.mutable.Map.empty

  /**
   * Local initialization block. This kind of block run once an instance
   * is created.
   */
  locally(loadIndexes(indexes, indexChannel))

  /** @inheritdoc */
  override def put(key: K, value: V): Unit = {

    val index: Index[K] = // ???
      Index(key = key, position = fileChannel.size())
    val record: IndexedRecord[K, V] = // ???
      IndexedRecord(
        removed   = false,
        timestamp = Instant.now(),
        key       = key,
        value     = value
      )

    put(index, record)
  }

  /** @inheritdoc */
  override def get(key: K): Try[V] =
    findRecord(key).flatMap(record =>
      if (!record.removed) Success(record.value)
      else Failure(new NoSuchElementException)
    )

  /**
   * Try to remove data with the given key.
   *
   * Data deletion is based on the flag tombstone in the record. if this
   * flag is set, then we consider that the data is deleted. By default,
   * this flag is unset.
   */
  override def delete(key: K): Unit =
    (for {
      record <- findRecord(key)
      updatedRecord = record.copy(removed = true)
      offset <- Try(indexes(key))
      _      <- Try(put(Index(key, fileChannel.size()), updatedRecord))
    } yield ()).get

  /** @inheritdoc */
  override def close(): Unit = {
    fileChannel.close()
    indexChannel.close()
  }

  // ---- PRIVATE PART ----------------

  private val sizeBuffer: ByteBuffer = ByteBuffer.allocate(INT.size)

  private def put(index: Index[K], record: IndexedRecord[K, V]): Unit = {

    def buildBuffer[A](value: A, serde: PseudobinSerde[A]): ByteBuffer = {
      val data: String            = serde.toPseudobin(value) + "\n"
      val sizeData: String        = INT.toPseudobin(data.length)
      val content: String         = sizeData + data
      val rawContent: Array[Byte] = content.getBytes(StandardCharsets.UTF_8)

      ByteBuffer.wrap(rawContent)
    }

    /**
     * Store a record in the data file and returns its position in the
     * file.
     */
    def storeRecord(record: IndexedRecord[K, V]): Long = {
      val buffer: ByteBuffer   = buildBuffer(record, recordSerde)
      val recordPosition: Long = fileChannel.size()

      fileChannel
        // set position to the end of the file
        .position(recordPosition)
        // write record data
        .write(buffer)

      recordPosition
    }

    def storeIndex(index: Index[K], recordPosition: Long): Unit = {
      val indexBuffer: ByteBuffer = buildBuffer(index, indexSerde)

      val indexPosition: Long =
        indexes
          // find offset from the key
          .get(index.key)
          // get the offset in the index file
          .map(_.indexPosition)
          // if no entry found in index file, simply add index at the end
          .getOrElse(indexChannel.size())

      // write index entry in the file
      indexChannel
        .position(indexPosition)
        .write(indexBuffer)

      // update index entry in memory
      indexes(index.key) = IndexOffset(recordPosition = recordPosition, indexPosition = indexPosition)
    }

    val recordPosition: Long = storeRecord(record)
    storeIndex(index, recordPosition)
  }

  // try to read record size from the file
  private def readRecordSize(key: K, sizeBuffer: ByteBuffer): Try[Int] =
    // first, read all necessary bytes from the file
    if (fileChannel.read(sizeBuffer) >= 0) {
      // get record size as string
      val sizeData = new String(sizeBuffer.array(), StandardCharsets.UTF_8)
      // convert and return string into size as INT
      INT.fromPseudobin(sizeData).map(_._1)
    } else {
      // if the read task return a negative value, we are at the end of the file
      // so, there are no more data to read
      Failure(new EOFException(key.toString))
    }

  // try to read a record from the file
  private def readRecord(key: K, size: Int): Try[IndexedRecord[K, V]] = {
    // create a buffer that will receive the bytes of the record
    val buffer = ByteBuffer.allocate(size)
    buffer.rewind()

    if (fileChannel.read(buffer) >= 0) {
      // convert record bytes into a string
      val data = new String(buffer.array(), StandardCharsets.UTF_8)
      // deserialize and return record data
      recordSerde.fromPseudobin(data).map(_._1)
    } else { Failure(new EOFException(key.toString)) }
  }

  private def findRecord(key: K): Try[IndexedRecord[K, V]] = {
    sizeBuffer.rewind()

    for {
      offset <- Try(indexes(key))
      _      <- Try(fileChannel.position(offset.recordPosition))
      size   <- readRecordSize(key, sizeBuffer)
      record <- readRecord(key, size)
    } yield record
  }

  private def loadIndexes(indexes: scala.collection.mutable.Map[K, IndexOffset], indexChannel: FileChannel): Unit = {
    // if index file is empty
    if (indexChannel.size() == 0L) {
      // TODO then, initialize index file from record file content
      ???
    }

    // TODO fill in indexes map from the content of index file
    ???
  }

}

object _04_indexed_file {

  val databaseDirname  = "data/target/_02_storage/_04_indexed_file"
  val databaseFilename = databaseDirname + "/data.db"
  val indexFilename    = databaseDirname + "/data.idx"

  def main(args: Array[String]): Unit = {
    val databaseFile = new File(databaseFilename)
    val indexFile    = new File(indexFilename)

    // ensure that we start from an existing empty directory
    databaseFile.getParentFile.mkdirs()
    if (databaseFile.exists()) {
      Files.delete(databaseFile.toPath)
    }
    if (indexFile.exists()) {
      Files.delete(indexFile.toPath)
    }

    Using(new IndexedFileKeyValueStore[String, User](databaseFile, indexFile, STRING, User.serde)) { database =>
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

      val user: User = database.get("123").get
      database.put("123", user.copy(age = None))
    }.get
  }

}

object _04_store_venues {

  val dataFilename     = "data/foursquare/venues.txt.gz"
  val databaseDirname  = "data/target/_02_storage/_04_indexed_file"
  val databaseFilename = databaseDirname + "/venues.db"
  val indexFilename    = databaseDirname + "/venues.idx"

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
    Using(new IndexedFileKeyValueStore(new File(databaseFilename), new File(indexFilename), STRING, Venue.serde)) {
      database =>
        val in = new GZIPInputStream(new FileInputStream(dataFilename))
        Using(Source.fromInputStream(in)) { data =>
          for (line <- data.getLines()) {
            // TODO convert each line into Venue instance
            ???

            // TODO store the Venue instance into the database
            ???
          }
        }.get

        // TODO query the database
        ???
    }.get
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
        override def toPseudobin(value: Venue): String = ???

        override def fromPseudobin(data: Input): Try[(Venue, Input)] = ???
      }
  }

}

/**
 * ==Improve index management==
 * In the approach presented here, some drawbacks appear:
 *   1. We have to manage two files and there might have
 *      desynchronization between them.
 *   1. There is a somewhat better file organization.
 *   1. There is an available optimization from the OS to store data in
 *      file.
 *   1. The index file content may not fit in memory.
 *
 * For the first point, it is possible that due to some failure, only
 * the index update and the record update has partially succeeded. This
 * may lead to a data corruption. To add a checksum in record metadata
 * helps to check for such correction. If this has been detected, the
 * database may stop with an error indicating to the user that he must
 * rebuild the index from the data file, before running the database
 * again. The index rebuilding might a long operation.
 *
 * For the second point, there are other file organizations, that might
 * be more efficient. There are B-tree and B+tree abstractions that are
 * used with classical databases and som file-systems.
 *
 * For the third point, some OS can map directly some memory area with
 * some mass storage area. The mapped memory area has still a random
 * access. The operation here are somewhat slower than memory access,
 * but they are faster than a usual disk access. The [[java.nio]]
 * package proposes the class [[java.nio.MappedByteBuffer]] created by
 * calling [[java.nio.channels.FileChannel#map]]. It represents a byte
 * buffer directly mapped to a file on a mass storage.
 *
 * We will talk about the last point in the next lab.
 */
