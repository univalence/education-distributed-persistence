package io.univalence.dataeng._03_in_process_db

import org.mapdb._

import io.univalence.dataeng._02_storage.User
import io.univalence.dataeng.internal.exercise_tools._

import scala.util.Using

import java.io.File

/**
 * =MapDB=
 * MapDB is another in-process database. Instead of implementing the
 * LSM-tree specification, MapDB is rather closed to COLA tree (Cache
 * Oblivious Look-Ahead tree). It is an enhancement of the B-tree
 * approach.
 *
 * MapDB has been developed in Kotlin. Data serialization can be based
 * on Java serialization.
 */

object _01_mapdb {
  def main(args: Array[String]): Unit = {
    val databaseFile = new File("data/target/_03_in_process_db/_02_mapdb/_01_mapdb.db")
    if (databaseFile.exists()) {
      databaseFile.delete()
    } else {
      databaseFile.getParentFile.mkdirs()
    }

    Using(
      DBMaker
        .fileDB(databaseFile)
        .fileMmapEnable()
        .make()
    ) { db =>
      val store: HTreeMap[String, User] =
        db.hashMap[String, User]("users", Serializer.STRING, Serializer.JAVA.asInstanceOf[Serializer[User]])
          .createOrOpen()
      exercise_ignore("Add entities in database") {
        store.put("123", User("123", "Jon", Some(32)))
        store.put("456", User("456", "Mary", None))
        store.put("789", User("789", "Tom", Some(33)))

        // try to get a record
        println(store.get("123"))

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

      exercise_ignore("Update a record") {
        // update record and observe the modification
        store.put("456", User("456", "Mary", Some(25)))

        println(store.get("456"))
      }

      exercise_ignore("Delete a record") {
        // delete a record and try to get it
        store.remove("789")
      }
    }.get
  }
}
