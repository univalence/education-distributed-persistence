package io.univalence.dataeng._02_storage

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike

import io.univalence.dataeng._02_storage.pseudobin.pseudobin._
import io.univalence.dataeng._02_storage.pseudobin.pseudobin.PseudobinSerde._

import java.io.File
import java.time.LocalDateTime
import java.util.UUID

class _03_flat_fileTest extends AnyFunSuiteLike with BeforeAndAfterAll {

  val shouldConserveFile: Boolean = true

  val dbFile: File =
    if (shouldConserveFile) {
      val name = s"data/target/test/data-${LocalDateTime.now()}-${UUID.randomUUID()}.db"
      val file = new File(name)
      file.getParentFile.mkdirs()

      file
    } else File.createTempFile("data", ".db")

  val db = new FlatFileKeyValueStore[String, User](dbFile, STRING, User.serde)

  override def afterAll(): Unit = db.close()

  test("should add and retrieve an new entry") {
    val id   = UUID.randomUUID().toString
    val user = User(id, "Jon", 20)

    db.put(user.id, user)

    val result = db.get(id)

    assert(user == result.get)
  }

  test("should not retrieve an unregistered id") {
    val id = "inexisting"

    val result = db.get(id)

    assert(result.isFailure)
  }

  test("should update an existing entry") {
    val id = UUID.randomUUID().toString

    val userv1 = User(id, "Jon", 20)
    db.put(userv1.id, userv1)

    val userv2 = User(id, "Jon", 25)
    db.put(userv2.id, userv2)

    val result = db.get(id)

    assert(userv2 == result.get)
  }

  test("should remove an existing entity entry") {
    val id = UUID.randomUUID().toString

    val userv1 = User(id, "Jon", 20)
    db.put(userv1.id, userv1)

    db.delete(userv1.id)

    val result = db.get(id)

    assert(result.isFailure)
  }

  case class User(id: String, name: String, age: Int)
  object User {
    val serde: PseudobinSerde[User] =
      new PseudobinSerde[User] {
        override def toPseudobin(user: User): String =
          (
            STRING.toPseudobin(user.id)
              + STRING.toPseudobin(user.name)
              + INT.toPseudobin(user.age)
          )

        override def fromPseudobin(data: Input): Maybe[User] =
          for {
            (id, input1)   <- STRING.fromPseudobin(data)
            (name, input2) <- STRING.fromPseudobin(input1)
            (age, input3)  <- INT.fromPseudobin(input2)
          } yield (User(id, name, age), input3)
      }
  }

}
