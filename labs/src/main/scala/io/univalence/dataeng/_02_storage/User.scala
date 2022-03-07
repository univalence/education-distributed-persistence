package io.univalence.dataeng._02_storage

import io.univalence.dataeng._02_storage.pseudobin.pseudobin.{Input, PseudobinSerde}
import io.univalence.dataeng._02_storage.pseudobin.pseudobin.PseudobinSerde._

import scala.util.Try

case class User(id: String, name: String, age: Option[Int])

object User {

  val serde: PseudobinSerde[User] =
    new PseudobinSerde[User] {
      override def toPseudobin(user: User): String =
        STRING.toPseudobin(user.id) + STRING.toPseudobin(user.name) + NULLABLE(INT).toPseudobin(user.age)

      override def fromPseudobin(data: Input): Try[(User, Input)] =
        for {
          (id, input1)   <- STRING.fromPseudobin(data)
          (name, input2) <- STRING.fromPseudobin(input1)
          (age, input3)  <- NULLABLE(INT).fromPseudobin(input2)
        } yield (User(id, name, age), input3)
    }

}
