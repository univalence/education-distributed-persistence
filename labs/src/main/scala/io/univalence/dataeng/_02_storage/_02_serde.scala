package io.univalence.dataeng._02_storage

import io.univalence.dataeng._02_storage.pseudobin.pseudobin
import io.univalence.dataeng._02_storage.pseudobin.pseudobin.{Input, Maybe, PseudobinSerde}
import io.univalence.dataeng._02_storage.pseudobin.pseudobin.PseudobinSerde._
import io.univalence.dataeng.internal.exercise_tools._

import scala.util.Try

/**
 * =Serde / Codec=
 * Before we start storing data in files, we will see how to manage the
 * mapping between data in memory and data in file. This mapping is
 * based on a tool that we will named a '''serde''', for
 * SERialization/DEserialization. We might also call it a '''codec''',
 * for enCOde/DECode.
 *
 * Some serialization format are well known, like CSV, XML, or JSON.
 * Those format are readable and flexible (as by some degrees, we can
 * modify the presentation of the data without changing their value).
 * But this flexibility comes with a cost in terms of performance and
 * size.
 *
 * On the other side, binary formats are conceived with less
 * flexibility, but on the other hand, it is more concise (especially
 * with scalar values) and design for performances. Example of binary
 * format are Avro, Protobuf, and Thrift. Binary format serialization
 * consists in converting your memory model (which support random
 * accesses) into a byte array, representing your data in a file (with
 * sequential accesses), or on a network.
 *
 * We will not use a real binary format here. Instead, we will use a
 * kind of pseudo-binary format (or pseudobin), which is a (almost)
 * readable format based on a string, but with far less flexibility than
 * a human readable format, so the serialization and the deserialization
 * is fast enough.
 */
object _02_serde {

  def main(args: Array[String]): Unit = {

    section("Serialization of an integer") {

      // Here is how it works...
      // We start with an integer.
      val intValue: Int = 42

      /**
       * We use the corresponding serde (`INT`) and call `toPseudobin`
       * (the serialization operation) to get a representation into a
       * string. (Usually, it would be a byte array.)
       */
      val intData: String = INT.toPseudobin(intValue)
      // An integer is encoded on a string of 10 characters left-padded with spaces.

      // TODO activate the exercise below
      // TODO and guess what will be the result of the serialization of the integer.
      exercise_ignore("Serialize an integer") {
        // println(s"""serialized integer value: "$intData"""")
        check(intData == "??")
      }

      /**
       * Now we can start from the string representation to deserialize
       * the integer. But let's talk first about the returned type of
       * the deserialization operation.
       *
       * By essence, the data in memory will not be corrupted. The
       * consistency of the data in memory is ensured by the execution
       * environment (here, the JVM). However, nothing can ensure the
       * consistency of data stored in a file except your application.
       * Even the filesystem only ensures the overall organization of
       * data on the disks, into directories and files. But it does not
       * ensure the consistency of the content of a file. So when
       * reading data from a file (or even from the network), you may
       * have corrupted data, because of different reason like they have
       * been generated from a previous version of the serializer, the
       * string did not contain the awaiting type, or someone has
       * changed it by hand... So that is why, `fromPseudobin` (the
       * deserialization operation) returns a
       * [[io.univalence.dataeng._02_storage.pseudobin.pseudobin.Maybe]]
       * instance.
       *
       * `Maybe` is defined as a type alias. It is first a
       * [[scala.util.Try]] type. The `Try` type is a type that can be
       * derived into an instance of [[scala.util.Success]] with a value
       * or into an instance of [[scala.util.Failure]] with an
       * exception, thus representing that a process may succeed or
       * fail. So, if your data is corrupted, you will get a `Failure`
       * with an exception after calling `fromPseudobin`. Otherwise, you
       * will get a `Success` with the awaiting value.
       *
       * For the value part in the example below, it is in fact composed
       * of an `Int` and an `Input`. The `Int` is your real value
       * extracted from the input data. `Input` is the state of your
       * input data (ie. the content of data and an offset, indicating
       * where the next reading must start from). It ensures a
       * continuation with other possible calls of another serde if
       * there are more data to read (but, this is not the case here).
       * So, our deserialization process is composable, with other
       * deserialization processes.
       *
       * Note: the `Input` is designed according to a pattern named
       * ''State type''. A state is whatever data that may evolve during
       * time. Here, Input evolves, because the offset increases while
       * we are reading data. To ensure ''immutability'' and
       * ''determinism'' (ie. ''referential transparency''), every
       * single function that results in evolving the state and returns
       * a value has the signature `S => (A, S)`, where
       *
       *   - `S` is the type of the state.
       *   - `A` the type of the value.
       *
       * In our deserialization process,
       *
       *   - `fromPseudobin` is the function that evolves the state.
       *   - `Input` is the type of the state.
       *   - `Int` is the type of the value.
       */
      val intResult: Maybe[Int] = INT.fromPseudobin(intData)

      exercise_ignore("Deserialize an integer") {
//        println(s"deserialized int result: $intResult")
        check(intResult == ??)
        // use .get only if you are sure that the deserialization has succeeded.
//        println(s"deserialized int value only: ${intResult.get._1}")
        check(intResult.get._1 == ??)
      }

      exercise_ignore("Failing deserialization of an integer") {
        val badFormat             = "[whatever]"
        val badResult: Maybe[Int] = INT.fromPseudobin(badFormat)

        check(badResult == ??)

        // What happen if you call .get on badResult?
      }
    }

    section("Serialization of string") {
      val stringValue: String = "Hello, world!"

      /**
       * A string is another another level. The difficulty here is to
       * represent values with a variable size. There are two main
       * approaches here: using a tag that indicates the end of the
       * string (like in C), prefixed the string with an integer
       * indicating its size.
       *
       * The first approach implies that all the sequences of characters
       * are different from the sequence use for the tag. In addition,
       * the approach is not deterministic, as you do not know when the
       * tag will appear.
       *
       * With the second approach, you have a more predictable and more
       * efficient deserialization process. A string is encoded in two
       * parts:
       *
       *   - The size of the string in character as a short integer on 6
       *     characters.
       *   - The string content.
       */

      val stringData: String = STRING.toPseudobin(stringValue)
//      println(s"""serialized string value: "${stringData}"""")
      check(stringData == ??)

      val stringResult: Maybe[String] = STRING.fromPseudobin(stringData)
//      println(s"deserialized string result: $stringResult")
      check(stringResult == ??)
    }

    section("Serialization of list") {

      /**
       * There are similarities between string type and list type.
       * Instead of having a suite of characters, we now have a suite of
       * values of the same type.
       *
       * So, in the same way has the string, for the serialized list
       * instances will be prefixed by the number of elements it
       * contains, followed by the suite of elements it contains,
       * themselves serialized with the serde of their type.
       */

      val listValue = List("Jon", "Mary", "Tom")

      val serde = LIST(STRING)

      /**
       * In this example, we can see that we can create serde for list
       * of string by composing serdes.
       */
      val listData = serde.toPseudobin(listValue)
//      println(s"""serialized array value: "${listData}"""")
      check(listData == ??)
      val listResult: Try[(List[String], Input)] = serde.fromPseudobin(listData)
//      println(s"deserialized array result: $listResult")
      check(listResult == ??)
    }

    section("Serialization of nullable value") {

      /**
       * Nullable values when serialized is simply based on a character
       * indicating if there is an element (`1`) or not (`0`).
       */

      val nullableValue = Option(42)

      val serde = NULLABLE(INT)

      val nullableData = serde.toPseudobin(nullableValue)
//      println(s"""serialized nullable value: "$nullableData"""")
      check(nullableData == ??)

      val nullableResult = serde.fromPseudobin(nullableData)
//      println(s"deserialized nullable result: $nullableResult")
      check(nullableResult == ??)
    }

    section("Serialize a structure") {

      /**
       * The serialization of data is not limited to some basic types.
       * On the usual projects, you will have to serialize structures
       * representing objects of day-to-day life.
       *
       * Unfortunately, we have tools to automatically generate serde
       * from a given structure. This is not the for common
       * serialization format like we have seen with JSON, or with Avro,
       * Protobuf... For Pseudobin, we have to do it by hand. But this
       * is an opportunity to learn.
       *
       * In this example, we starts from a structure (ie. case class in
       * Scala) that represents users.
       */
      case class User(id: String, name: String, age: Option[Int])

      object User {

        /**
         * In a companion object, we create a serde for `User` entities.
         */
        val serde: PseudobinSerde[User] =
          new PseudobinSerde[User] {

            /**
             * Serialized structure simply consists in concatenating
             * fields one after the other.
             */
            override def toPseudobin(user: User): String =
              (
                STRING.toPseudobin(user.id)
                  + STRING.toPseudobin(user.name)
                  + NULLABLE(INT).toPseudobin(user.age)
              )

            /**
             * Deserialize a user is done simply by extracting fields
             * one after another. For-comprehension helps us to have a
             * more readable code, that manages `Try` type. We can also
             * se that the input changes after each field extraction.
             */
            override def fromPseudobin(data: pseudobin.Input): Maybe[User] =
              for {
                // every time we call fromPseudobin, we get a value and a new input
                // (with the same data, but a different offset)
                (id, input1) <- STRING.fromPseudobin(data)
                // we pass the new input to the following serde in a view to get the next value
                (name, input2) <- STRING.fromPseudobin(input1)
                // and so on...
                (age, input3) <- NULLABLE(INT).fromPseudobin(input2)
              }
              // once we have read all necessary data, we can generate the user.
              yield (User(id, name, age), input3)
          }

      }

      /**
       * Now, we will not serialize a user, but rather a list of users.
       */

      val users =
        List(
          User("123", "Jon", Some(32)),
          User("456", "Mary", None)
        )

      // we create a serde for list of users
      val serde = LIST(User.serde)

      val usersData = serde.toPseudobin(users)
//      println(s"""serialized users value: "${usersData}"""")
      check(usersData == ??)

      val usersResult = serde.fromPseudobin(Input(usersData, 0))
//      println(s"deserialized users result: $usersResult")
      check(usersResult == ??)
    }

  }

}
