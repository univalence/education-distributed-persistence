package io.univalence.dataeng._02_storage.pseudobin

import scala.util.{Failure, Success, Try}

/**
 * Tools to manage pseudo-binary data.
 *
 * A pseudo-binary data, or ''pseudobin''. Is a (almost) readable binary
 * format. It is not as optimized of pure binary format, but it is
 * easier to analyse and more optimized than the fully readable formats
 * like JSON, CSV, or XML. pseudobin is designed to be a predictable
 * format.
 *
 * Features
 *   - Numbers are readable
 *   - Booleans are readable
 *   - Strings are prefixed by its size
 *   - Arrays are prefixed by the number of elements
 *   - Nullable value is prefixed by an flag indicating if value is
 *     present
 *
 * Limitations
 *   - The space reserved for numbers allows to represent them with
 *     their sign if they are negative. But there a risk that in this
 *     space you have a non valid numbers. For example, short integers
 *     are represented over 6 characters. But, if you have `999999`,
 *     this will produce an exception during the deserialization.
 */
object pseudobin {

  type Maybe[A] = Try[(A, Input)]

  /**
   * Serialization/deserialization tool for pseudobin.
   *
   * In the companion object some serdes are declared:
   *
   *   - Integers: `INT`, `SHORT`, `LONG`
   *   - Real: `DOUBLE`
   *   - `BOOLEAN`
   *   - `STRING`
   *   - Parameterized: `ARRAY` (for List), `NULLABLE` (for Option)
   *
   * To create an serde instance from a parameterized serde, you have to
   * provide the serde of the underlying type. Eg. `ARRAY(INT)` creates
   * a serde instance for `List[Int]`.
   *
   * If you want a serde for a case class, you have to create a
   * companion object for your case class and create an instance of
   * serde. For example
   *
   * {{{
   *   case class Message(content: String, criticality: Int)
   *   object Message {
   *     val serde: PseudobinSerde[Message] = new PseudobinSerde {
   *       override def toPseudobin(value: Message): String =
   *         STRING.toPseudobin(value.content) + INT.toPseudobin(value.criticality)
   *
   *         override def fromPseudobin(data: Input): Maybe[Message] =
   *           for {
   *             (content, input1)     <- STRING.fromPseudoBin(data)
   *             (criticality, input2) <- INT.fromPseudoBin(input1)
   *           } yield (Message(content, criticality), input2)
   *     }
   *   }
   * }}}
   *
   * Then, when you run `Message.serde.toPseudobin(Message("hello",
   * 1))`, you get (where `.` is used in place of space character)
   *
   * {{{
   *   ".....5hello..........1"
   * }}}
   */
  trait PseudobinSerde[A] {
    def toPseudobin(value: A): String
    def fromPseudobin(data: Input): Maybe[A]
    def fromPseudobin(data: String): Maybe[A] = fromPseudobin(Input(data, 0))
  }

  object PseudobinSerde {
    object INT extends PseudobinSerde[Int] {
      val size = 11

      override def toPseudobin(value: Int): String = leftPad(value.toString, size, " ")

      override def fromPseudobin(data: Input): Maybe[Int] =
        for {
          raw   <- Try(data.get(size))
          value <- Try(raw.trim.toInt)
        } yield (value, data.advance(size))
    }

    object SHORT extends PseudobinSerde[Short] {
      val size = 6

      override def toPseudobin(value: Short): String = leftPad(value.toString, size, " ")

      override def fromPseudobin(data: Input): Maybe[Short] =
        for {
          raw   <- Try(data.get(size))
          value <- Try(raw.trim.toShort)
        } yield (value, data.advance(size))
    }

    object LONG extends PseudobinSerde[Long] {
      val size = 20

      override def toPseudobin(value: Long): String = leftPad(value.toString, size, " ")

      override def fromPseudobin(data: Input): Maybe[Long] =
        for {
          raw   <- Try(data.get(size))
          value <- Try(raw.trim.toLong)
        } yield (value, data.advance(size))
    }

    object DOUBLE extends PseudobinSerde[Double] {
      val size = 24

      override def toPseudobin(value: Double): String = leftPad(value.toString, size, " ")

      override def fromPseudobin(data: Input): Maybe[Double] =
        for {
          raw   <- Try(data.get(size))
          value <- Try(raw.trim.toDouble)
        } yield (value, data.advance(size))
    }

    object BOOLEAN extends PseudobinSerde[Boolean] {
      val size = 5

      override def toPseudobin(value: Boolean): String = {
        val raw = if (value) "true" else "false"
        leftPad(raw, size, " ")
      }

      override def fromPseudobin(data: Input): Maybe[Boolean] =
        for {
          raw <- Try(data.get(size))
          value <-
            raw.trim match {
              case "true"  => Success(true)
              case "false" => Success(false)
              case raw     => Failure(new IllegalArgumentException(s"Boolean should be true or false. Got: $raw"))
            }
        } yield (value, data.advance(size))
    }

    object STRING extends PseudobinSerde[String] {
      override def toPseudobin(data: String): String = {
        val size = data.length.toShort
        SHORT.toPseudobin(size) + data
      }

      override def fromPseudobin(data: Input): Maybe[String] =
        for {
          (size, newInput) <- SHORT.fromPseudobin(data)
          raw              <- Try(newInput.get(size))
        } yield (raw, newInput.advance(size))
    }

    class LIST[A](serde: PseudobinSerde[A]) extends PseudobinSerde[List[A]] {
      override def toPseudobin(value: List[A]): String = {
        val size = value.length.toShort

        SHORT.toPseudobin(size) + value.map(serde.toPseudobin).mkString("")
      }

      override def fromPseudobin(data: Input): Maybe[List[A]] =
        for {
          (size, newInput) <- SHORT.fromPseudobin(data)
          result <-
            (0 until size).foldLeft(Try((List.empty[A], newInput))) {
              case (Success((array, input)), index) =>
                for {
                  (value, nextInput) <- serde.fromPseudobin(input)
                } yield (array :+ value, nextInput)
              case (f @ Failure(_), _) => f
            }
        } yield result
    }
    object LIST {
      def apply[A](serde: PseudobinSerde[A]): PseudobinSerde[List[A]] = new LIST(serde)
    }

    class NULLABLE[A](serde: PseudobinSerde[A]) extends PseudobinSerde[Option[A]] {
      override def toPseudobin(value: Option[A]): String =
        value match {
          case None    => "0"
          case Some(v) => "1" + serde.toPseudobin(v)
        }

      override def fromPseudobin(data: Input): Maybe[Option[A]] =
        for {
          (indicator, newInput) <- Try((data.get(1), data.advance(1)))
          result <-
            indicator match {
              case "0" => Success(Option.empty[A], newInput)
              case "1" => serde.fromPseudobin(newInput).map { case (v, i) => (Option(v), i) }
              case raw =>
                Failure(new IllegalArgumentException(s"Nullable value should have indicator to 0 or 1. Got: $raw"))
            }
        } yield result
    }
    object NULLABLE {
      def apply[A](serde: PseudobinSerde[A]): PseudobinSerde[Option[A]] = new NULLABLE(serde)
    }

  }

  /**
   * Represent input data during a deserialization process.
   *
   * @param data
   *   data to deserialize
   * @param offset
   *   current offset in the data to deserialize
   */
  case class Input(data: String, offset: Int) {

    /**
     * Move current location in data forward.
     *
     * @param n
     *   number of characters to move current location over
     */
    def advance(n: Int): Input = copy(offset = offset + n)

    /**
     * Get the `n` following characters in the input.
     *
     * @param n
     *   number of character to extract
     * @return
     *   a subset of the data
     * @throws IndexOutOfBoundsException
     *   when `n` is bigger than the count of remaining characters.
     */
    def get(n: Int): String = data.substring(offset, offset + n)
  }

  /**
   * Append the pattern to the left-side of the input string.
   *
   * @param s
   *   input string.
   * @param toSize
   *   target size of the output string.
   * @param pattern
   *   pattern to use to fill the left-side.
   */
  def leftPad(s: String, toSize: Int, pattern: String): String =
    if (toSize < s.length) s
    else {
      val delta     = toSize - s.length
      val count     = delta / pattern.length
      val padding   = pattern * count
      val remaining = delta % pattern.length

      padding + pattern.substring(pattern.length - remaining) + s
    }

}
