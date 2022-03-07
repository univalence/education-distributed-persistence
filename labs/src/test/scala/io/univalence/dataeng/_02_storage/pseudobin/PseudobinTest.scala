package io.univalence.dataeng._02_storage.pseudobin

import org.scalatest.funsuite.AnyFunSuiteLike

import scala.util.Success

class PseudobinTest extends AnyFunSuiteLike {
  import pseudobin._
  import pseudobin.PseudobinSerde._

  test("should serialize positive short") {
    assert(SHORT.toPseudobin(42) === "    42")
  }

  test("should serialize negative short") {
    assert(SHORT.toPseudobin(-42) === "   -42")
  }

  test("should serialize true") {
    assert(BOOLEAN.toPseudobin(true) === " true")
  }

  test("should serialize false") {
    assert(BOOLEAN.toPseudobin(false) === "false")
  }

  test("should serialize a string") {
    assert(STRING.toPseudobin("hello") === "     5hello")
  }

  test("should serialize an array") {
    assert(LIST(STRING).toPseudobin(List("hello", "world")) === "     2     5hello     5world")
  }

  test("should serialize None") {
    assert(NULLABLE(SHORT).toPseudobin(None) == "0")
  }

  test("should serialize Some(42)") {
    assert(NULLABLE(SHORT).toPseudobin(Some(42)) == "1    42")
  }

  test("should deserialize positive short") {
    assert(SHORT.fromPseudobin(Input("    42", 0)) == Success((42, Input("    42", 6))))
  }

  test("should deserialize negative short") {
    assert(SHORT.fromPseudobin(Input("   -42", 0)) == Success((-42, Input("   -42", 6))))
  }

  test("should deserialize true") {
    assert(BOOLEAN.fromPseudobin(Input(" true", 0)) == Success((true, Input(" true", 5))))
  }

  test("should deserialize false") {
    assert(BOOLEAN.fromPseudobin(Input("false", 0)) == Success((false, Input("false", 5))))
  }

  test("should deserialize string") {
    assert(STRING.fromPseudobin(Input("     5hello", 0)) == Success(("hello", Input("     5hello", 11))))
  }

  test("should deserialize an array") {
    assert(
      LIST(STRING).fromPseudobin(Input("     2     5hello     5world", 0))
        == Success((List("hello", "world"), Input("     2     5hello     5world", 28)))
    )
  }

  test("should deserialize None") {
    assert(NULLABLE(SHORT).fromPseudobin(Input("0", 0)) == Success(Option.empty[Short], Input("0", 1)))
  }

  test("should deserialize Some(42)") {
    assert(NULLABLE(SHORT).fromPseudobin(Input("1    42", 0)) == Success(Some(42.toShort), Input("1    42", 7)))
  }

}
