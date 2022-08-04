package io.univalence.dataeng._02_storage

import org.scalatest.funsuite.AnyFunSuiteLike

class _01_treemapTest extends AnyFunSuiteLike {

  test("should put and get new data") {
    val store = generateStore()

    val user = User("123", "Jon", 25)
    store.put(user.id, user)

    val result = store.get(user.id).get

    assert(result == user)
  }

  test("should update and get updated data") {
    val store = generateStore()

    val userv1 = User("123", "Jon", 25)
    store.put(userv1.id, userv1)

    val userv2 = userv1.copy(age = 32)
    store.put(userv2.id, userv2)

    val result = store.get(userv2.id).get

    assert(result == userv2)
  }

  test("should not get unknown data") {
    val store = generateStore()

    val result = store.get("[unknown]")

    assert(result.isFailure)
  }

  test("should not get deleted data") {
    val store = generateStore()

    val user = User("123", "Jon", 25)
    store.put(user.id, user)
    store.delete(user.id)

    val result = store.get(user.id)

    assert(result.isFailure)
  }

  test("get the minimal key") {
    val store = generateStore()

    val jon  = User("123", "Jon", 25)
    val mary = User("456", "Mary", 22)

    store.put("A", jon)
    store.put("B", mary)

    assert(store.min == ("A", jon))
  }

  test("get the maximal key") {
    val store = generateStore()

    val jon  = User("123", "Jon", 25)
    val mary = User("456", "Mary", 22)

    store.put("A", jon)
    store.put("B", mary)

    assert(store.max == ("B", mary))
  }

  test("get a range with bounds included") {
    val store = generateStore()

    val jon   = User("123", "Jon", 25)
    val mary  = User("456", "Mary", 22)
    val tom   = User("789", "Tom", 35)
    val ellen = User("987", "Ellen", 45)

    store.put("A", jon)
    store.put("B", mary)
    store.put("C", tom)
    store.put("D", ellen)

    assert(
      store.range("B", "C").toList == List(
        ("B", mary),
        ("C", tom)
      )
    )
  }

  test("get a range with bounds not in store") {
    val store = generateStore()

    val jon   = User("123", "Jon", 25)
    val mary  = User("456", "Mary", 22)
    val tom   = User("789", "Tom", 35)
    val ellen = User("987", "Ellen", 45)

    store.put("A", jon)
    store.put("C", mary)
    store.put("F", tom)
    store.put("P", ellen)

    assert(
      store.range("B", "H").toList == List(
        ("C", mary),
        ("F", tom)
      )
    )
  }

  case class User(id: String, name: String, age: Int)

  def generateStore() = new _01_treemap.TreeKeyValueStore[String, User]

}
