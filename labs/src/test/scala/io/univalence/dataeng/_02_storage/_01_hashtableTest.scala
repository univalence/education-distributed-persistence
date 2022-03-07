package io.univalence.dataeng._02_storage

import org.scalatest.funsuite.AnyFunSuiteLike

class _01_hashtableTest extends AnyFunSuiteLike {

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

  case class User(id: String, name: String, age: Int)

  def generateStore() = new _01_hashtable.MapKeyValueStore[String, User]

}
