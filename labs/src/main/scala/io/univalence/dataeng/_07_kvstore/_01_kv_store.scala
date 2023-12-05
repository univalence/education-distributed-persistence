package io.univalence.dataeng._07_kvstore

import io.univalence.dataeng.internal.exercise_tools._

object _01_kv_store {
  def main(args: Array[String]): Unit =
    section("Simple store") {
      exercise("PUT and GET") {
        val ages = new InMemoryStore

        ages.put("mary", 13)
        ages.put("tom", 25)
        ages.put("jon", 12)

        /**
         * `Store#get` method returns an instance of `Try`. This type
         * has 2 sub-classes:
         *   - `Success(value)` to represent a computation that has
         *     succeeded (the value parameter is the result of the
         *     computation).
         *   - `Failure(exception)` to represent of a computation that
         *     has failed (with the associated exception).
         */

        check(ages.get("mary") == ??)
        check(ages.get("tom") == ??)
        check(ages.get("unknown").isFailure == ??)
      }

      exercise("PUT and GET and record order") {

        /**
         * KV-store used to sort records in lexicographical order and
         * not by insert order.
         */
        val ages = new InMemoryStore

        ages.put("mary", 13)
        ages.put("tom", 25)
        ages.put("jon", 12)

        // get all the keys stored
        val keys: List[String] =
          ages
            .scan()
            .get
            .map(record => record.key)
            .toList

        comment("Keys appear in the store in this order")
        check(keys == List(??))
      }
    }
}
