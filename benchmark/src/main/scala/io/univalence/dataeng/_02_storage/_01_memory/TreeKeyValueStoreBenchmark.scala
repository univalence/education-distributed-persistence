package io.univalence.dataeng._02_storage._01_memory

import org.openjdk.jmh.annotations._

import io.univalence.dataeng._02_storage._01_treemap.TreeKeyValueStore

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
class TreeKeyValueStoreBenchmark {

  var user: User                          = _
  var db: TreeKeyValueStore[String, User] = _

  @Setup
  def setup(): Unit = {
    user = User("123", "Jon", Some(32))
    db   = new TreeKeyValueStore()
    db.put("456", User("456", "Mary", None))
  }

  @Benchmark
  def bench_put(): Unit = {
    db.put("123", user)
    ()
  }

  @Benchmark
  def bench_get(): Unit = {
    db.get("456")
    ()
  }

  @Benchmark
  def bench_delete(): Unit = {
    db.delete("456")
    ()
  }

  case class User(id: String, name: String, age: Option[Int])

}
