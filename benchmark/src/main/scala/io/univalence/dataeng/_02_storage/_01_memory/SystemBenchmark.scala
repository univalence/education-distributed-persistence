package io.univalence.dataeng._02_storage._01_memory

import scala.util.Random

import org.openjdk.jmh.annotations._

@State(Scope.Thread)
//@BenchmarkMode(Array(Mode.Throughput))
class SystemBenchmark {

  var intValue: Int       = _
  var stringValue: String = _

  @Setup(Level.Invocation)
  def setup(): Unit = {
    intValue    = Random.nextInt(100000)
    stringValue = Random.nextString(20)
  }

  @Benchmark
  def addInt(): Unit = {
    intValue = intValue + intValue
    ()
  }

  @Benchmark
  def addString(): Unit = {
    stringValue = stringValue + stringValue
    ()
  }

}
