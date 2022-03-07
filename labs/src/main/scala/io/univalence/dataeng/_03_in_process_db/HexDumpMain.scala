package io.univalence.dataeng._03_in_process_db

import io.univalence.dataeng.internal.exercise_tools

import scala.util.Using
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Paths, StandardOpenOption}

object HexDumpMain {

  // TODO change filename below if necessary
  val filename = "data/target/_03_in_process_db/_02_mapdb/_01_mapdb.db"

  def main(args: Array[String]): Unit =
    Using(FileChannel.open(Paths.get(filename), StandardOpenOption.READ)) { channel =>
      val buffer = ByteBuffer.allocate(16)

      var byteRead     = channel.read(buffer)
      var offset: Long = 0L

      while (byteRead >= 0 && offset < 5000) {
        buffer.rewind()

        val data = buffer.array()
        exercise_tools.hexdump(data, offset)

        offset += byteRead
        byteRead = channel.read(buffer)
      }

      println("[EOF]")
    }.get
}
