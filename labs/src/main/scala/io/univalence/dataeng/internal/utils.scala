package io.univalence.dataeng.internal

import java.awt.event.KeyEvent
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try, Using}

object utils {

  def using[A <: AutoCloseable, B](resource: A)(f: A => B): B = Using(resource)(f).get

  /** Retries a function until it succeed or else return None. */
  @tailrec
  def retry[T](retryCount: Int, delayMs: Int)(fn: => T)(condition: T => Boolean): Option[T] =
    if (condition(fn)) {
      Some(fn)
    } else {
      if (retryCount <= 1) {
        None
      } else {
        Thread.sleep(delayMs)
        retry(retryCount - 1, delayMs)(fn)(condition)
      }
    }

  def hexdump(data: Array[Byte], offset: Long, packSize: Int = 4, packCount: Int = 4): Unit = {
    require(packSize > 0, "packSize parameter should be 1 or above")
    require(packCount > 0, "packCount parameter should be 1 or above")

    val lineSize = packSize * packCount

    def dumpOneLine(data: Array[Byte], offset: Long): Unit = {
      val hexLine =
        data
          .map(b => f"$b%02x")
          .grouped(packSize)
          .map(_.mkString(" "))
          .mkString(" | ")
      val charLine = data.map(b => if (isPrintable(b.toChar)) b.toChar else '.').mkString
      val offsetLine = f"$offset%08x"

      val paddingSize = lineSize - data.length
      val padding = " " * (paddingSize * 3 + (paddingSize / packSize) * 2)

      println(
        s"${Console.YELLOW}$offsetLine${Console.RESET}  $hexLine $padding |${Console.BOLD}$charLine${Console.RESET}|"
      )
    }

    if (data.length > lineSize) {
      data
        .grouped(lineSize)
        .zipWithIndex
        .foreach { case (line, index) =>
          dumpOneLine(line, offset + index * lineSize)
        }
    } else {
      dumpOneLine(data, offset)
    }
  }

  def isPrintable(c: Char): Boolean = {
    val block = Character.UnicodeBlock.of(c)

    (!Character.isISOControl(c)
      && c != KeyEvent.CHAR_UNDEFINED
      && block != null
      && block != Character.UnicodeBlock.SPECIALS)
  }


}
