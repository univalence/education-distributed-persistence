package io.univalence.dataeng.internal

import java.awt.event.KeyEvent
import java.time.LocalDateTime

object exercise_tools {

  import io.univalence.dataeng.internal.exercise_macros._

  import scala.language.experimental.macros

  def ?? : Any = null

  def check(expression: Boolean): Unit = macro checkMacro

  def comment(content: String): Unit = macro commentMacro

  def part(label: String): Unit = macro partMacro

  def exercise(label: String)(f: => Unit): Unit = macro exerciseMacro
  def exercise_ignore(label: String)(f: => Unit): Unit = macro ignoreExerciseMacro

  def section(label: String)(f: => Unit): Unit = macro sectionMacro

  def time[A](label: String)(f: => A): A = {
    val start = java.lang.System.nanoTime()
    println(partIndent + s"""${Console.MAGENTA}>>> Start "$label" @ ${LocalDateTime.now}${Console.RESET}""")

    try f
    finally {
      val end = java.lang.System.nanoTime()
      println(partIndent + s"""${Console.MAGENTA}>>> End "$label" @ ${LocalDateTime.now}${Console.RESET}""")
      val delta = (end - start) * 1e-9

      println(
        partIndent + s"""${Console.MAGENTA}>>> Elapsed time for "$label": ${Console.RED}${displayTime(
            delta
          )}${Console.RESET}"""
      )
    }
  }

  private def displayTime(seconds: Double): String = {
    def round2dec(value: Double): Double = Math.round(value * 100.0) / 100.0

    val posSeconds = Math.abs(seconds)
    lazy val order = Math.floor(Math.log10(posSeconds))
    if (posSeconds < 1e-9) "0s"
    else if (posSeconds >= 60.0 * 60.0) {
      val hour       = (seconds / (60.0 * 60.0)).toInt
      val minutes    = ((seconds / 60.0) - hour * 60.0).toInt
      val remSeconds = round2dec((seconds / 60.0 - (hour * 60.0 + minutes)) * 60.0)
      s"${hour}h${minutes}m${remSeconds}s"
    } else if (posSeconds >= 60.0 && posSeconds < 60.0 * 60.0) {
      val minutes    = (seconds / 60.0).toInt
      val remSeconds = round2dec((seconds / 60.0 - minutes) * 60.0)
      s"${minutes}m${remSeconds}s"
    } else if (order < 0 && order >= -3.0) {
      s"${round2dec(seconds * 1e3)}ms"
    } else if (order < -3 && order >= -6.0) {
      s"${round2dec(seconds * 1e6)}µs"
    } else if (order < -6 && order >= -9.0) {
      s"${round2dec(seconds * 1e9)}ns"
    } else {
      s"${round2dec(seconds)}s"
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
      val charLine   = data.map(b => if (isPrintable(b.toChar)) b.toChar else '.').mkString
      val offsetLine = f"$offset%08x"

      val paddingSize = lineSize - data.length
      val padding     = " " * (paddingSize * 3 + (paddingSize / packSize) * 2)

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

  private def isPrintable(c: Char): Boolean = {
    val block = Character.UnicodeBlock.of(c)

    (!Character.isISOControl(c)
    && c != KeyEvent.CHAR_UNDEFINED
    && block != null
    && block != Character.UnicodeBlock.SPECIALS)
  }

}
