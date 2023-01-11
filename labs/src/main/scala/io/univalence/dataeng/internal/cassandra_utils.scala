package io.univalence.dataeng.internal

import com.datastax.oss.driver.api.core.cql.{ResultSet, Row}
import com.datastax.oss.protocol.internal.ProtocolConstants.DataType

object cassandra_utils {

  import scala.jdk.CollectionConverters._

  def display(result: ResultSet): Unit = displayRows(result.all().asScala.toList)

  def displayRows(rows: List[Row]): Unit = {
    val nullValue: String                                = "<null>"
    def leftPad(s: String, length: Int, c: Char): String = (c.toString * (length - Math.min(s.length, length))) + s

    val columns = rows.headOption.map(_.getColumnDefinitions.asScala)
    val colSizes: Option[Iterable[Int]] =
      columns.map(cols =>
        rows.foldLeft(cols.map(_.getName.toString.length)) { case (lengths, row) =>
          cols
            .map(_.getName)
            .map(name => Option(row.getObject(name)).map(_.toString.length).getOrElse(nullValue.length))
            .zip(lengths)
            .map { case (valueSize, size) => Math.max(valueSize, size) }
        }
      )
    val printable = {
      for {
        cols  <- columns
        sizes <- colSizes
      } yield {
        val sep = sizes.map(size => "".padTo(size, '-')).mkString("+", "+", "+") + "\n"

        sep +
          cols
            .zip(sizes)
            .map { case (col, size) =>
              col.getName.toString.padTo(size, ' ')
            }
            .mkString("|", "|", "|\n") + sep +
          rows
            .map(row =>
              cols
                .zip(sizes)
                .map { case (col, size) =>
                  val value =
                    Option(row.getObject(col.getName))
                      .map(_.toString)
                      .getOrElse(s"\u001b[1m$nullValue${Console.RESET}")
                  col.getType.getProtocolCode match {
                    case DataType.INT | DataType.FLOAT | DataType.DOUBLE | DataType.DECIMAL =>
                      leftPad(value, size, ' ')
                    case _ =>
                      value.padTo(size, ' ')
                  }
                }
                .mkString("|", "|", "|")
            )
            .mkString("\n") + "\n" + sep
      }
    }
      .getOrElse("Nothing")

    print(printable)
  }

}
