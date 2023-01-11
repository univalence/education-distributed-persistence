package io.univalence.dataeng.internal

object exercise_macros {

  import scala.reflect.macros.blackbox

  type Context = blackbox.Context

  final case class ExerciseContext(label: String)

  var activatedContexts: Seq[ExerciseContext] = Seq.empty

  val COLOR_GREY = "\u001b[38;5;8m"

  def partIndent: String = "\t" * activatedContexts.size

  case class Section(path: String, line: Int, label: String) {
    def reportOnly(f: => Unit): Unit = {
      activatedContexts = activatedContexts :+ ExerciseContext(label)
      val content = activatedContexts.map(_.label).mkString(" > ")
      println(s"${Console.YELLOW}+++ $content ${Console.RED}(TO ACTIVATE)${Console.RESET} ($path:$line)")
      activatedContexts = activatedContexts.init
    }

    def run(f: => Unit): Unit = {
      activatedContexts = activatedContexts :+ ExerciseContext(label)
      val content = activatedContexts.map(_.label).mkString(" > ")

      try {
        println(s"${Console.YELLOW}+++ $content ($path:$line)${Console.RESET}")

        f
      } catch {
        case PartException(l, c) =>
          throw PartException(s"$label > $l", c)
        case e: Exception =>
          throw PartException(label, e)
      } finally activatedContexts = activatedContexts.init
    }
  }

  def ignoreExerciseMacro(c: Context)(label: c.Expr[String])(f: c.Tree): c.Expr[Unit] = {
    import c._
    import universe._

    val position: c.universe.Position = label.tree.pos
    val line: Int                     = position.line
    val path: String                  = position.source.path

    c.Expr[Unit](q"""
io.univalence.dataeng.internal.exercise_macros.Section(
  path  = $path,
  line  = $line,
  label = $label
).reportOnly($f)
""")
  }

  def exerciseMacro(c: Context)(label: c.Expr[String])(f: c.Tree): c.Expr[Unit] = {
    import c._
    import universe._

    val position: c.universe.Position = label.tree.pos
    val line: Int                     = position.line
    val path: String                  = position.source.path

    c.Expr[Unit](q"""
io.univalence.dataeng.internal.exercise_macros.Section(
  path  = $path,
  line  = $line,
  label = $label
).run($f)
""")
  }

  def sectionMacro(c: Context)(label: c.Expr[String])(f: c.Tree): c.Expr[Unit] = {
    import c._
    import universe._

    val position: c.universe.Position = label.tree.pos
    val line: Int                     = position.line
    val path: String                  = position.source.path

    val inF = c.Expr[Unit](f)

    c.Expr[Unit](q"""
io.univalence.dataeng.internal.exercise_macros.Section(
  path  = $path,
  line  = $line,
  label = $label
).run($inF)
""")
  }

  case class PartException(label: String, cause: Throwable)
      extends RuntimeException(s"""Exception caught in part "$label"""", cause)

  def partMacro(c: Context)(label: c.Expr[String]): c.Expr[Unit] = {
    import c._
    import universe._

    val position: c.universe.Position = label.tree.pos
    val line: Int                     = position.line
    val path: String                  = position.source.file.path

    c.Expr[Unit](
      q"""{
  println(s"$${Console.YELLOW}+++ PART $${$label} ($${$path}:$${$line})$${Console.RESET}")
}"""
    )
  }

  def checkMacro(c: blackbox.Context)(expression: c.Expr[Boolean]): c.Expr[Unit] = {
    import c._
    import universe._

    val position: c.universe.Position = expression.tree.pos
    val path: String                  = position.source.path

    val startLine: Int      = position.source.offsetToLine(position.start)
    val endLine: Int        = position.source.offsetToLine(position.end)
    val offsetLine: Int     = position.source.lineToOffset(startLine)
    val lineContent: String = (startLine to endLine).map(line => position.source.lineToString(line)).mkString("\n")
    val line                = startLine + 1

    val expressionString =
      lineContent.substring(
        position.start - offsetLine,
        position.end - offsetLine
      )

    c.Expr[Unit](
      q"""{
  val result =
    scala.util.Try($expression) match {
      case scala.util.Success(true) =>
        s"$${Console.GREEN}OK (line:$${$line})$${Console.RESET}"
      case scala.util.Success(false) =>
        s"$${Console.YELLOW}FAILED ($${$path}:$${$line})$${Console.RESET}"
      case scala.util.Failure(e) =>
        s"$${Console.RED}ERROR ($${$path}:$${$line}: $${e.getClass.getCanonicalName}: $${e.getMessage})$${Console.RESET}"
    }

  val displayExpression =
    $expressionString.trim
      .replace("\n", "\n" + io.univalence.dataeng.internal.exercise_macros.partIndent)
      .replaceFirst("(//[^\n]+)\n", $COLOR_GREY + "$$1" + Console.RESET + "\n")

  val content = s">>> $$displayExpression => $$result"

  println(io.univalence.dataeng.internal.exercise_macros.partIndent + content)
}"""
    )
  }

}
