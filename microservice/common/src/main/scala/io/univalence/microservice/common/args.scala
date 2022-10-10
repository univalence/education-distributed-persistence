package io.univalence.microservice.common

object args {

  case class ArgsState(args: List[(String, Option[String])], currentParam: Option[String])

  object ArgsState {
    def init: ArgsState = ArgsState(args = List.empty, currentParam = None)
  }

  def readArgs(args: List[String]): List[(String, Option[String])] = {
    val state =
      args.foldLeft(ArgsState.init) {
        case (ArgsState(a, None), arg) if arg.startsWith("--") =>
          if (arg.contains("=")) {
            val kv = arg.split("=", 2)
            ArgsState(a :+ (kv(0).substring(2) -> Some(kv(1))), None)
          } else
            ArgsState(a, Some(arg.substring(2)))

        case (ArgsState(a, None), arg) =>
          val parameters = a.map { case (k, v) => s"$k=$v" }.mkString(" ")
          throw new IllegalArgumentException(s"bad parameter \"$arg\" after $parameters")

        case (ArgsState(a, Some(k)), arg) if arg.startsWith("--") =>
          if (arg.contains("=")) {
            val kv = arg.split("=", 2)
            ArgsState(a :+ (k -> None) :+ (kv(0).substring(2) -> Some(kv(1))), None)
          } else
            ArgsState(a :+ (k -> None), Some(arg.substring(2)))

        case (ArgsState(a, Some(k)), arg) =>
          ArgsState(a :+ (k -> Some(arg)), None)
      }

    if (state.currentParam.isDefined)
      state.args :+ (state.currentParam.get -> None)
    else
      state.args
  }

}
