package util

import com.monovore.decline.{Command, CommandApp, Opts, PlatformApp, Visibility}

abstract class CommandApp2(
  name: String,
  header: String,
  helpFlag: Boolean = true,
  version: String = ""
) {

  def main: Opts[Unit]

  private val command = {
    // Code copied from com.monovore.decline.CommandApp
    val showVersion =
      if (version.isEmpty) Opts.never
      else
        Opts
          .flag("version", "Print the version number and exit.", visibility = Visibility.Partial)
          .map(_ => System.err.println(version))

    Command(name, header, helpFlag)(showVersion orElse main)
  }

  final def main(args: Array[String]): Unit =
    command.parse(PlatformApp.ambientArgs getOrElse args, sys.env) match {
      case Left(help) => System.err.println(help)
      case Right(_) => ()
    }
}
