package net.dobachi

/**
  * Created by dobachi on 2017/02/19.
  */
object ExecuteQueriesOptionParser {
  def apply() = {
    new scopt.OptionParser[ExecuteQueriesConfig]("tpcds") {
      arg[String]("benchmark").action((x, c) =>
        c.copy(x)
      ).text("The type of benchmark. default: impala-tpcds-queries")

      opt[String]('a', "applicationName").action((x, c) =>
        c.copy(x)
      ).text("YARN application name")
    }
  }
}
