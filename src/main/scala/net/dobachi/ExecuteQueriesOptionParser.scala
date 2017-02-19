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

      opt[String]("queryFilter").action((x, c) =>
        c.copy(x)
      ).text("The filter to chose queries which you execute. Specify queries with comma. e.g. query01.sql,query02.sql")

      opt[String]('a', "applicationName").action((x, c) =>
        c.copy(x)
      ).text("YARN application name")
    }
  }
}
