package net.dobachi.tpcds.execute

/**
  * Created by dobachi on 2017/02/19.
  */
object OptionParser {
  def apply() = {
    new scopt.OptionParser[Config]("tpcds") {
      arg[String]("benchmark").action((x, c) =>
        c.copy(benchmark = x)
      ).text("The type of benchmark. default: impala-tpcds-queries")

      opt[String]("queryFilter").action((x, c) =>
        c.copy(queryFilter = x)
      ).text("The filter to chose queries which you execute. Specify queries with comma. e.g. query01.sql,query02.sql")

      opt[String]("excludedQueries").action((x, c) =>
        c.copy(excludedQueries = x)
      ).text("The list of queries which are excluded from execution. e.g. query01,sql,query02.sql")

      opt[String]("database").action((x, c) =>
        c.copy(database = x)
      ).text("The name of database. default: tpcds")

      opt[String]("appName").action((x, c) =>
        c.copy(appName = x)
      ).text("YARN application name")
    }
  }
}
