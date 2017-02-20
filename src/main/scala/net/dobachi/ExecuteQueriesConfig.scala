package net.dobachi

/**
  * Created by dobachi on 2017/02/11.
  */
case class ExecuteQueriesConfig(benchmark: String = "tpcds",
                                queryFilter: String = "",
                                excludedQueries: String = "",
                                database: String = "tpcds",
                                appName: String = "tpcds")
