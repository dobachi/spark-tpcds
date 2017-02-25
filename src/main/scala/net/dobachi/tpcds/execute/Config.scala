package net.dobachi.tpcds.execute

/**
  * Created by dobachi on 2017/02/11.
  */
case class Config(benchmark: String = "tpcds",
                  queryFilter: String = "",
                  excludedQueries: String = "",
                  database: String = "tpcds",
                  appName: String = "tpcds")
