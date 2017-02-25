package net.dobachi.tpcds.gendata

/**
  * Config for scopt
  */
case class Config(partitionNum: Int = 10,
                  toolDir: String = "/usr/local/tpc-ds/default/tools",
                  scaleFactor: Int = 1,
                  outputDir: String = "/tmp/tpcds",
                  appName: String = "GenerateTpcdsData",
                  databaseName: String = "tpcds",
                  enableOverwrite: Boolean = false)
