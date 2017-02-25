package net.dobachi.tpcds.gendata

import scopt.OptionParser

/**
  * Created by dobachi on 2017/02/25.
  */
object OptionParser {
  def apply(): OptionParser[Config] = {
    new OptionParser[Config]("GenerateTpcdsData") {
      head("GenerateTpcdsData")

      arg[String]("outputDir").action((x, c) =>
        c.copy(outputDir = x)).text("The URL of output files. If you write data into Hive database, " +
        "this parameter is ignored and data is written into warehouse directory which you configured in hive-site.xml.")

      opt[Int]("partitionNum").action((x, c) =>
        c.copy(partitionNum = x)).text("The number of partitions, which is also used to configure the parallelism. default: 10")

      opt[String]("toolDir").action((x, c) =>
        c.copy(toolDir = x)).text("The path of TPC-DS tools directory which contains dsdgen. default: /usr/local/tpc-ds/default/tools")

      opt[Int]("scaleFactor").action((x, c) =>
        c.copy(scaleFactor = x)).text("The scaleFactor, which is one of parameters of dsdgen. default: 1")

      opt[String]("appName").action((x, c) =>
        c.copy(appName = x)).text("The name of application which is used for YARN")

      opt[String]("databaseName").action((x, c) =>
        c.copy(databaseName = x)).text("The name of database when you create Hive table")

      opt[Unit]("enableOverwrite").action((_, c) =>
        c.copy(enableOverwrite = true)).text("Enable overwrite mode")
    }

  }

}
