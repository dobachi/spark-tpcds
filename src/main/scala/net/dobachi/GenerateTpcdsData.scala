/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package net.dobachi

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

/**
  * Generate data of TPC-DS
  */
object GenerateTpcdsData {

  val log = LogManager.getLogger(this.getClass)

  // Scopt configuration
  case class Config(partitionNum: Int = 10,
                    toolDir: String = "/usr/local/tpc-ds/default/tools",
                    scaleFactor: Int = 1,
                    outputDir: String = "/tmp/tpcds",
                    appName: String = "GenerateTpcdsData",
                    databaseName: String = "tpcds",
                    enableOverwrite: Boolean = false)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("GenerateTpcdsData") {
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

    parser.parse(args, Config()) match {
      case Some(config) =>
        implicit val spark = SparkSession.builder().appName(config.appName).enableHiveSupport().getOrCreate()

        log.info("Defining DataFrames")
        val tpcdsData = new TpcdsData(config.partitionNum, config.toolDir, config.scaleFactor,
          config.outputDir, config.databaseName, config.enableOverwrite)

        // If you want to create raw Parquet file instead of Hive table,
        // you can use saveAsParquetFiles method instead of createTable method.
        log.info("Creating tables")
        tpcdsData.createTable()

      case None =>
        sys.exit(1)
    }
  }

}