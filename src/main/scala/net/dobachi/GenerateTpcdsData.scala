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
                    enableOverwrite: Boolean = false)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("GenerateTpcdsData") {
      head("GenerateTpcdsData")

      arg[String]("outputDir").action((x, c) =>
        c.copy(outputDir = x)).text("The URL of output files")

      opt[Int]("partitionNum").action((x, c) =>
        c.copy(partitionNum = x)).text("The number of partitions, which is also used to configure the parallelism. default: 10")

      opt[String]("toolDir").action((x, c) =>
        c.copy(toolDir = x)).text("The path of TPC-DS tools directory which contains dsdgen. default: /usr/local/tpc-ds/default/tools")

      opt[Int]("scaleFactor").action((x, c) =>
        c.copy(scaleFactor = x)).text("The scaleFactor, which is one of parameters of dsdgen. default: 1")

      opt[String]("appName").action((x, c) =>
        c.copy(appName = x)).text("The name of application which is used for YARN")

      opt[Unit]("enableOverwrite").action((_, c) =>
        c.copy(enableOverwrite = true)).text("Enable overwrite mode")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        implicit val spark = SparkSession.builder().appName(config.appName).getOrCreate()

        log.info("defining DataFrames")
        val tpcdsData = new TpcdsData(config.partitionNum, config.toolDir, config.scaleFactor,
          config.outputDir, config.enableOverwrite)

        log.info("saving DataFrames")
        tpcdsData.save()

      case None =>
        sys.exit(1)
    }
  }

}