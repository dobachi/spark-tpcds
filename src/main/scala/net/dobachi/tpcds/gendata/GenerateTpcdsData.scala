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


package net.dobachi.tpcds.gendata

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

/**
  * Generate data of TPC-DS
  */
object GenerateTpcdsData {

  val log = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val parser = OptionParser()

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