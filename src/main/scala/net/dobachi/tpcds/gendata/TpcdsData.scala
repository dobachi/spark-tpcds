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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.sys.process._

/**
  * Representation of tables
  */
class TpcdsData(partitionNum: Int, toolDir: String, scaleFactor: Int, outputDir: String, databaseName: String,
                enableOverwrite: Boolean = false)(implicit spark: SparkSession)
extends Serializable {

  val maxIndex = partitionNum - 1
  val factsBaseRdd = spark.sparkContext.parallelize(0 to maxIndex, partitionNum)
  val dimensionBaseeRdd = spark.sparkContext.parallelize(Array(0), 1)
  val dsdgenBin = s"$toolDir/dsdgen"
  val writeMode = if (enableOverwrite) "overwrite" else "error"

  private def executeDsdgen(baseRDD: RDD[Int], table: Table) = {
    val dsdgenRdd = baseRDD.flatMap{ i =>
      if (! new java.io.File(dsdgenBin).exists) {
        sys.error(s"Could not find dsdgen at $dsdgenBin. Run install")
      }

      val commands = Seq(
        "bash", "-c",
        s"cd $toolDir && ./dsdgen -table ${table.name} -filter Y -scale $scaleFactor -RNGSEED 100 -quiet Y")
      println(commands)
      commands.lineStream
    }
    dsdgenRdd.setName(s"${table.name}, sf=$scaleFactor, strings")

    val splittedRdd = dsdgenRdd.map { str =>
      val values = str.split("\\|", -1).dropRight(1).map {
        case "" => null
        case any: String => any
      }
      Row.fromSeq(values)
    }

    (table, splittedRdd)
  }

  val factsTables = new FactsTables()
  val dimensionTables = new DimensionTables()

  val factsRdds = factsTables.definitions.map { table =>
    executeDsdgen(factsBaseRdd, table)
  }

  val dimensionRdds = dimensionTables.definitions.map { table =>
    executeDsdgen(dimensionBaseeRdd, table)
  }

  private def genDFs(table: Table, rdd: RDD[Row]) = {
    val stringDF = spark.sqlContext.createDataFrame(rdd, StructType(table.schema.fields.map(f => StructField(f.name, StringType))))
    val columns = table.schema.fields.map { f =>
      col(f.name).cast(f.dataType).as(f.name)
    }
    val withSchemaDF = stringDF.select(columns: _*)
    (table, withSchemaDF)
  }

  val factsDFs = factsRdds.map { case (table, rdd) =>
    genDFs(table, rdd)
  }

  val dimensionDFs = dimensionRdds.map { case (table, rdd) =>
    genDFs(table, rdd)
  }

  private def saveDFsToParquetFiles(dfs: Seq[(Table, DataFrame)]) = {
    dfs.foreach { case (table, df) =>
        val outputURL = outputDir + "/" + table.name
        df.write.mode(writeMode).format("parquet").save(outputURL)
    }
  }

  private def saveDFsToTables(dfs: Seq[(Table, DataFrame)]) = {
    dfs.foreach { case (table, df) =>
      df.write.format("parquet").mode(writeMode).saveAsTable(table.name)
    }
  }

  def saveAsParquetFiles() = {
    saveDFsToParquetFiles(factsDFs)
    saveDFsToParquetFiles(dimensionDFs)
  }

  def createTable() = {
    spark.sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    spark.sqlContext.sql(s"USE $databaseName")

    saveDFsToTables(factsDFs)
    saveDFsToTables(dimensionDFs)
  }
}
