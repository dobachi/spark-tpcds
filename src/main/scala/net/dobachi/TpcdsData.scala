package net.dobachi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.sys.process._

/**
  * Created by dobachi on 2017/02/18.
  */
class TpcdsData(partitionNum: Int, toolDir: String, scaleFactor: Int, outputDir: String, databaseName: String,
                enableOverwrite: Boolean = false)(implicit spark: SparkSession)
extends Serializable {

  val maxIndex = partitionNum - 1
  val baseRdd = spark.sparkContext.parallelize(0 to maxIndex, partitionNum)
  val dsdgenBin = s"$toolDir/dsdgen"

  val tables = new Tables()

  val dsdgenRdds = tables.definitions.map { table =>
    val dsdgenRdd = baseRdd.flatMap{ i =>
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

  val dsdgenDFs = dsdgenRdds.map { case (table, rdd) =>
    val stringDF = spark.sqlContext.createDataFrame(rdd, StructType(table.schema.fields.map(f => StructField(f.name, StringType))))
    val columns = table.schema.fields.map { f =>
      col(f.name).cast(f.dataType).as(f.name)
    }
    val withSchemaDF = stringDF.select(columns: _*)
    (table, withSchemaDF)
  }

  def saveAsParquetFiles() = {
    if (enableOverwrite) {
      dsdgenDFs.foreach { case (table, df) =>
        val outputURL = outputDir + "/" + table.name
        df.write.mode("overwrite").format("parquet").save(outputURL)
      }
    } else {
      dsdgenDFs.foreach { case (table, df) =>
        val outputURL = outputDir + "/" + table.name
        df.write.format("parquet").save(outputURL)
      }
    }
  }

  def createTable() = {
    spark.sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    spark.sqlContext.sql(s"USE $databaseName")
    if (enableOverwrite) {
      dsdgenDFs.foreach { case (table, df) =>
        df.write.format("parquet").mode("overwrite").saveAsTable(table.name)
      }
    } else {
      dsdgenDFs.foreach { case (table, df) =>
        df.write.format("parquet").saveAsTable(table.name)
      }
    }
  }
}
