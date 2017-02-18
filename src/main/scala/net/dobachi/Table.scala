package net.dobachi

import org.apache.spark.sql.types.{StructField, StructType}

/**
  * Created by dobachi on 2017/02/18.
  */
case class Table(name: String, partitionColumns: Seq[String], fields: StructField*) {
  val schema = StructType(fields)
}
