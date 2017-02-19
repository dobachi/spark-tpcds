package net.dobachi

import java.io.InputStream
import java.nio.file.Path

import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by dobachi on 2017/02/19.
  */
class Query(val path: Path)(implicit spark: SparkSession) extends Serializable {
  val contents: Array[String] = {
    val stream : InputStream = getClass.getResourceAsStream(path.toString)
    val queryString = Source.fromInputStream(stream).mkString
    queryString.split(";").dropRight(1)
  }

  def executeQuery() = {
    contents.foreach{ q =>
      spark.sql(q)
    }
  }
}
