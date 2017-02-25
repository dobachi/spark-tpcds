package net.dobachi.tpcds.execute

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

  def processTime(f: => Any): Long = {
    val start = System.currentTimeMillis()
    f
    val end = System.currentTimeMillis()
    end - start
  }

  def executeQuery(): Long = {
    processTime{
      contents.foreach{ q =>
        spark.sql(q)
      }
    }
  }
}
