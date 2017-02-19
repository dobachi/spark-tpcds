package net.dobachi

import java.nio.file.{FileSystems, Files, Path, Paths}

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
  * Created by dobachi on 2017/02/19.
  */
class QueryContainer(benchmark: String)(implicit spark: SparkSession) extends Serializable {
  val basePath = "/queries"
  val benchPath = basePath + "/" + benchmark

  val paths: Array[Path] = {
    val resource = getClass.getResource(benchPath)
    val uri = resource.toURI()
    val path = if (uri.getScheme == "jar") {
      val fileSystem = FileSystems.newFileSystem(uri, Map.empty[String, Object].asJava)
      fileSystem.getPath(benchPath);
    } else {
      Paths.get(uri);
    }

    val walk = Files.walk(path, 1)
    walk.sorted().toArray().tail.map(o => o.asInstanceOf[Path])
  }

  val allQueries: Array[Query] = {
    paths.map(p => new Query(p))
  }

  def fileNames() = {
    paths.map(p => p.getFileName())
  }

  def pathStrings() = {
    paths.map(p => p.toString)
  }

  def filterQueryByFileName(filterString: String): Array[Query] = {
    val chosen = filterString.split(",").map(_.trim)
    val filtered = allQueries.filter(q => chosen.contains(q.path.getFileName.toString))

    filtered match {
      case arr if arr.isEmpty =>
        throw new RuntimeException("None of files matched")
      case arr => arr
    }
  }

  def executeQueries(queries: Array[Query]) = {
    queries.foreach{ q =>
      q.executeQuery()
    }
  }

  def executeFilteredQueries(filterString: String) = {
    val queries = filterQueryByFileName(filterString)
    executeQueries(queries)
  }
}
