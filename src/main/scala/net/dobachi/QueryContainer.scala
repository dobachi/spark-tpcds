package net.dobachi

import java.nio.file.{FileSystems, Files, Path, Paths}

import scala.collection.JavaConverters._

/**
  * Created by dobachi on 2017/02/19.
  */
class QueryContainer(benchmark: String) extends Serializable {
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

  val queries: Array[Query] = {
    paths.map(p => new Query(p))
  }

  def fileNames() = {
    paths.map(p => p.getFileName())
  }

  def pathStrings() = {
    paths.map(p => p.toString)
  }

  def findQueryByFileName(targetFileName: String): Query = {
    val filtered = queries.filter(q => q.path.getFileName.toString == targetFileName)

    filtered match {
      case arr if arr.isEmpty =>
        throw new RuntimeException("None of files matched")
      case arr if arr.length > 1 =>
        throw new RuntimeException("Multiple files matched")
      case arr => arr(0)
    }
  }
}
