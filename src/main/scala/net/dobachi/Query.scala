package net.dobachi

import java.io.InputStream
import java.nio.file.Path

import scala.io.Source

/**
  * Created by dobachi on 2017/02/19.
  */
class Query(val path: Path) extends Serializable {
  val content: String = {
    val stream : InputStream = getClass.getResourceAsStream(path.toString)
    Source.fromInputStream(stream).mkString
  }
}
