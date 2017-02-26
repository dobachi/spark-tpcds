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

package net.dobachi.tpcds.execute

import java.nio.file.{FileSystems, Files, Path, Paths}

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
  * Created by dobachi on 2017/02/19.
  */
class QueryContainer(benchmark: String, database: String)(implicit spark: SparkSession) extends Serializable {
  val log = LogManager.getLogger(this.getClass)

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
    walk.sorted().toArray().tail.map{
      case o: Path => o
      case o => throw new RuntimeException(s"Found an invalid object: ${o.toString()}")}
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

  def filterQueryByFileName(filterString: String, queries: Array[Query]): Array[Query] = {
    val chosen = filterString.split(",").map(_.trim)
    val filtered = queries.filter(q => chosen.contains(q.path.getFileName.toString))

    filtered match {
      case arr if arr.isEmpty =>
        throw new RuntimeException("None of files matched")
      case arr => arr
    }
  }

  def excludeQueryByFileName(filterString: String, queries: Array[Query]): Array[Query] = {
    val chosen = filterString.split(",").map(_.trim)
    val excluded = queries.filter(q => ! chosen.contains(q.path.getFileName.toString))

    excluded match {
      case arr if arr.isEmpty =>
        throw new RuntimeException("None of files matched")
      case arr => arr
    }
  }

  def executeAllQueries(): Array[ProcessTime] = {
    spark.sql(s"USE ${database}")
    log.info(s"USE ${database}")

    allQueries.map{ q =>
      log.info(s"Execute ${q.path}")
      val pTime = q.executeQuery()
      ProcessTime(q, pTime)
    }
  }

  def executeQueries(queries: Array[Query]): Array[ProcessTime] = {
    spark.sql(s"USE ${database}")
    log.info(s"USE ${database}")

    queries.map{ q =>
      log.info(s"Execute ${q.path}")
      val pTime = q.executeQuery()
      ProcessTime(q, pTime)
    }
  }

  def executeFilteredQueries(filterString: String, excludeString: String) = {
    val queries = if(filterString == "") {
      excludeQueryByFileName(excludeString, allQueries)
    } else {
      val filtered = filterQueryByFileName(filterString, allQueries)
      excludeQueryByFileName(excludeString, filtered)
    }
    executeQueries(queries)
  }
}
