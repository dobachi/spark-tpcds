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

import java.io.InputStream
import java.nio.file.Path

import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Representation of each query.
  * This class has the following functions.
  *
  * - Execute query
  * - Calculate the execution time
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
