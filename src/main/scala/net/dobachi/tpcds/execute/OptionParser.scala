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

/**
  * Created by dobachi on 2017/02/19.
  */
object OptionParser {
  def apply() = {
    new scopt.OptionParser[Config]("tpcds") {
      arg[String]("benchmark").action((x, c) =>
        c.copy(benchmark = x)
      ).text("The type of benchmark. default: impala-tpcds-queries")

      opt[String]("queryFilter").action((x, c) =>
        c.copy(queryFilter = x)
      ).text("The filter to chose queries which you execute. Specify queries with comma. e.g. query01.sql,query02.sql")

      opt[String]("excludedQueries").action((x, c) =>
        c.copy(excludedQueries = x)
      ).text("The list of queries which are excluded from execution. e.g. query01,sql,query02.sql")

      opt[String]("database").action((x, c) =>
        c.copy(database = x)
      ).text("The name of database. default: tpcds")

      opt[String]("appName").action((x, c) =>
        c.copy(appName = x)
      ).text("YARN application name")
    }
  }
}
