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


package example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * An exmaple of Spark application
  */
object Example {

  /**
    * Create a sample of DataFrame, which is used for the test of calculation.
    *
    * @param n The number of records
    * @param spark SparkSession to be used in this method.
    * @return DataFrame
    */
  def createDataFrame(n: Int)(implicit spark: SparkSession): DataFrame = {
    val num = SampleIntegerGenerator().generate(n)
    val rdd = spark.sparkContext.parallelize(num).map(SampleInteger)
    spark.createDataFrame[SampleInteger](rdd)
  }

  /**
    * A sample method to execute calculation using DataFrame.
    *
    * @param df DataFrame which consists of records of integers.
    * @return DataFrame of result of calculation
    */
  def calcNumbers(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    val window = Window.partitionBy().orderBy(desc("n"))
    val diff = max($"n").over(window) - df("n")
    df.select($"*", diff.as("diff"))
  }

  def main(args: Array[String]): Unit = {
    val appName = "Example"
    val master = "local[2]"
    implicit val spark = SparkSession.builder().appName(appName).master(master).getOrCreate()
    val n = 10
    val inputDf = createDataFrame(n)
    val sumDf = calcNumbers(inputDf)
    sumDf.show()
  }

}
