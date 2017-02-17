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

class ExampleSuite extends TestSparkSession {

  import example.Example._

  test("Numbers generate an array of Int") {
    val n = 3
    val generator = SampleIntegerGenerator()
    val arr = generator.generate(n)
    val answer = Array(0, 1, 2)

    assert(arr === answer)
  }

  test("Create DataFrame") {
    implicit def spark = _spark

    val n = 3
    val df = createDataFrame(n)
    val answer = Array(0, 1, 2)

    assert(df.collect().map(r => r.getAs[Int]("n")) === answer)
  }

  test("Calculate") {
    implicit def spark = _spark

    val rdd = spark.sparkContext.parallelize(0 to 2).map(SampleInteger)
    val df = spark.createDataFrame(rdd)
    val sum = calcNumbers(df)
    val answer = Array(0, 1, 2)

    assert(sum.collect().map(r => r.getAs[Int]("diff")) === answer)
  }
}
