import net.dobachi.tpcds.gendata.TpcdsData

/**
  * Scala Shell to create tables of TPC-DS data.
  * You can also use an application instead of this script.
  */

val partitionNum = 1
val toolDir = "/usr/local/tpc-ds/default/tools"
val scaleFactor = 1
val outputDir = "/tmp/tpcds"
implicit def sparkSession = spark
val tpcdsData = new TpcdsData(partitionNum, toolDir, scaleFactor, outputDir)
tpcdsData.createTable()
