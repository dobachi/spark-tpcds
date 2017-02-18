import net.dobachi.TpcdsData

/**
  * Created by dobachi on 2017/02/18.
  */

val partitionNum = 1
val toolDir = "/usr/local/tpc-ds/default/tools"
val scaleFactor = 1
val outputDir = "/tmp/tpcds"
implicit def sparkSession = spark
val tpcdsData = new TpcdsData(partitionNum, toolDir, scaleFactor, outputDir)
tpcdsData.save()
