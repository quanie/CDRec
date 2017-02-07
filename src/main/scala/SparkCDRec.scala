import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by dudo on 20/09/16.
  */
object SparkCDRec extends Application {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("CDRec")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    val sparkContext = new SparkContext(sparkConf)

    //Turn off spark-shell msg
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    //1. Data preparation
    dataPreparation(args)

    //2. CrossDomain Recommendation
    if(gnNumberOfDataSet==1){
      println("CDRec requires at least two datasets")
    }
    else {
      val AS = new CDRec(gnNumberOfDataSet, gpnTensorMode, gpnTensorLength, gpnTensorSplit, gpsInputPath)
        .setOptimizationOption(gsOptimizationOption)
        .setStoppingCondition(gsStoppingCondition)
        .setRank(gnRank)
        .setOutputPath(gsOutputPath)
        .setSparkContext(sparkContext)
        .setCheckPoint(1)
        .setCoupleMap(gsCoupleMapFilePath)

      if(gpsValidationPath!=null)
        AS.setValidationFileName(gpsValidationPath)
      AS.factorize()
    }
    //n. Close context
    sparkContext.stop()
  }
}
