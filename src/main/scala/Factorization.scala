import java.io.{File, FileWriter}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.annotation.elidable
import scala.annotation.elidable._

/** ******************************************************************************************
  * Created by dudo on 15/04/16.
  * ******************************************************************************************/
abstract class Factorization extends Logging{
  //Optimization options
  protected var sOptimizationOption: String = ""  //CF_Lambda_LeastsquareFunc
                                                  //LeastsquareFunc - "LS": Least Squares; "WLS": Weighted Least Squares
  //Stopping options
  protected var dMinDiff: Double = 0              //Stopping condition - Error difference
  protected var dMaxHour: Double = 0              //Stopping condition - running hour
  protected var nMaxIter: Int = 0                 //Stopping condition - max Iteration

  def getMinDiff() = dMinDiff
  def getMaxHour() = dMaxHour
  def getMaxIter() = nMaxIter

  //Factorization options
  protected var rank:Int = 3                         //default decomposition rank

  //Gradient option
  protected var sGradientName: String = ""

  //output parameters
  protected var outputPath: String = ""               //output path
  protected var checkPointIter:Int = 10               //CheckPoint after each 10 iterations

  def getOutputPath() = outputPath
  def getCheckPointIter() = checkPointIter

  //Spark parameters
  protected var sparkContext:SparkContext = _
  /** storage level for user/product in/out links */
  protected var inputRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER//.MEMORY_AND_DISK
  protected var finalRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER//.MEMORY_AND_DISK

  //for debug purpose
  @elidable(FINE) protected def debugln(msg: String)      = {
    println(msg)
  }
  @elidable(FINE) protected def debug(msg: String)        = {
    print(msg)
  }

  /** ******************************************************************************************
    * Function factorize: factorizes input tensor(s)
    *       Child class must implement this function
    * ******************************************************************************************/
  def factorize():Double

  /** ******************************************************************************************
    * Function performOptimization
    * @param ne
    * @param points
    * @param xOld
    * @param optimizationOption
    * @return factor
    * ******************************************************************************************/
  protected def performOptimization(ne:NormalEquation,
                                    points:Array[(Double, Array[Double], Double)],
                                    xOld: Array[Double],
                                    optimizationOption: String):Array[Double]={
    val gradient = new Gradient
    var xNew: Array[Double] = null
    var dLoss: Double = 0

    val options = optimizationOption.split("_")
    options(0) match{
      case "CF" => {
        /** using normal equation (pseudo inverse is computed by Cholesky decomposition) **/
        val dLambda = options(1).toDouble

        val solver = new CholeskySolver
        xNew = solver.solve(ne, dLambda)
      }
    }

    xNew
  }

  /** ******************************************************************************************
    * ComputeLoss
    *
    * Return: Loss
    * ******************************************************************************************/
  def computeLoss(tensorSlice: RDD[(Int, Iterable[(String, Double)])],
                  factors: Array[Factor],
                  tensorSize: Long,
                  rank: Int): Double ={
    val arrnIndex = new Array[Int](factors.length)

    var Loss = tensorSlice.map(f=> {
      val iterator = f._2.toIterator
      var SE = 0.toDouble

      //1.1. prepare data from all elements
      while (iterator.hasNext) {
        val (sIdx, y) = iterator.next //y
        val value = y

        //1.1.1. get index of an entry
        val idx = sIdx.split("\t")
        assert(idx.length == arrnIndex.length)
        for (i<-0 until idx.length)
          arrnIndex(i) = idx(i).toInt

        var predictedValue:Double = 0
        for (r<-0 until rank){
          var dTmpProd: Double = 1
          for (i<-factors.indices) {
            dTmpProd *= factors(i).get(arrnIndex(i),r)
          }
          predictedValue += dTmpProd
        }

        val diff = value - predictedValue
        SE += diff * diff
      }

      SE
    }).reduce(_+_)

    if(sGradientName == "WLS")
      Loss = Loss/tensorSize

    //Return
    Loss
  }

  /** ******************************************************************************************
    * savePredictedResult
    *
    * Return: Unit
    * ******************************************************************************************/
  def savePredictedResult(sOutputPath: String,
                          sPrefix: String,
                          tensorSlice: RDD[(Int, Iterable[(String, Double)])],
                          factors: Array[Factor],
                          tensorSize: Long,
                          rank: Int): Unit ={
    val arrnIndex = new Array[Int](factors.length)
    val sDelimiter = "\t"

    val aResult = tensorSlice.map(f=> {
      val iterator = f._2.toIterator
      val res = new Array[String](f._2.size)
      var count = 0

      //1.1. prepare data from all elements
      while (iterator.hasNext) {
        val (sIdx, y) = iterator.next //y
        val value = y

        //1.1.1. get index of an entry
        val idx = sIdx.split(sDelimiter)
        assert(idx.length == arrnIndex.length)
        for (i<-0 until idx.length){
          arrnIndex(i) = idx(i).toInt

          if(i==0)
            res(count) = idx(i)
          else
            res(count) += sDelimiter + idx(i)
        }

        var predictedValue:Double = 0
        for (r<-0 until rank){
          var dTmpProd: Double = 1
          for (i<-factors.indices) {
            dTmpProd *= factors(i).get(arrnIndex(i),r)
          }
          predictedValue += dTmpProd
        }
        res(count) += sDelimiter + predictedValue + "\n"

        count += 1
      }

      res
    }).collect()

    //Save file
    val sFileName = sPrefix + ".txt"
    val fOutputDir = new File(sOutputPath)

    if (!fOutputDir.exists()) {
      fOutputDir.mkdirs()
    }

    val fOutput = new File(fOutputDir, sFileName)
    val fileWriterOutput = new FileWriter(fOutput)

    for(i<-0 until aResult.length){
      for(j<-0 until aResult(i).length){
        val data = aResult(i)(j).split(sDelimiter)

        val columnIdx = data(1).toInt

        var value = data(data.length-1).toDouble
        //convert back to value
        if(columnIdx<63)
          value *= 301894
        else if(columnIdx<360)
          value *= 301895
        else if(columnIdx<578)
          value *= 230410
        else if(columnIdx<719)
          value *= 215337
        else if(columnIdx<896)
          value *= 299896
        else if(columnIdx<1039)
          value *= 116771
        else if(columnIdx<1153)
          value *= 301895
        else if(columnIdx<1356)
          value *= 299898
        else if(columnIdx<1473)
          value *= 301895
        else if(columnIdx<1542)
          value *= 299897
        else if(columnIdx<1852)
          value *= 301897
        else
          value *= 100532

        var strToWrite = "" + data(0)
        //compose string to write
        for(k<-1 until data.length-1)
          strToWrite += sDelimiter + data(k)
        strToWrite += sDelimiter + value + "\n"

        fileWriterOutput.append(strToWrite)
        fileWriterOutput.flush()
      }
    }

    fileWriterOutput.flush()
    fileWriterOutput.close()
  }

  /** ******************************************************************************************
    * Function setOptimizationOption: sets optimization options
    * @param option
    * @return
    * ******************************************************************************************/
  def setOptimizationOption(option: String): this.type = {
    this.sOptimizationOption = option

    val options = sOptimizationOption.split("_")
    options(0) match{
      case "CF"   => sGradientName = options(2)
      // catch the default with a variable so you can print it
      case whoa   => println("Unexpected case: " + whoa.toString)
    }

    this
  }
  /** ******************************************************************************************
    * Function setStoppingCondition: sets stopping conditions
    *
    * @param condition
    * @return
    * ******************************************************************************************/
  def setStoppingCondition(condition: String): this.type = {
    val options = condition.split("_")
    var i = 0

    while(i < options.length){
      options(i).toInt match{
        case 0 => this.dMinDiff = options(i+1).toDouble
        case 1 => this.nMaxIter = options(i+1).toInt
        case 2 => this.dMaxHour = options(i+1).toDouble
        // catch the default with a variable so you can print it
        case whoa  => println("Unexpected case: " + whoa.toString)
      }
      i+=2
    }
    this
  }

  /** ******************************************************************************************
    * Function setOutputPath: sets output path
    * @param path
    * @return
    * ******************************************************************************************/
  def setOutputPath(path: String): this.type = {
    this.outputPath = path
    this
  }
  /** ******************************************************************************************
    * Function setRank: set decomposition rank (default = 3)
    * @param rank
    * @return
    * ******************************************************************************************/
  def setRank(rank: Int): this.type = {
    this.rank = rank
    this
  }
  /** ******************************************************************************************
    * Get rank
    * ******************************************************************************************/
  def getRank(): Int = rank

  /** ******************************************************************************************
    * Function setCheckPoint: sets checkpoint (default = 10)
    * @param checkPointIter
    * @return
    * ******************************************************************************************/
  def setCheckPoint(checkPointIter: Int): this.type = {
    this.checkPointIter = checkPointIter
    this
  }
  /** ******************************************************************************************
    * Function setSparkContext: sets Spark Context variable
    * @param sc
    * @return
    * ******************************************************************************************/
  def setSparkContext(sc: SparkContext): this.type = {
    this.sparkContext = sc
    this
  }
  /** ******************************************************************************************
    * Function setFinalRDDStorageLevel: sets storage level for final RDDs (all factors used in
    * TensorFactorizationModel). The default value is `MEMORY_AND_DISK`. Users can change it to
    * a serialized storage, e.g. `MEMORY_AND_DISK_SER` and set `spark.rdd.compress` to `true`
    * to reduce the space requirement, at the cost of speed.
    *
    * @param storageLevel
    * @return
    * ******************************************************************************************/
  def setFinalRDDStorageLevel(storageLevel: StorageLevel): this.type = {
    this.finalRDDStorageLevel = storageLevel
    this
  }
  /** ******************************************************************************************
    * Function setInputRDDStorageLevel: Sets storage level for input RDDs. The default
    * value is `MEMORY_AND_DISK`. Users can change it to a serialized storage, e.g.
    * `MEMORY_AND_DISK_SER` and set `spark.rdd.compress` to `true` to reduce the space requirement,
    * at the cost of speed.
    *
    * @param storageLevel
    * @return
    * ******************************************************************************************/
  def setInputRDDStorageLevel(storageLevel: StorageLevel): this.type = {
    this.inputRDDStorageLevel = storageLevel
    this
  }
}
