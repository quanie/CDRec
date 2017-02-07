import scala.annotation.elidable
import scala.annotation.elidable._

abstract class Application{
  protected var DEBUG = true

  protected var gsStoppingCondition: String = ""            //Stopping conditions
  protected var gsOptimizationOption: String = ""           //Optimization Option

  //Input data
  protected var gnNumberOfDataSet:Int = 1
  protected var gpnTensorMode: Array[Int] = null            //Mode of all coupled tensor
  protected var gpnTensorLength: Array[Array[Int]] = null   //Length of all coupled tensor
  protected var gpnTensorSplit: Array[Array[Int]] = null    //Split of all coupled tensor
  protected var gplTensorSize: Array[Long] = null           //Size of tensor
  protected var gnRank: Int = 3
  protected var gpsInputPath: Array[String] = null          //input paths
  protected var gpsValidationPath: Array[String] = null          //input paths
  protected var gsOutputPath: String = ""                   //output path
  protected var gsCoupleMapFilePath: String = ""                  //coupled map file path

  //Variable for file I/O
  //private val gsdfsPath = "hdfs://atlas5:9000/user/dudo/" //"/home/dudo/spark-1.6.0-bin-hadoop2.6/" //
  //private val gsDriverOutputPath = "/scratch/dudo/LiST/"//"/home/dudo/Research/Paper/LiST/LiST/"

  @elidable(FINE) def debugln(msg: String)      = println(msg)
  @elidable(FINE) def debug(msg: String)        = print(msg)

  /** ******************************************************************************************
    * Name: displayArguments
    * Function: display all parameters for debug purpose
    *
    * Return:  void
    * ********************************************************************************************/
  def displayArguments(): Unit ={
    debugln("Number of dataSet: " + gnNumberOfDataSet)
    //Mode and Length
    for(i<-0 until gnNumberOfDataSet) {
      debug("Tensor " + (i+1) + ":\n\tMode = " + gpnTensorMode(i) + "\n\tLength =")

      for(j<-0 until gpnTensorMode(i))
        debug("\t" + gpnTensorLength(i)(j))
      debug("\n")
    }

    //Split
    for(i<-0 until gnNumberOfDataSet) {
      debug("Tensor " + (i+1) + ":\n\tMode = " + gpnTensorMode(i) + "\n\tSplit =")

      for(j<-0 until gpnTensorMode(i))
        debug("\t" + gpnTensorSplit(i)(j))
      debug("\n")
    }

    debugln("Rank: " + gnRank)

    if(gnNumberOfDataSet>1)
      debugln("Couple Map file path: " + gsCoupleMapFilePath)

    //Optimization Options
    val options = gsOptimizationOption.split("_")
    debugln("Optimization options: ")
    options(0) match{
      case "CF" => {
        debugln("\t\t Method: Closed form")
        debugln("\t\t Lambda: " + options(1))
        options(2) match{
          case "LS" => debugln("\t\t least squares error |Ax - b|^2")
          case "WLS" => debugln("\t\t weighted least squares error 1/w * |Ax - b|^2")
          case other => {
            debugln("\t\t unexpected least squares function " + other)
            debugln("\t\t Usage: LS for least squares error |Ax - b|^2")
            debugln("\t\t Usage: WLS for weighted least squares error 1/w * |Ax - b|^2")
          }
        }
      }
      case whoa => println("\t\tUnexpected optimization options: " + whoa)
    }

    //Stopping conditions
    val conditions = gsStoppingCondition.split("_")
    var i = 0

    debugln("Stopping conditions: ")
    while(i < conditions.length){
      conditions(i).toInt match{
        case 0 => debugln("\t\t Difference is smaller than " + conditions(i+1).toDouble)
        case 1 => debugln("\t\t Max Iteration is " + conditions(i+1).toInt)
        case 2 => debugln("\t\t Running hour is less than " + conditions(i+1).toDouble + " hours")
        // catch the default with a variable so you can print it
        case whoa  => println("\t\tUnexpected condition: " + whoa.toString)
      }
      i+=2
    }


    for(i<-0 until gnNumberOfDataSet){
      debugln("Tensor " + (i+1) +" path: " + gpsInputPath(i))
      if(gpsValidationPath!=null)
        debugln("Tensor " + (i+1) +" validation path: " + gpsValidationPath(i))
    }
    debugln("Output path: " + gsOutputPath)
  }

  /** ******************************************************************************************
    * Name: dataPreparation
    * Function: prepare data and environment for code to run
    *
    * Return:  void
    * ********************************************************************************************/
  def dataPreparation(args: Array[String]): Unit ={
    var nArgsIdx:Int = 0

    if(DEBUG)
    {
      debugln("Arguments: ")
      for(i<-args.indices)
        debugln("\t" + i + " : " + args(i))
    }

    //#0: number of data set
    gnNumberOfDataSet = args(nArgsIdx).toInt
    assert(gnNumberOfDataSet>0)

    gpnTensorMode = new Array[Int](gnNumberOfDataSet)
    gpnTensorLength = new Array[Array[Int]](gnNumberOfDataSet)
    gpnTensorSplit = new Array[Array[Int]](gnNumberOfDataSet)
    gplTensorSize = new Array[Long](gnNumberOfDataSet)
    gpsInputPath = new Array[String](gnNumberOfDataSet)

    var nNumberOfSlice = 0

    //mode and length of each data
    for(i<-0 until gnNumberOfDataSet){
      nArgsIdx +=1
      val sInString = args(nArgsIdx).split("_")
      val nMode = sInString.length
      assert(nMode>1)

      gpnTensorMode(i) = nMode
      gpnTensorLength(i) = new Array[Int](nMode)
      for(j<-0 until nMode){
        gpnTensorLength(i)(j) = sInString(j).toInt
        nNumberOfSlice += gpnTensorLength(i)(j)
      }
    }
    //Split
    nArgsIdx +=1
    val splitNumber = args(nArgsIdx).toInt

    for(i<-0 until gnNumberOfDataSet){
      gpnTensorSplit(i) = new Array[Int](gpnTensorMode(i))
      for(j<-0 until gpnTensorMode(i)){
        if(gpnTensorLength(i)(j)>splitNumber)
          gpnTensorSplit(i)(j) = splitNumber
        else
          gpnTensorSplit(i)(j) = gpnTensorLength(i)(j)
      }
    }

    //Rank
    nArgsIdx +=1
    gnRank = args(nArgsIdx).toInt	//Rank of the factorization

    //Couple Map file path
    if(gnNumberOfDataSet>1){
      nArgsIdx +=1
      gsCoupleMapFilePath = args(nArgsIdx)
    }

    //Optimization Option
    nArgsIdx +=1
    gsOptimizationOption = args(nArgsIdx)

    //Stopping Condition
    nArgsIdx +=1
    gsStoppingCondition = args(nArgsIdx)

    //Input and output path
    for(i<-0 until gnNumberOfDataSet){
      nArgsIdx +=1
      //gpsInputPath(i) = gsdfsPath + args(nArgsIdx)
      gpsInputPath(i) = args(nArgsIdx) //must be specified full path on HDFS
    }
    nArgsIdx +=1
    //gsOutputPath = gsdfsPath + args(nArgsIdx)
    //gsOutputPath = gsDriverOutputPath + args(nArgsIdx)
    gsOutputPath = args(nArgsIdx) //must be specified full path on local

    //Prepare output path
    for(i<-0 until gnNumberOfDataSet)
    {
      var str =""
      if (gpsInputPath(i).contains(".")) {
        if(gpsInputPath(i).contains("/"))
          str = gpsInputPath(i).substring(gpsInputPath(i).lastIndexOf("/")+1, gpsInputPath(i).lastIndexOf("."))
        else
          str = gpsInputPath(i).substring(0, gpsInputPath(i).lastIndexOf("."))
        gsOutputPath += str
      }
      else{
        if(gpsInputPath(i).contains("/"))
          str = gpsInputPath(i).substring(gpsInputPath(i).lastIndexOf("/")+1, gpsInputPath(i).lastIndexOf("."))
        else
          str = gpsInputPath(i)
        gsOutputPath += str
      }
    }

    //validation
    nArgsIdx +=1
    if (nArgsIdx<args.length){
      gpsValidationPath = new Array[String](gnNumberOfDataSet)
      gpsValidationPath(0) = args(nArgsIdx) //must be specified full path on HDFS

      for(i<-1 until gnNumberOfDataSet){
        nArgsIdx +=1
        gpsValidationPath(i) = args(nArgsIdx) //must be specified full path on HDFS
      }
    }

    //DEBUG
    /*nArgsIdx +=1
    if (nArgsIdx<args.length){
      if (args(nArgsIdx).toInt ==0 )
        DEBUG = false
      else
        DEBUG = true
    }
    */

    debugln("Done data preparation")

    //Display arguments
    if(DEBUG)
      displayArguments()
  }
}