import java.io._
import java.util

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

/**
  * Created by dudo on 20/09/16.
  */
abstract class CoupledFactorization(nNumberOfTensor: Int,
                                    pnTensorMode: Array[Int],
                                    pnTensorDimension: Array[Array[Int]],
                                    pnTensorSplit: Array[Array[Int]],
                                    pnTensorFilePath: Array[String])
  extends Factorization with Serializable {

  protected var factors: Array[Array[Factor]] = null
  protected var dTrainLoss_PreIter = 1.0E80

  //Validation
  protected var bIsValidation = false
  protected val dBestValidationLoss = Array.fill[Double](nNumberOfTensor)(1.0E80)
  protected val sValidationFileName = Array.fill[String](nNumberOfTensor)("")        //Name of validation files
  protected var rddValidationTensor: Array[RDD[(Int, Iterable[(String, Double)])]] = null
  protected var lValidationTensorSize: Array[Long] = null

  //Required data
  protected var tensorSlice: Array[Array[RDD[(Int, Iterable[(String, Double)])]]] = null
  protected var bcFactors: Array[Array[Factor]] = null
  protected var bcRank: Int = 0
  protected var bcTensorSize: Array[Long] = null
  protected var bcOptimizationOption: String =""

  //Map of coupled Pair
  protected var sCoupleMapFileName = ""
  protected val coupleMap = new util.HashMap[String, String]()
  protected val coupleIdx = new util.HashMap[String, Int]()

  /** ******************************************************************************************
    * Function globalInit: initializes parameters
    *
    * @return tensorSlice, coupledSlice, factors, rank, tensorSize, optimizationOption
    * ********************************************************************************************/
  protected def globalInit(){
    //0. Double check parameters
    debugln("\tStep 0. Double check parameters")
    assert(nNumberOfTensor == pnTensorMode.length)
    assert(pnTensorDimension.length == pnTensorSplit.length)
    for(tensorIdx<-pnTensorDimension.indices){
      assert(pnTensorDimension(tensorIdx).length == pnTensorSplit(tensorIdx).length)
      for(modeIdx<-0 until pnTensorMode(tensorIdx))
        assert(pnTensorSplit(tensorIdx)(modeIdx)<=pnTensorDimension(tensorIdx)(modeIdx))
    }

    //1. Create tensorslice
    debugln("\tStep 1. Create Tensorslice")
    val tensorSize = createTensorSlice(nNumberOfTensor, pnTensorMode, pnTensorSplit)
    for (i <- 0 until nNumberOfTensor)
      debugln("\t\tTensorSize " + i + ": " + tensorSize(i))

    /**for caching data before factorization**/
    for(i<-0 until nNumberOfTensor){
      for(j<-0 until pnTensorMode(i))
        tensorSlice(i)(j).count()
    }

    //2. Initialization
    debugln("\tStep 2. Init Factors")
    factors = initFactors(1, 1)

    /** **********************************************************************
      * 3. For validation
      * **********************************************************************/
    if(bIsValidation){
      debugln("\tStep 3. Init Validation file")
      val (validationTensor, validationTensorSize) = createValidationTensor(nNumberOfTensor, pnTensorSplit(0)(0))

      rddValidationTensor = new Array[RDD[(Int, Iterable[(String, Double)])]](nNumberOfTensor)
      lValidationTensorSize = new Array[Long](nNumberOfTensor)

      for(i<-0 until nNumberOfTensor){
        rddValidationTensor(i) = validationTensor(i)
        lValidationTensorSize(i) = validationTensorSize(i)
        debugln("\t\tValidation size: " + lValidationTensorSize(i))
      }
    }

    //4. Create Couple Map
    debugln("\tStep 4. Create Couple Map")
    createCoupleMap(sCoupleMapFileName)

    //5. Broadcast data
    debugln("\tStep 5. Broadcast data")
    bcFactors = factors.map(f1 => f1.map(f2=>sparkContext.broadcast(f2).value))
    bcRank = sparkContext.broadcast(getRank()).value
    bcTensorSize = sparkContext.broadcast(tensorSize).value
    bcOptimizationOption = sparkContext.broadcast(sOptimizationOption).value
  }

  /** ******************************************************************************************
    * Function factorize: factorizes input tensor(s)
    * Child class must implement this function
    * ******************************************************************************************/
  override def factorize(): Double = {
    var lStartTime: Long = 0
    var nIter: Int = 0
    var bIsConverged = false
    val dValidationMSE = new Array[Double](nNumberOfTensor)

    //0. Global Init
    debugln("Global Init ... ")
    globalInit()

    //1. Init
    debugln("Local Init ... ")
    step1_LocalInit()

    //Get start time
    lStartTime = System.currentTimeMillis() / 1000L
    //2.1. Compute Loss
    val dTrainMSE = step2B_ComputeLoss()

    //2.2 Check if converged?
    bIsConverged = isConverged(dTrainMSE, lStartTime, nIter)
    /** **********************************************************************
      * For validation
      * **********************************************************************/
    if(bIsValidation){
      for(i<-sValidationFileName.indices) {
        if (sValidationFileName(i) != "")
          dValidationMSE(i) = computeValidationLoss(i)
      }
      findBestValidationLoss(dValidationMSE, lValidationTensorSize, nIter)
    }
    //2. Iteration
    debugln("Main iteration ... ")
    while(!bIsConverged) {
      //2.3 Increase the counter
      nIter +=1

      //2.4. Update all Factors
      step2_UpdateAllFactors()

      //2.5. Compute Loss
      val dTrainMSE = step2B_ComputeLoss()

      //2.8 Check if converged?
      bIsConverged = isConverged(dTrainMSE, lStartTime, nIter)

      /** **********************************************************************
        * For validation
        * **********************************************************************/
      if(bIsValidation){
        for(i<-sValidationFileName.indices) {
          if (sValidationFileName(i) != "")
            dValidationMSE(i) = computeValidationLoss(i)
        }
        findBestValidationLoss(dValidationMSE, lValidationTensorSize, nIter)
      }
    }

    //3. Free mem
    debugln("Free mem ... ")

    for(tensorIdx<-0 until nNumberOfTensor) {
      for (modeIdx <- 0 until pnTensorMode(tensorIdx)) {
        tensorSlice(tensorIdx)(modeIdx).unpersist()
      }
    }
    step3_Close()

    dTrainMSE(0)
  }

  def step1_LocalInit()
  def step2_UpdateAllFactors()
  def step3_Close()

  /** ******************************************************************************************
    * step2B_ComputeLoss
    * @return
    * ******************************************************************************************/
  def step2B_ComputeLoss(): Array[Double] ={
    val dTrainMSE = new Array[Double](nNumberOfTensor)

    for(tensorIdx<-0 until nNumberOfTensor) {
      dTrainMSE(tensorIdx) = computeTrainingLoss(tensorIdx)
    }

    dTrainMSE
  }

  def computeTrainingLoss(tensorIdx: Int):Double={
    computeLoss(tensorSlice(tensorIdx)(0), factors(tensorIdx), bcTensorSize(tensorIdx), bcRank)
  }

  def computeValidationLoss(tensorIdx: Int):Double ={
    computeLoss(rddValidationTensor(tensorIdx), factors(tensorIdx), lValidationTensorSize(tensorIdx), bcRank)
  }

  /** ******************************************************************************************
    * Function createTensorSlice: creates tensor slices
    *
    * @param numberOfTensor
    * @param pTensorMode
    * @param pTensorSplit
    * @return (tensor Slice RDD [], tensor Size [])
    * ******************************************************************************************/
  protected def createTensorSlice(numberOfTensor: Int,
                                  pTensorMode: Array[Int],
                                  pTensorSplit: Array[Array[Int]]): Array[Long] = {
    val arrRDDTensorData = pnTensorFilePath.map(s => sparkContext.textFile(s, pTensorSplit(0)(0)))
    val tensorSize = arrRDDTensorData.map(rdd=>rdd.count())
    tensorSlice = new Array[Array[RDD[(Int, Iterable[(String, Double)])]]](numberOfTensor)

    for(tensorIdx<-0 until numberOfTensor){
      assert(pTensorMode(tensorIdx)>=2)

      tensorSlice(tensorIdx) = new Array[RDD[(Int, Iterable[(String, Double)])]](pTensorMode(tensorIdx))

      for(modeIdx<-0 until pTensorMode(tensorIdx)){
        tensorSlice(tensorIdx)(modeIdx) = arrRDDTensorData(tensorIdx).map(f => {
          val ele = f.split("\t")
          val size = ele.length
          val key = ele(modeIdx).toInt
          val value = ele(size-1).toDouble

          var sIdx: String =  ele(0)
          for(i<-1 until size-1){
            sIdx +="\t" + ele(i)
          }

          (key, (sIdx, value))
        }).groupByKey().partitionBy(new HashPartitioner(pTensorSplit(tensorIdx)(modeIdx))).persist(inputRDDStorageLevel)
      }
    }

    tensorSize
  }

  /** ******************************************************************************************
    * Function createValidationTensor: creates validation tensor
    *
    * @param numberOfTensor
    * @param nTensorSplit
    * @return (tensor Slice RDD [], tensor Size [])
    * ******************************************************************************************/
  protected def createValidationTensor(numberOfTensor: Int,
                                       nTensorSplit: Int): (Array[RDD[(Int, Iterable[(String, Double)])]], Array[Long]) = {
    sValidationFileName.foreach(s=> require(s!=""))

    val arrRDDTensorData = sValidationFileName.map(s => sparkContext.textFile(s, nTensorSplit))
    val tensorSize = arrRDDTensorData.map(rdd=>rdd.count())
    val tensorSlice = new Array[RDD[(Int, Iterable[(String, Double)])]](numberOfTensor)

    for(tensorIdx<-0 until numberOfTensor) {
      tensorSlice(tensorIdx) = arrRDDTensorData(tensorIdx).map(f => {
        val ele = f.split("\t")
        val size = ele.length
        val key = ele(0).toInt
        val value = ele(size - 1).toDouble

        var sIdx: String = ele(0)
        for (i <- 1 until size - 1) {
          sIdx += "\t" + ele(i)
        }

        (key, (sIdx, value))
      }).groupByKey().partitionBy(new HashPartitioner(nTensorSplit)).persist(inputRDDStorageLevel)
    }

    (tensorSlice, tensorSize)
  }
  /** ******************************************************************************************
    * Function initFactors: initializes Factors
    *
    * @param initType 0: predefined value// 1: random
    * @param initVal  predefined value
    *
    * @return factors
    * ********************************************************************************************/
  protected def initFactors(initType: Int, initVal: Double=1):Array[Array[Factor]]={
    val nRank = getRank()
    //debugln("Rank in initFactors = " + nRank)

    val factors = new Array[Array[Factor]](nNumberOfTensor)
    for(tensorIdx<-0 until nNumberOfTensor){
      factors(tensorIdx) = new Array[Factor](pnTensorMode(tensorIdx))
      for(modeIdx<-0 until pnTensorMode(tensorIdx)){
        factors(tensorIdx)(modeIdx) = new Factor(pnTensorDimension(tensorIdx)(modeIdx), nRank)
        factors(tensorIdx)(modeIdx).init(initType, initVal)
      }
    }

    factors
  }

  /** ******************************************************************************************
    * Function extractFactor: extracts Factors
    *
    * @param tensorIdx
    * @param modeIdx
    * @param fact
    * @return
    * ******************************************************************************************/
  protected def extractFactor(tensorIdx: Int,
                              modeIdx: Int,
                              fact: Array[(Int, Array[Double])]){
    for (i<-fact.indices){
      factors(tensorIdx)(modeIdx).setRow(fact(i)._1, fact(i)._2)
    }
  }
  /** ******************************************************************************************
    * Function copyFactors: copies tensor(tensorIdx2, modeIdx2) to tensor(tensorIdx1, modeIdx1)
    *
    * @param tensor1Idx: index of tensor 1
    * @param mode1Idx: mode index of tensor 1
    * @param tensor2Idx: index of tensor 2
    * @param mode2Idx: mode index of tensor 2
    * ******************************************************************************************/
  protected def copyFactors(tensor1Idx: Int, mode1Idx: Int, tensor2Idx: Int, mode2Idx: Int): Unit ={
    for (i<-0 until factors(tensor1Idx)(mode1Idx).getLength){
      factors(tensor1Idx)(mode1Idx).setRow(i, factors(tensor2Idx)(mode2Idx).getRow(i))
    }
  }
  /** ******************************************************************************************
    * saveFactors
    * ******************************************************************************************/
  def saveFactors(): Unit ={
    for(tensorIdx<-0 until nNumberOfTensor){
      saveFactors("", tensorIdx)
    }
  }
  /** ******************************************************************************************
    * saveFactors
    * ******************************************************************************************/
  def saveFactors(sPrefix: String, tensorIdx: Int): Unit ={
    for(modeIdx<-factors(tensorIdx).indices) {
      //Create file for writing
      val str: String = sPrefix + "U_" + tensorIdx + "_" + modeIdx + ".txt"
      factors(tensorIdx)(modeIdx).saveFactors(outputPath, str)
    }
  }
  /** ******************************************************************************************
    * Function saveLog: saves logging
    * ******************************************************************************************/
  protected def saveLog(dMSE: Array[Double], dSMSE: Double, lTime: Long, nIter: Int): Unit = {
    var strLoggingToWrite: String = ""

    if(nIter==0){
      strLoggingToWrite = "Iter"
      strLoggingToWrite += "\tTime"
      for(i<-dMSE.indices)
        strLoggingToWrite += "\tSE(" + i + ")"
      strLoggingToWrite += "\tSSE"
    }

    strLoggingToWrite += "\n" + nIter +"\t" + lTime
    for(i<-dMSE.indices)
      strLoggingToWrite += "\t" + dMSE(i)

    strLoggingToWrite += "\t" + dSMSE

    logInfo(strLoggingToWrite)

    //write log
    try {
      val fOutputDir = new File(outputPath)
      val fLogging: File = new File(fOutputDir, "Log.txt")

      val fw = new FileWriter(fLogging,true) //the true will append the new data
      fw.write(strLoggingToWrite)  //appends the string to the file
      fw.flush()
      fw.close()
    }
    catch {
      case ioe: IOException => System.err.println("IOException: " + ioe.getMessage())
    }
  }

  /** ******************************************************************************************
    * Function isConverged: checks if converged
    *
    * @param dMSE
    * @param lStartTime
    * @param nIter
    * @return true/false
    * ********************************************************************************************/
  protected def isConverged(dMSE: Array[Double], lStartTime: Long, nIter: Int): Boolean ={
    var bIsConverged = false
    val dSMSE = dMSE.sum

    val dSMSEDiff = dTrainLoss_PreIter - dSMSE
    val relfit = dSMSEDiff/dTrainLoss_PreIter

    if(dMinDiff!=0){//min diff stopping condition
      bIsConverged = true
      if (relfit>=dMinDiff && !relfit.isNaN && !relfit.isInfinity)
        bIsConverged = false

      if(bIsConverged)
        debugln("\tConverged - min diff")
    }

    //#2. Max executed hour stoping condition readches
    val lEndTime = System.currentTimeMillis() / 1000L
    val lTimeLapsed = lEndTime - lStartTime
    debugln("\tTime: " + lTimeLapsed)
    for(i<-dMSE.indices)
      debug("\t\t" + i + "\t-- MSE:\t"+ dMSE(i))
    debugln("\t-- SMSE:\t"+ dSMSE+ "\tDiff:\t" + dSMSEDiff + "\tDiff/Pre:\t" + relfit)

    if (dMaxHour!=0&&lTimeLapsed/3600>dMaxHour) {
      bIsConverged = true
      debugln("\tConverged - max hour")
    }

    //#3. Max executed iteration stoping condition readches
    if (nMaxIter!=0&&nIter==nMaxIter) {
      bIsConverged = true
      debugln("\tConverged - max iter")
    }

    /**Logging**/
    if(!bIsConverged&&(nIter%checkPointIter==0)){
      saveLog(dMSE, dSMSE, lTimeLapsed, nIter)
      saveFactors()
    }
    else{
      if(bIsConverged&&relfit>0)
        saveFactors()

      saveLog(dMSE, dSMSE, lTimeLapsed, nIter)
    }

    dTrainLoss_PreIter = dSMSE
    bIsConverged
  }

  /** ******************************************************************************************
    * Function findBestValidationLoss: performs some operations if this is the best validation
    *
    * @param dMSE
    * @param tensorSize
    * @param nIter
    * @return true/false
    * ******************************************************************************************/
  protected def findBestValidationLoss(dMSE: Array[Double], tensorSize: Array[Long], nIter: Int): Unit ={
    val dRMSE = dMSE

    for(i<-dRMSE.indices){
      if(sGradientName == "LS")
        dRMSE(i)=Math.sqrt(dRMSE(i)/tensorSize(i))
      else
        dRMSE(i)=Math.sqrt(dRMSE(i))

      if(dRMSE(i)<dBestValidationLoss(i)){
        saveFactors("Validation_", i)
        dBestValidationLoss(i) = dRMSE(i)
      }
    }

    //write log
    try {
      val fOutputDir = new File(outputPath)
      val fLogging: File = new File(fOutputDir, "Validation_Log.txt")

      val fw = new FileWriter(fLogging,true) //the true will append the new data
      var strToWrite = "" + nIter
      for(i<-dRMSE.indices){
        strToWrite += "\t" + dRMSE(i)
      }
      strToWrite += "\n"

      fw.write(strToWrite)  //appends the string to the file
      fw.close()
    }
    catch {
      case ioe: IOException => System.err.println("IOException: " + ioe.getMessage())
    }
  }

  /** ******************************************************************************************
    * Function getNumberOfTensor: gets numberofTensor
    * ******************************************************************************************/
  def getNumberOfTensor: Int = nNumberOfTensor
  /** ******************************************************************************************
    * Function getMode: gets mode
    * ******************************************************************************************/
  def getMode: Array[Int] = pnTensorMode
  /** ******************************************************************************************
    * Function getDimension: gets dimension
    * ******************************************************************************************/
  def getDimension: Array[Array[Int]] = pnTensorDimension
  /** ******************************************************************************************
    * Set validation filename
    * @param filename
    * ******************************************************************************************/
  def setValidationFileName(filename: Array[String]){
    require(sValidationFileName.length == filename.length)
    bIsValidation = true

    for(i<-filename.indices)
      sValidationFileName(i) = filename(i)
  }
  /** ******************************************************************************************
    * setCoupleMap
    *
    * @param filename
    * @return
    * ******************************************************************************************/
  def setCoupleMap(filename: String): this.type = {
    this.sCoupleMapFileName = filename
    this
  }

  /** ******************************************************************************************
    * createCoupleMap
    *
    * @param filename
    * ******************************************************************************************/
  def createCoupleMap(filename:String){
    var count = 0

    val fInput = new File(filename)
    val fileReader = new FileReader(fInput)
    val reader = new BufferedReader(fileReader)
    //read one line
    var line = reader.readLine()
    while (line!=null) {
      val data = line.split("\t")
      val key = data(0)
      val value = data(1)

      coupleMap.put(key, value)
      coupleMap.put(value, key)
      coupleIdx.put(key, count)
      coupleIdx.put(value, count)

      count += 1
      //read next line
      line = reader.readLine()
    }
    reader.close()
  }

  /** ******************************************************************************************
    * isCoupled
    *
    * @param tensorIdx
    * @param modeIdx
    * @return
    * ******************************************************************************************/
  def isCoupled(tensorIdx: Int, modeIdx: Int): Boolean ={
    val key = "" + tensorIdx + "_" + modeIdx
    coupleMap.containsKey(key)
  }

  /** ******************************************************************************************
    * getCoupledTensorMode
    *
    * @param tensorIdx
    * @param modeIdx
    * @return
    * ******************************************************************************************/
  def getCoupledTensorMode(tensorIdx: Int, modeIdx: Int):(Int, Int)={
    require(isCoupled(tensorIdx,modeIdx))
    val key = "" + tensorIdx + "_" + modeIdx

    val value = coupleMap.get(key)
    val data = value.split("_")
    val coupledTensorIdx = data(0).toInt
    val coupledModeIdx = data(1).toInt

    (coupledTensorIdx, coupledModeIdx)
  }

  /** ******************************************************************************************
    * getCoupleIdx
    *
    * @param tensorIdx
    * @param modeIdx
    * @return
    * ******************************************************************************************/
  def getCoupleIdx(tensorIdx: Int, modeIdx: Int): Int ={
    require(isCoupled(tensorIdx,modeIdx))
    val key = "" + tensorIdx + "_" + modeIdx
    coupleIdx.get(key)
  }
}
