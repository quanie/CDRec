import org.apache.spark.rdd.RDD

/**
  * Created by dudo on 26/10/16.
  *     L = |X1 - U1 [V0|V1]| + |X2 - U2 [V0|V2]| + |U1_c - U2_c|
  *
  *     Note: coupled mode of X1 and X2 must be the same!!!!
  *           i.e., L = |X1 - U1 [V0|V1]| + |X2 - U2 [V0|V2]| + |U1_c - U2_c|
  *             or  L = |X1 - [U0|U1] V1| + |X2 - [U0|U2] V2| + |V1_c - V2_c|
  */
class CDRec(nNumberOfTensor: Int,
            pnTensorMode: Array[Int],
            pnTensorDimension: Array[Array[Int]],
            pnTensorSplit: Array[Array[Int]],
            pnTensorFilePath: Array[String])
  extends CoupledFactorization(nNumberOfTensor, pnTensorMode, pnTensorDimension, pnTensorSplit, pnTensorFilePath) with Serializable {

  protected var coupledSlice: Array[RDD[(Int, (Iterable[(String, Double)],Iterable[(String, Double)]))]] = null
  protected var bcCommonColumn: Int = 0
  protected var bcMeans: Array[Array[Array[Double]]] = null

  override def step1_LocalInit(): Unit = {
    debugln("Rank in CDRec: " + bcRank)
    val nCommonColumn = 3

    coupledSlice = createCoupledTensor(tensorSlice)
    bcMeans = factors.map(f1 => f1.map(f2=>sparkContext.broadcast(f2.getColumnMean()).value))

    bcCommonColumn = sparkContext.broadcast(nCommonColumn).value
  }

  override def step2_UpdateAllFactors(): Unit ={
    for(tensorIdx<-0 until nNumberOfTensor) {
      for (modeIdx <- 0 until pnTensorMode(tensorIdx)) {
        if(isCoupled(tensorIdx, modeIdx)){
          val (coupledTensorIdx, coupledModeIdx) = getCoupledTensorMode(tensorIdx, modeIdx)

          if(bcCommonColumn>0){
            if(tensorIdx==0){
              println("\tUpdate common V")
              updateCommonCoupledFactors(tensorIdx, modeIdx, coupledTensorIdx, coupledModeIdx, coupledSlice(0), bcFactors, bcTensorSize, bcRank, bcCommonColumn, bcOptimizationOption)
            }
          }

          if(bcCommonColumn<bcRank){
            println("\tUpdate specific V" + tensorIdx)
            updateSpecificCoupledFactors(tensorIdx, modeIdx, tensorSlice(tensorIdx)(modeIdx), bcFactors, bcTensorSize(tensorIdx), bcRank, bcCommonColumn, bcOptimizationOption)
          }
        }
        else /**This is where coupled mode of X1 and X2 must be the same!!!!**/{
          println("\tUpdate U" + tensorIdx)
          updateFactors(tensorIdx, modeIdx, tensorSlice(tensorIdx)(modeIdx), bcFactors, bcMeans, bcTensorSize(tensorIdx), bcRank, bcOptimizationOption)
        }
      }
    }

    //not yet update bcMeans
    bcMeans = factors.map(f1 => f1.map(f2=>sparkContext.broadcast(f2.getColumnMean()).value))
  }

  override def step3_Close(): Unit ={
    for(tensorIdx<-0 until nNumberOfTensor) {
      for (modeIdx <- 0 until pnTensorMode(tensorIdx)) {
        if(isCoupled(tensorIdx, modeIdx)){
          val (coupledTensorIdx, _) = getCoupledTensorMode(tensorIdx, modeIdx)

          if(tensorIdx<coupledTensorIdx) {
            val coupledSliceIdx = getCoupleIdx(tensorIdx, modeIdx)
            coupledSlice(coupledSliceIdx).unpersist()
          }
        }
      }
    }
  }

  /** ******************************************************************************************
    * Function updateFactors: solves x that minimize |y-ax|^2^
    *
    * @param tensorIdx: index of tensor
    * @param modeIdx: mode index of tensor
    * @param tensorSlice:
    * @param factors:
    * @param tensorSize
    * @param rank
    * @param optimizationOption
    *
    * @return loss
    * ********************************************************************************************/
  protected def updateFactors(tensorIdx: Int,
                              modeIdx: Int,
                              tensorSlice: RDD[(Int, Iterable[(String, Double)])],
                              factors: Array[Array[Factor]],
                              means: Array[Array[Array[Double]]],
                              tensorSize: Long,
                              rank: Int,
                              optimizationOption: String): Unit ={
    val arrnIndex = new Array[Int](factors(tensorIdx).length) //index of an entry (mode = factors.length)
    val dWeight: Double = 1.toDouble/tensorSize
    val dWeight2: Double = 1.toDouble/rank

    var otherTensorIdx = 0
    if(tensorIdx==0)
      otherTensorIdx = 1

    //1. Compute for each tensor slices cached in this worker node
    val fact = tensorSlice.map(f=> {
      //1.1. declares variables
      var nCount: Int = 0 //counter
      val iterator = f._2.toIterator

      /** needs two storage variables: points and ne**/
      val points = new Array[(Double, Array[Double], Double)](f._2.size+rank)  //(y, a, weight)
      val ne = new NormalEquation(rank) //Create NormalEquation variable
      ne.reset()

      //get xOld
      val xOld = factors(tensorIdx)(modeIdx).getRow(f._1)

      //1.1. prepare data from all elements
      while (iterator.hasNext) {
        val a = Array.fill[Double](rank)(1) //a
        val (sIdx, y) = iterator.next //y

        //1.1.1. get index of an entry
        val idx = sIdx.split("\t")
        require(idx.length == factors(tensorIdx).length)
        for (i <- 0 until idx.length)
          arrnIndex(i) = idx(i).toInt

        //1.1.2. Compute a
        for (i<-factors(tensorIdx).indices) {
          if (i != modeIdx){
            for (r <- 0 until rank)
              a(r) *= factors(tensorIdx)(i).get(arrnIndex(i),r)
          }
        }

        //1.1.3. adds to two variables
        if(sGradientName=="WLS"){
          ne.add(a, y, dWeight)
          points(nCount) = (y, a, dWeight)
        }
        else{
          ne.add(a, y)
          points(nCount) = (y, a, 1)
        }

        //1.1.4. update counter
        nCount += 1
      }

      //cluster difference -- separated columns
      /*require(commonRow<=rank)
      for(columnIdx<-0 until commonRow){*/
      for(columnIdx<-0 until rank){
        val b = Array.fill[Double](rank)(0) //b

        val thisValue = factors(tensorIdx)(modeIdx).get(f._1, columnIdx)
        var z:Double = 0.toDouble
        val len = factors(tensorIdx)(modeIdx).getLength

        b(columnIdx) = 1.toDouble/len
        val factorMean = means(tensorIdx)(modeIdx)(columnIdx) - thisValue/len
        val otherTensorFactorMean = means(otherTensorIdx)(modeIdx)(columnIdx)
        z = otherTensorFactorMean - factorMean

        if(sGradientName=="WLS"){
          ne.add(b, z, dWeight2)
          points(nCount) = (z, b, dWeight2)
        }
        else{
          ne.add(b, z)
          points(nCount) = (z, b, 1)
        }

        nCount += 1
      }

      //1.2. Compute x
      val xNew = performOptimization(ne, points, xOld, optimizationOption)

      //1.3. Return value
      (f._1, xNew)
    }).collect()

    //2. extract to a factor
    extractFactor(tensorIdx, modeIdx, fact)
  }

  /** ******************************************************************************************
    * Function updateSpecificCoupledFactors: solves x that minimize |y-ax|^2^
    *
    * @param tensorIdx: index of tensor
    * @param modeIdx: mode index of tensor
    * @param tensorSlice:
    * @param factors:
    * @param tensorSize
    * @param rank
    * @param optimizationOption
    *
    * @return loss
    * ********************************************************************************************/
  protected def updateSpecificCoupledFactors(tensorIdx: Int,
                                             modeIdx: Int,
                                             tensorSlice: RDD[(Int, Iterable[(String, Double)])],
                                             factors: Array[Array[Factor]],
                                             tensorSize: Long,
                                             rank: Int,
                                             commonColumn: Int,
                                             optimizationOption: String): Unit ={
    require(commonColumn<rank)
    val dimension = rank - commonColumn
    val start = commonColumn
    val end = rank

    val arrnIndex = new Array[Int](factors(tensorIdx).length) //index of an entry (mode = factors.length)
    val dWeight: Double = 1.toDouble/tensorSize

    //1. Compute for each tensor slices cached in this worker node
    val fact = tensorSlice.map(f=> {
      //1.1. declares variables
      var nCount: Int = 0 //counter
      val iterator = f._2.toIterator

      /** needs two storage variables: points and ne**/
      val points = new Array[(Double, Array[Double], Double)](f._2.size)  //(y, a, weight)
      val ne = new NormalEquation(dimension) //Create NormalEquation variable
      ne.reset()

      //get xOld <=== dimension: rank - commonColumn
      val xOld = new Array[Double](dimension)
      val xRow = factors(tensorIdx)(modeIdx).getRow(f._1)
      for(i<-start until end)
        xOld(i-start) = xRow(i)

      //1.1. prepare data from all elements
      while (iterator.hasNext) {
        val a = Array.fill[Double](dimension)(1) //a
        val (sIdx, y) = iterator.next //y

        //1.1.1. get index of an entry
        val idx = sIdx.split("\t")
        require(idx.length == factors(tensorIdx).length)
        for (i <- 0 until idx.length)
          arrnIndex(i) = idx(i).toInt

        var sumCommonColumn = 0.toDouble
        val sum = Array.fill[Double](commonColumn)(1)

        //1.1.2. Compute a
        for (i<-factors(tensorIdx).indices) {
          if (i != modeIdx){
            for (r <- start until end)
              a(r-start) *= factors(tensorIdx)(i).get(arrnIndex(i),r)
          }


          for (r <- 0 until commonColumn)
            sum(r) *= factors(tensorIdx)(i).get(arrnIndex(i),r)
        }

        for (r <- 0 until commonColumn)
          sumCommonColumn+=sum(r)
        val value = y - sumCommonColumn

        //1.1.3. adds to two variables
        if(sGradientName=="WLS"){
          ne.add(a, value, dWeight)
          points(nCount) = (value, a, dWeight)
        }
        else{
          ne.add(a, value)
          points(nCount) = (value, a, 1)
        }

        //1.1.4. update counter
        nCount += 1
      }

      //1.2. Compute x
      val xNew = performOptimization(ne, points, xOld, optimizationOption)

      //1.3. Return value
      (f._1, xNew)
    }).collect()

    //2. extract to a factor
    extractFactor(tensorIdx, modeIdx, fact, start, end)
  }

  /** ******************************************************************************************
    * updateCommonCoupledFactors
    * @param tensorIdx
    * @param modeIdx
    * @param coupledTensorIdx
    * @param coupledModeIdx
    * @param tensorSlice
    * @param factors
    * @param tensorSize
    * @param rank
    * @param commonColumn
    * @param optimizationOption
    * ******************************************************************************************/
  protected def updateCommonCoupledFactors(tensorIdx: Int,
                                           modeIdx: Int,
                                           coupledTensorIdx: Int,
                                           coupledModeIdx: Int,
                                           tensorSlice: RDD[(Int, (Iterable[(String, Double)],Iterable[(String, Double)]))],
                                           factors: Array[Array[Factor]],
                                           tensorSize: Array[Long],
                                           rank: Int,
                                           commonColumn:Int,
                                           optimizationOption: String): Unit ={
    require(commonColumn>0&&tensorIdx==0)
    val dimension = commonColumn
    val start = 0
    val end = commonColumn

    val arrnIndex1 = new Array[Int](factors(tensorIdx).length)          //index of an entry of main tensor
    val arrnIndex2 = new Array[Int](factors(coupledTensorIdx).length)   //index of an entry of coupled tensor

    val dWeight1: Double = 1.toDouble/tensorSize(tensorIdx)
    val dWeight2: Double = 1.toDouble/tensorSize(coupledTensorIdx)

    //1. Compute for each tensor slices cached in this worker node
    val fact = tensorSlice.map(f=> {
      //1.1. declares variables
      var nCount: Int = 0 //counter
      val iterator1 = f._2._1.toIterator
      val iterator2 = f._2._2.toIterator

      /** needs two storage variables: points and ne**/
      val points = new Array[(Double, Array[Double], Double)](f._2._1.size + f._2._2.size) //(y&z, a&b, weight)
      val ne = new NormalEquation(dimension) //Create NormalEquation variable
      ne.reset()
      val ne2 = new NormalEquation(dimension) //Create NormalEquation variable
      ne2.reset()

      //get xOld <=== dimension: commonColumn
      val xOld = new Array[Double](dimension)
      val xRow = factors(tensorIdx)(modeIdx).getRow(f._1)
      for(i<-start until end)
        xOld(i-start) = xRow(i)

      //1.1. prepare data from the main tensor's all elements
      while (iterator1.hasNext) {
        val a = Array.fill[Double](dimension)(1) //a
        val (sIdx, y) = iterator1.next      //y

        //1.1.1. get index of an entry
        val idx = sIdx.split("\t")
        require(idx.length == arrnIndex1.length)
        for (i <- 0 until idx.length)
          arrnIndex1(i) = idx(i).toInt

        var sumCommonColumn = 0.toDouble
        val sum = Array.fill[Double](rank - commonColumn)(1)

        //1.1.2. Compute a
        for (i<-factors(tensorIdx).indices) {
          if (i != modeIdx){
            for (r <- start until end)
              a(r-start) *= factors(tensorIdx)(i).get(arrnIndex1(i),r)
          }

          for (r <- commonColumn until rank)
            sum(r-commonColumn) *= factors(tensorIdx)(i).get(arrnIndex1(i),r)
        }

        for (r <- commonColumn until rank)
          sumCommonColumn+=sum(r-commonColumn)
        val value = y - sumCommonColumn

        //1.1.3. adds to two variables
        if(sGradientName=="WLS"){
          ne.add(a, value, dWeight1)
          points(nCount) = (value, a, dWeight1)
        }
        else{
          ne.add(a, value)
          points(nCount) = (value, a, 1)
        }

        //1.1.4. update counter
        nCount += 1
      }

      //1.2. prepare data from from the coupled tensor's all elements
      while (iterator2.hasNext) {
        val b = Array.fill[Double](dimension)(1) //b
        val (sIdx, z) = iterator2.next      //z

        //1.2.1. get index of an entry
        val idx = sIdx.split("\t")
        require(idx.length == arrnIndex2.length)
        for (i <- 0 until idx.length)
          arrnIndex2(i) = idx(i).toInt

        //1.2.2. Compute b
        var sumCommonColumn = 0.toDouble
        val sum = Array.fill[Double](rank - commonColumn)(1)

        for (i<-factors(coupledTensorIdx).indices) {
          if (i != coupledModeIdx){
            for (r <- start until end)
              b(r-start) *= factors(coupledTensorIdx)(i).get(arrnIndex2(i),r)
          }

          for (r <- commonColumn until rank)
            sum(r-commonColumn) *= factors(coupledTensorIdx)(i).get(arrnIndex2(i),r)
        }

        for (r <- commonColumn until rank)
          sumCommonColumn+=sum(r-commonColumn)
        val value = z - sumCommonColumn

        //1.2.3. adds to two variables
        if(sGradientName=="WLS"){
          ne2.add(b, value, dWeight2)
          points(nCount) = (value, b, dWeight2)
        }
        else{
          ne2.add(b, value)
          points(nCount) = (value, b, dWeight2)
        }

        //1.2.4. update counter
        nCount += 1
      }

      //1.3. Merge 2 normal equations
      ne.merge(ne2)

      //1.4. Compute x
      val xNew = performOptimization(ne, points, xOld, optimizationOption)

      //1.5. Return value
      (f._1, xNew)
    }).collect()

    //2. extract to a factor
    extractFactor(tensorIdx, modeIdx, fact, start, end)
    extractFactor(coupledTensorIdx, coupledModeIdx, fact, start, end)
  }

  /** ******************************************************************************************
    * Function extractFactor: extracts Factors
    *
    * @param tensorIdx
    * @param modeIdx
    * @param fact
    * @return loss
    * ******************************************************************************************/
  protected def extractFactor(tensorIdx: Int,
                              modeIdx: Int,
                              fact: Array[(Int, Array[Double])],
                              start: Int,
                              end: Int){

    for (i<-fact.indices){
      for(r<-start until end)
        factors(tensorIdx)(modeIdx).set(fact(i)._1, r, fact(i)._2(r-start))
    }
  }

  /** ******************************************************************************************
    * Function createCoupledTensor: create coupledTensor
    * Child class must implement this function
    * ******************************************************************************************/
  protected def createCoupledTensor(tensorSlice: Array[Array[RDD[(Int, Iterable[(String, Double)])]]]): Array[RDD[(Int, (Iterable[(String, Double)],Iterable[(String, Double)]))]] ={
    //3. Joined coupled data
    debugln("Step 3. Joined Coupled Data")
    val coupledSlice = new Array[RDD[(Int, (Iterable[(String, Double)], Iterable[(String, Double)]))]](nNumberOfTensor - 1)

    for(tensorIdx<-0 until nNumberOfTensor) {
      for (modeIdx <- 0 until pnTensorMode(tensorIdx)) {
        if(isCoupled(tensorIdx, modeIdx)){
          val (coupledTensorIdx, coupledModeIdx) = getCoupledTensorMode(tensorIdx, modeIdx)

          if(tensorIdx<coupledTensorIdx){//join
          val coupledSliceIdx = getCoupleIdx(tensorIdx, modeIdx)
            coupledSlice(coupledSliceIdx) = tensorSlice(tensorIdx)(modeIdx).join(tensorSlice(coupledTensorIdx)(coupledModeIdx)).persist(inputRDDStorageLevel)

            //uncache non-used tensor slices
            tensorSlice(tensorIdx)(modeIdx).unpersist()
            tensorSlice(coupledTensorIdx)(coupledModeIdx).unpersist()
          }
          else{
            //already joint => do nothing
          }

        }
      }
    }
    /**for caching coupledSlice**/
    coupledSlice.foreach(f=>f.count())

    coupledSlice
  }

  /** ******************************************************************************************
    * step2B_ComputeLoss
    * @return
    * ******************************************************************************************/
  override def step2B_ComputeLoss(): Array[Double] ={
    val dTrainMSE = new Array[Double](nNumberOfTensor + 1)

    for(tensorIdx<-0 until nNumberOfTensor) {
      dTrainMSE(tensorIdx) = computeTrainingLoss(tensorIdx)
    }

    var isComputeFactorLoss = false
    var coupledTensorIdx = 0
    var tensorIdx = 0
    var modeIdx = 0


    for(tIdx<-0 until nNumberOfTensor) {
      for (mIdx <- 0 until pnTensorMode(tensorIdx)) {
        if (!isCoupled(tIdx, mIdx) && (!isComputeFactorLoss)) {
          isComputeFactorLoss = true
          tensorIdx = tIdx
          modeIdx = mIdx

          if(tIdx == 0)
            coupledTensorIdx = 1
        }
      }
    }

    dTrainMSE(nNumberOfTensor) = meanLoss(tensorIdx, modeIdx, coupledTensorIdx, modeIdx)

    dTrainMSE
  }

  /** ******************************************************************************************
    * meanLoss
    *
    * @param tensorIdx
    * @param modeIdx
    * @param coupledTensorIdx
    * @param coupledModeIdx
    * @return
    * ******************************************************************************************/
  protected def meanLoss(tensorIdx: Int,
                         modeIdx: Int,
                         coupledTensorIdx: Int,
                         coupledModeIdx: Int): Double={
    val mean1 = factors(tensorIdx)(modeIdx).getColumnMean()
    val mean2 = factors(coupledTensorIdx)(coupledModeIdx).getColumnMean()

    var SE = 0.toDouble

    for(r<-0 until rank){
      val diff = mean1(r) - mean2(r)

      SE += diff * diff
    }

    if(sGradientName == "WLS")
      SE /= (rank)

    //Return
    SE
  }
}