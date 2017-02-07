import java.io.{File, FileWriter}

/** ******************************************************************************************
  * Name: Factor
  * Created by dudo on 15/04/16.
  * ********************************************************************************************/
class Factor(private val nLength: Int,
             private val nRank: Int)
  extends Serializable {

  protected var pnLength: Array[Int] = null
  protected val nMode: Int = 2

  //Init Factor
  pnLength = new Array[Int](nMode)
  pnLength(0) = nLength
  pnLength(1) = nRank

  private val data = new Array[Array[Double]](nLength)
  for(i<-0 until nLength)
    data(i) = new Array[Double](nRank)


  def getMode: Int = nMode

  def getLength(modeIdx: Int): Int = {
    require(modeIdx < nMode)
    pnLength(modeIdx)
  }
  /** ******************************************************************************************
    * Name: getRank and getLength
    * ********************************************************************************************/
  def getRank: Int = nRank
  def getLength: Int = nLength

  /** ******************************************************************************************
    * Name: set
    * Function: set an entry of the factor
    * Return:  Unit
    * ********************************************************************************************/
  def set(index: Array[Int], value: Double) {
    require(index.length == nMode)
    data(index(0))(index(1)) = value
  }

  def set(i: Int, j: Int, value: Double) {
    val index: Array[Int] = Array(i, j)
    set(index, value)
  }
  /** ******************************************************************************************
    * Name: setRow
    * Function: set a row of the factor
    *
    * @param rowIdx index of the row to be set
    * @param value value to be set
    *
    * Return: Unit
    * ********************************************************************************************/
  def setRow(rowIdx: Int, value: Array[Double]) {
    require(rowIdx < nLength)
    require(value.length == nRank)
    data(rowIdx) = value.clone()
  }

  /** ******************************************************************************************
    * Name: get
    * Function: return an entry of the factor
    * Return:  Double
    * ********************************************************************************************/
  def get(index: Array[Int]): Double = {
    require(index.length == nMode)
    val rowIdx = index(0)
    val rankIdx = index(1)

    if(rowIdx >= nLength)
      println("\t Error - rowIdx: " + rowIdx)
    if(rankIdx >= nRank)
      println("\t Error - rankIdx: " + rankIdx)

    data(rowIdx)(rankIdx)
  }

  def get(i: Int, j: Int): Double = {
    val index: Array[Int] = Array(i, j)
    get(index)
  }
  /** ******************************************************************************************
    * Name: getRow
    * Function: return a row of the factor
    *
    * @param rowIdx index of the row to be returned
    *
    * Return:  Array[Double]
    * ********************************************************************************************/
  def getRow(rowIdx: Int): Array[Double] = {
    require(rowIdx < nLength)
    data(rowIdx).clone()
  }

  /** ******************************************************************************************
    * Name: getColumnMean
    * Function: return a mean of each column of the factor
    *
    * @param rankIdx: column Index
    *
    * Return:  Double
    * ********************************************************************************************/
  def getColumnMean(rankIdx: Int): Double = {
    var mean: Double = 0

    for (rowIdx<-0 until nLength)
      mean += data(rowIdx)(rankIdx)

    mean /= nLength
    mean
  }

  def getColumnMean(): Array[Double] = {
    val mean = new Array[Double](nRank)

    for(rankIdx<-0 until nRank){
      mean(rankIdx) = getColumnMean(rankIdx)
    }

    mean
  }

  /** ******************************************************************************************
    * Name: init
    * Function: initialize factor with value
    *
    * @param initType 0: predefined value// 1: random
    * @param initVal  predefined value
    *
    * Return:  Unit
    * ********************************************************************************************/
  def init(initType: Int, initVal: Double) {
    val randSeed = scala.util.Random

    for(i<-0 until nLength){
      for (r<-0 until nRank) {
        if (initType == 0) //Init with predefined value
          data(i)(r) = initVal
        else //Init with random value
          data(i)(r) = randSeed.nextDouble()
      }
    }
  }
  /** ******************************************************************************************
    * Name: initDiagonal
    * Function: initialize diagonal with value
    *
    * @param initVal  predefined value
    *
    * Return:  Unit
    * ********************************************************************************************/
  def initDiagonal(initVal: Double) {
    require(nLength == nRank)

    for(i<-0 until nLength){
      for (r<-0 until nRank) {
        if(i==r)
          data(i)(r) = initVal
        else
          data(i)(r) = 0
      }
    }
  }
  def initDiagonal(initVal: Double, kRow: Int) {
    require(nLength == nRank)
    val randSeed = scala.util.Random

    for(i<-0 until kRow){
      for (r<-0 until nRank) {
        if(i==r)
          data(i)(r) = initVal
        else
          data(i)(r) = 0
      }
    }
    for(i<-kRow until nLength){
      for (r<-0 until nRank) {
        data(i)(r) = randSeed.nextDouble()
      }
    }
  }
  /** ******************************************************************************************
    * Name: saveFactor
    * Function: save factor to file
    *
    * @param sOutputDirPath output dir path
    * @param sFileName  filename
    *
    * Return:  Unit
    * ********************************************************************************************/
  def saveFactors(sOutputDirPath: String, sFileName: String): Unit ={
    val fOutputDir = new File(sOutputDirPath)

    if (!fOutputDir.exists()) {
      fOutputDir.mkdirs()
    }

    val fOutput = new File(fOutputDir, sFileName)
    val fileWriterOutput = new FileWriter(fOutput)

    for(row<-0 until nLength){
      var strOutputToWrite: String =""
      for(col<-0 until nRank){
        if(col==0)
          strOutputToWrite += data(row)(col)
        else
          strOutputToWrite += "\t" + data(row)(col)
      }
      strOutputToWrite +="\n"
      //write output
      fileWriterOutput.append(strOutputToWrite)
      fileWriterOutput.flush()
    }
    fileWriterOutput.flush()
    fileWriterOutput.close()
  }
}