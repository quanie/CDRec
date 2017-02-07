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

class Gradient extends Serializable {
  /** *************************************************************************************************
    * Function computeGradient: computes gradient
    *             gradient = -2 * weight * (y-ax) * a
    *
    * @param y    label for this data point
    * @param a    features for one data point
    * @param x    features to be computed
    * @param weight   weights/coefficients corresponding to label
    * @return gradient: Double
    * *************************************************************************************************/
  def compute(y: Double,
              a: Array[Double],
              x: Array[Double],
              weight: Double = 1.0): Array[Double] ={
    val diff = dot(a, x) - y
    val scaleFactor = 2.0 * weight * diff
    val gradient = scal(scaleFactor, a)
    gradient
  }

  /** *************************************************************************************************
    * Function: dot
    *       dot product
    * @param x
    * @param y
    * @return
    * *************************************************************************************************/
  def dot(x: Array[Double], y: Array[Double]): Double ={
    require(x.length == y.length)
    var dResult:Double = 0

    for(i<-x.indices)
      dResult += x(i) * y(i)

    dResult
  }

  /** *************************************************************************************************
    * Function: scal
    *
    * @param a
    * @param y
    * @return
    * *************************************************************************************************/
  def scal(a: Double, y: Array[Double]): Array[Double] ={
    val rArray = new Array[Double](y.length)

    for(i<-y.indices){
      rArray(i) = y(i) * a
    }
    /*************************************************
      * if don't use rArray and just use this code
      *     yArray(i) *= a
      *     Vectors.dense(yArray)
      * then the updated values are assigned to y!!!
      * this is strange!!!
      ************************************************/
    rArray
  }
}