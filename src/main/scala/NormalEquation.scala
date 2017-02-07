import com.github.fommil.netlib.BLAS.{getInstance => blas}
import java.{util => ju}

/** ******************************************************************************************
  * Representing a normal equation to solve the following weighted least squares problem:
  *
  * minimize \sum,,i,, c,,i,, (a,,i,,^T^ x - b,,i,,)^2^ + lambda * x^T^ x.
  *
  * Its normal equation is given by
  *
  * \sum,,i,, c,,i,, (a,,i,, a,,i,,^T^ x - b,,i,, a,,i,,) + lambda * x = 0.
  * ******************************************************************************************/
class NormalEquation(val k: Int) extends Serializable {
  /** Number of entries in the upper triangular part of a k-by-k matrix. */
  val triK = k * (k + 1) / 2
  /** A^T^ * A */
  val ata = new Array[Double](triK)
  /** A^T^ * b */
  val atb = new Array[Double](k)

  private val da = new Array[Double](k)
  private val upper = "U"

  private def copyToDouble(a: Array[Double]): Unit = {
    var i = 0
    while (i < k) {
      da(i) = a(i)
      i += 1
    }
  }

  /** ******************************************************************************************
    * Adds an observation.
    * @param a
    * @param b
    * @param c
    * @return
    * ******************************************************************************************/
  def add(a: Array[Double], b: Double, c: Double = 1.0): this.type = {
    require(c >= 0.0)
    require(a.length == k)
    copyToDouble(a)
    blas.dspr(upper, k, c, da, 1, ata)
    if (b != 0.0) {
      blas.daxpy(k, c * b, da, 1, atb, 1)
    }
    this
  }

  /** ******************************************************************************************
    * Merges another normal equation object.
    * @param other
    * @return
    * ******************************************************************************************/
  def merge(other: NormalEquation): this.type = {
    require(other.k == k)
    blas.daxpy(ata.length, 1.0, other.ata, 1, ata, 1)
    blas.daxpy(atb.length, 1.0, other.atb, 1, atb, 1)
    this
  }

  /** ******************************************************************************************
    * Resets everything to zero, which should be called after each solve.
    * ******************************************************************************************/
  def reset(): Unit = {
    ju.Arrays.fill(ata, 0.0)
    ju.Arrays.fill(atb, 0.0)
  }
}