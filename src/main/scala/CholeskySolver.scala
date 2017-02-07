/** Cholesky solver for least square problems. */
class CholeskySolver{
  /** ******************************************************************************************
    * Solves a least squares problem with L2 regularization:
    *
    *   min norm(A x - b)^2^ + lambda * norm(x)^2^
    *
    * @param ne a [[NormalEquation]] instance that contains AtA, Atb, and n (number of instances)
    * @param lambda regularization constant
    * @return the solution x
    * ******************************************************************************************/
  def solve(ne: NormalEquation, lambda: Double): Array[Double] = {
    val k = ne.k
    // Add scaled lambda to the diagonals of AtA.
    var i = 0
    var j = 2
    while (i < ne.triK) {
      ne.ata(i) += lambda
      i += j
      j += 1
    }
    CholeskyDecomposition.solve(ne.ata, ne.atb)

    val x = new Array[Double](k)
    i = 0
    while (i < k) {
      x(i) = ne.atb(i)
      i += 1
    }
    ne.reset()
    x
  }
}


