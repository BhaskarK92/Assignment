object ObjectRationalCal {

  def main(args: Array[String]): Unit = {

    val r1 = new Calculator(9,5)
    val r2 = new Calculator(7,3)

    val sumR1R2 = r1.add(r2)
    val subR1R2 = r1.subtract(r2)
    val mulR1R2 = r1.multiply(r2)
    val divR1R2 = r1.division(r2)
    val gcdR1R2 = r1.gcd_Rational(r2)


    println(sumR1R2)
    println(subR1R2)
    println(mulR1R2)
    println(divR1R2)

    println(gcdR1R2)

  }

}
