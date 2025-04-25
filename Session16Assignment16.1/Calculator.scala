class Calculator (n :Int,d :Int)  {
  require(d!=0)  //for a rational number denominator should be natural number, i.e d should be natural number.

  //function to define GCD
  private def gcd(x: Int, y: Int): Int = {
    if (x == 0) y
    else if (x < 0) gcd(x, y)
    else if (y < 0) gcd(x, y)
    else gcd(y % x, x)
  }
 private val g = gcd(n.abs, d.abs)// to calculate absolute GCD of numerator and denominator

  val numerator : Int = n
  val denominator :Int = d

  override def toString = numerator +"/"+ denominator /*override the default implementation by adding a method toString
   to class calculator. This will help us to print out the values of numerator and denominator of Rational number*/
  def this(n:Int) = this(n,1)  //auxiliary constructor

  def add(that: Calculator): //function define to calculate add operation
  Calculator = new Calculator(numerator*that.denominator + that.numerator*denominator,denominator*that.denominator)
  def add(i:Int): Calculator = new Calculator(numerator + i*denominator,denominator) //add method overloading


  def subtract(that: Calculator): //function define to calculate subtract operation
  Calculator = new Calculator(numerator*that.denominator - that.numerator*denominator,denominator*that.denominator)
  def subtract(i:Int): Calculator = new Calculator(numerator - i*denominator,denominator)

  def multiply(that: Calculator): //function define to calculate multiplication  operation
  Calculator = new Calculator(numerator*that.numerator ,denominator*that.denominator)
  def multiply(i:Int): Calculator = new Calculator(numerator*i ,denominator)


  def division(that: Calculator): //function define to calculate division  operation
  Calculator = new Calculator(numerator*that.denominator ,denominator*that.numerator)
  def division(i:Int): Calculator = new Calculator(numerator ,denominator*i)


  def gcd_Rational(that: Calculator): //function define to calculate GCD operation for two rational numbers
  Calculator = new Calculator(gcd(numerator*that.denominator,denominator*that.numerator)/(denominator*that.denominator))
  def gcd_rational(i:Int) : Calculator = new Calculator(gcd(numerator/denominator,i))
}
