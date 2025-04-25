object fibonacci_recursion
{
  /*fibonacci sequence by forloop method*/
  
  def main(args: Array[String])
  {
    var num1 = 1
    var num2 = 2

    println("Enter the number:")

    val count: Int = scala.io.StdIn.readLine().toInt

    println("Fibonacci Series of " + count + " numbers:")

    for (a <- 1 to count)
    {
      println(num1 + " ")

      val  sumOfPrevTwo = num1 + num2
      num1 = num2
      num2 = sumOfPrevTwo
  }
  }
}