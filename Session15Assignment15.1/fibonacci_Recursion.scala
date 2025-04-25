object fibonacci_recursion

{
  /*fibonacci sequence by recursion method*/
  
  def main(args: Array[String]): Unit =
  {
    println("Enter the number:")

    val n: Int = scala.io.StdIn.readLine().toInt

    println("Fibonacci Series of " + n + " numbers:" + fib1(n))

    def fib1(n: Int): Int = {
      if (n <= 2) {
        n
      }

      else {
        fib1(n - 1) + fib1(n - 2)
      }
    }
	println(fib1(n))
  }
}
