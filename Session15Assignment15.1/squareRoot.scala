object squareRoot

{
/*Babylonian Method to calculate the square root */
 
 def main(args: Array[String]): Unit = 
  {

    println("Enter the Number for square root")

    val n = scala.io.StdIn.readFloat()

    var x = n
    var y =1.toFloat
    val accuracy = 0.000001    /*accuracy for accuracy level*/

    while(x-y > accuracy)
    {
      x = (x+y)/2
      y= n/x
    }
    println("Square Root :"+x)

  }
}
