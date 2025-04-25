object GCD
{
  def main(args: Array[String])
  {
    def hcf(a:Int,b:Int):Int=
    {
      if(b==0)
        return  a
      else
        hcf(b,a%b)

    }


    println(hcf(120,150))

  }
}