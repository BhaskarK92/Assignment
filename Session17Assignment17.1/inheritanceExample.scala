class inheritanceExample {
  def a() {
    println("version1")
  }
}
class inheritanceExample1 extends inheritanceExample {
  def b()
  {
    println("version2")
  }
}

object inherit{
  def main(args: Array[String])
  {
  val obj = new inheritanceExample1
    obj.a
    obj.b
  }
}
