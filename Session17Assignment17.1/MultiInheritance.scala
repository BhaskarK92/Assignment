class MultiInheritance {
  def a() {
    println("version1")
  }
}
class inheritance extends MultiInheritance {
  def b()
  {
    println("version2")
  }
}
class inheritance2 extends inheritance {
  def c()
  {
    println("version1 by multiple inheritance")
    println("version2 by multiple inheritance")
  }
}
object inherit{
  def main(args: Array[String])
  {
    val obj = new inheritance2
    obj.a
    obj.b
    obj.c
  }
}
