/**
  * Created by yanghj on 3/30/17.
  */
import Neo4jConnector.Neo4JConnector

object main {
  def main(args: Array[String]): Unit = {
    val driver = new Neo4JConnector
    driver.init()
    println("Hello, world!")
  }
}
