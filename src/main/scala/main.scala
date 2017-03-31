/**
  * Created by yanghj on 3/30/17.
  */
import Neo4jConnector.Neo4JConnector
import scala.concurrent._

object main {
  def main(args: Array[String]): Unit = {
    val driver = new Neo4JConnector
    driver.drop()
    driver.init()
    driver.addNode("testNode", Array(
      Map("tag" -> "Name",
          "column" -> "first_name",
          "confidence" -> "0.5"),
      Map("tag" -> "Date",
          "column" -> "1993-1-11",
          "confidence" -> "0.8")))
    println("Hello, world!")
  }
}
