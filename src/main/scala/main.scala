/**
  * Created by yanghj on 3/30/17.
  */
import Neo4jConnector.Neo4JConnector
import scala.concurrent._
import ExecutionContext.Implicits.global

object main {
  def main(args: Array[String]): Unit = {
    val driver = new Neo4JConnector
    driver.drop()
    driver.init()
    driver.addNode("testNode", Array(("Name", "first_name"), ("Date", "1993-1-11")))
    println("Hello, world!")
  }
}
