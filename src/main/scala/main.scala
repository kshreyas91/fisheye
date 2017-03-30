/**
  * Created by yanghj on 3/30/17.
  */
import Neo4jAsyncDriver.Neo4jAsyncDriver

object main {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    var driver = new Neo4jAsyncDriver
    var res = driver.run("CREATE (a:Person {name: {name}, title: {title}})", Map(("name", "lalala"), ("title", "hehe")))
    res = driver.run( "MATCH (a:Person) WHERE a.name = {name} RETURN a.name AS name, a.title AS title", Map(("name", "lalala")))
    println(res)
  }
}
