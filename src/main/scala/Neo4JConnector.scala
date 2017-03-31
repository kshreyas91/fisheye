/**
  * Created by yanghj on 3/30/17.
  */

package Neo4jConnector

import scala.concurrent.duration._
import Neo4jAsyncDriver.Neo4jAsyncDriver
import org.neo4j.driver.v1._
import scala.concurrent._
import ExecutionContext.Implicits.global

sealed trait FieldType
case object PhoneNumber extends FieldType
case object VehiclePlate extends FieldType
case object Name extends FieldType
case object Unknown extends FieldType
case object CreditCardNumber extends FieldType
case object IpNumber extends FieldType
case object Date extends FieldType
case object StreetAddress extends FieldType
case object Email extends FieldType

case class Concept(name: String)
case class FieldName(field: FieldType)

class Neo4JConnector {
  val classes = Array(
    "PhoneNumber",
    "VehiclePlate",
    "Name",
    "CreditCardNumber",
    "IpNumber",
    "Date",
    "StreetAddress",
    "Email"
  )
  val driver = new Neo4jAsyncDriver

  def drop(): List[Record] = {
    return Await.result(driver.run("MATCH (n) DETACH DELETE n"), 10.second)
  }

  def init(): List[Record] = {
    Await.result(driver.run("CREATE (n:Concept {name: \"PII\"})"), 10.second)
    return Await.result(driver.run(
      """
        | MATCH (n:Concept {name:"PII"})
        | FOREACH (name in {classes} |
        | CREATE (n)-[:include]->(:Concept {name:name}))
      """.stripMargin, Map(
        "classes" -> classes
      )), 10 second)
  }

  def addNode(node: String, tags: Array[(String, String)]): Unit = {
    for (pair <- tags) {
      Await.result(driver.run(
        """
          | MATCH (n:Concept)
          | WHERE n.name={tag}
          | CREATE (n)-[:has {column:{column}}]->(:Table {title:{node}})
        """.stripMargin, Map(
          "tag" -> pair._1,
          "column" -> pair._2,
          "node" -> node
        )), 10.second)
    }
  }
}
