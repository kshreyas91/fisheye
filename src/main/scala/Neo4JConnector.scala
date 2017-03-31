/**
  * Created by yanghj on 3/30/17.
  */

package Neo4jConnector

import Neo4jAsyncDriver.Neo4jAsyncDriver
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

  def drop(): Unit = {
    driver.run("MATCH (n) DETACH DELETE n")
  }

  def init(): Unit = {
    var r = driver.run("CREATE (n:Concept {name: \"PII\"})")
    r onComplete { case _ =>
      driver.run(
        """
          | MATCH (n:Concept {name:"PII"})
          | FOREACH (name in {classes} |
          | CREATE (n)-[:include]->(:Concept {name:name}))
        """.stripMargin, Map(
          "classes" -> classes
        ))
    }
  }

  def addNode(node: String, tags: Array[String]): Unit = {
    driver.run(
      """
        | FOREACH (tag in {tags} |
        | MATCH (n:Concept {name:tag})
        | CREATE (n)-[:has]->(:Table {title:node}))
      """.stripMargin, Map(
        "tags" -> tags,
        "node" -> node
      )
    )
  }
}
