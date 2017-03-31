/**
  * Created by yanghj on 3/30/17.
  */

package Neo4jConnector

import scalaz.Scalaz._
import scala.concurrent.duration._
import Neo4jAsyncDriver.Neo4jAsyncDriver
import org.neo4j.driver.v1._
import scala.concurrent._

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
    Await.result(driver.run("MATCH (n) DETACH DELETE n"), 10.second)
  }

  def init(): List[Record] = {
    Await.result(driver.run("CREATE (n:Concept {name: \"PII\"})"), 10.second)
    Await.result(driver.run("CREATE (n:Concept {name: \"None-PII\"})"), 10.second)
    Await.result(driver.run(
      """
        | MATCH (n:Concept {name:"None-PII"})
        | CREATE (n)-[:include]->(:Concept {name:"Unknown"})
      """.stripMargin), 10.second)
    Await.result(driver.run(
      """
        | MATCH (n:Concept {name:"PII"})
        | FOREACH (name in {classes} |
        | CREATE (n)-[:include]->(:Concept {name:name}))
      """.stripMargin, Map(
        "classes" -> classes
      )), 10.second)
  }

  /**
    * add a node in neo4j
    *
    * @param title node title
    * @param connections maps of properties, it should have following fields
    *                    tag: the classifier result
    *                    column: the column name of this connection
    *                    confidence: the confidence score of this connection
    */
  def addNode(title: String, connections: Array[Map[String, String]]): Unit = {
    val titleMap = Map("title" -> title)
    Await.result(driver.run("CREATE(t:TABLE {title:{title}})", Map(
      "title" -> title
    )), 10.second)
    for (connection <- connections) {
      driver.run(
        """
          | MATCH (n:Concept), (t:TABLE)
          | WHERE n.name={tag} AND t.title={title}
          | CREATE (t)-[:has {column:{column}, confidence:{confidence}}]->(n)
        """.stripMargin, titleMap |+| connection)
    }
  }
}
