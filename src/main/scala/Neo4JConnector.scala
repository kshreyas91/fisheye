/**
  * Created by yanghj on 3/30/17.
  */

sealed trait FieldType
case object Phone_number extends FieldType
case object Vehicle_plate extends FieldType
case object Name extends FieldType
case object Unknown extends FieldType
case object Credit_card_number extends FieldType
case object Ip_number
case object Date extends FieldType
case object Street_addresse extends FieldType
case object Email extends FieldType

case class Concept(name: String)
case class FieldName(field: FieldType)

class Neo4JConnector {

}
