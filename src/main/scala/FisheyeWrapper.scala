/**
  * Created by shreyaskulkarni on 3/30/17.
  */


import com.typesafe.scalalogging.{LazyLogging, Logger}
import io.circe.{HCursor, Json, JsonObject, parser}
import io.circe.syntax._

import scalaj.http.{Http, HttpOptions, HttpResponse}
import java.net.{SocketTimeoutException, UnknownHostException}
import javax.net.ssl.SSLHandshakeException

import Neo4jConnector.Neo4JConnector

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable

// case classes
case class metaDataParams(name:String,id:String,columns:Vector[String],permalink:String)
case class PredictionResults(confidence:String,tag:String, column: String)
case class PredictionDataParams(name:String,id:String,inference: Vector[PredictionResults],permalink:String)
// case class for neo4j


// JSON object singleton


object FisheyeWrapper extends LazyLogging with JsonWorkHorse {

  /*
  @params datasource
   */

  private val LIMIT = 10
  private val READ_TIMEOUT = 50000
  private val CONN_TIMEOUT = 10000
  private val token = "GPGuyRELzwEXtRJbJDib89U59"
  private val DATALOGUE_URL = "http://pii-detector-dev.us-east-1.elasticbeanstalk.com/batch-label"
  private val maxOffset = 9000 //something happened with Socrata, can only get 10K metadata objects.
  private val limit = 1000
  private var offset = 0


  def main(args:Array[String]): Unit = {
    args.length match {
      case 0 => println("No data source defined")
      case 1 => buildVisualGraph(args(0))
      case _ => println("Multiple arguments were detected. Please rerun application datasource as an argument")
    }
  }

  def buildVisualGraph(datasource:String):Unit = {
    if(datasource == "socrata"){

      val metadataParams : Vector[metaDataParams]= getAllMetaDataParams().take(100)
      val predParams: Vector[PredictionDataParams] = metadataParams.map{ m =>
        val predVec = getTagList(m.permalink, m.columns).toVector
        //println("Prediction Vector:" + predVec)
        PredictionDataParams(m.name,m.id,predVec,m.permalink)
      }
      buildNeo4jGraph(predParams)

      //println(predParams) //DELETE THIS


      }
    else{
      println("Unidentified Data Source. Try again with valid data source")
    }


  }

  def buildNeo4jGraph(predParams: Vector[PredictionDataParams]): Unit ={
    val driver = new Neo4JConnector
    driver.drop()
    driver.init()
    for ( pred <- predParams){
      val predParamsMaps = pred.inference.map(i => getCCParams(i)).toArray
      driver.addNode(pred.name, predParamsMaps)
    }

  }

//  def createMapfromCC(predParam : PredictionResults): Unit ={
//    val newMapp =
//    predParam.getClass().getDeclaredFields().foreach(println)
//
//  }

  def getCCParams(cc: PredictionResults) =
    (Map[String, String]() /: cc.getClass.getDeclaredFields) {(a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(cc).asInstanceOf[String])
    }

  def getTagList(permalink:String, colVec: Vector[String]): List[PredictionResults] ={
    var predictedTags = ListBuffer[PredictionResults]()
    //val permalink : String = //"https://data.cityofboston.gov/d/msk6-43c6"
    //for
    //val columnName : String = "zip"
    val requestURL : String = getRequestURL(permalink, LIMIT)
    val data = fetchData(requestURL)


    //      toJson(data).asObject.get.apply("zip").get.asArray
    val parsedData = parser.parse(data.getOrElse("")).getOrElse(Json.Null)
    if( parsedData != Json.Null){
      val arr = parsedData.asArray.getOrElse(Vector[Json]())
      val data = arr.flatMap(j => j.asObject)

      for ( c <- colVec){
        val objs = data.flatMap( o => o.apply(c).getOrElse(Json.Null).asString)
        objs.size match {
          case 0 => logger.info(s"No data for column:$c ")
          case _ =>
            val prediction : PredictionResults = getColumnPrediction(objs, c)
            predictedTags += prediction
        }
        }

    predictedTags.toList


    }
    else
    {
      logger.error("Parsing of the received JSON failed. Try again")
      predictedTags.toList
    }

  }

  def fetchData(requestURL :String): Option[String] ={

//    val dataResponse : HttpResponse[String] = Http(requestURL).option(HttpOptions.readTimeout(READ_TIMEOUT)).option(HttpOptions.connTimeout(CONN_TIMEOUT)).asString
//    val resultArray : Option[Vector[Json]] = parser.parse(dataResponse).fold(_ => ???, json => json).asObject.get.apply("zip").get.asArray
//    //val resultArray = toJson(dataResponse).asObject.get.apply("results").get.asArray
    logger.info(s"Making HTTP request to $requestURL")

    try{
      Thread.sleep(100)
      val resp = Http(requestURL).option(HttpOptions.readTimeout(READ_TIMEOUT)).option(HttpOptions.connTimeout(CONN_TIMEOUT)).asString
      logger.info(s"HTTP response code from $requestURL is :${resp.code}")
      resp.isNotError match {
        case true => Some(resp.body)
        case false => None
      }
    }
    catch {
      case uhe: UnknownHostException => None
      case ste: SocketTimeoutException => None
      case ssl: SSLHandshakeException => None
    }
  }


  def getRequestURL(permaLink: String,  limit : Int) : String = {
    val updatedLink = permaLink.split("/").toVector.updated(3,"resource").mkString("/")
    val requestURL : String = updatedLink + ".json?" +  "&$limit=" + limit
    requestURL
  }

  // refactor
  def sendRequest(off:Int):HttpResponse[String] =
    Http(s"http://api.us.socrata.com/api/catalog/v1?offset=${off}&limit=${limit}&only=datasets")
      .header("X-App-Token",token).asString

  def getMetaDataParams(body:String): Vector[metaDataParams] = {
    val meta: Vector[Json] = toJson(body).asObject.get.apply("results").get.asArray.getOrElse(Vector.empty[Json])
    val mdpVector : Vector[Option[metaDataParams]]  = meta.map{ j =>
      j.asObject match {
        case None => None
        case Some(obj) =>
          val resource = obj.apply("resource").get.asObject.get //TODO: replace get with getOrElse,safer...
        val name : String = resource.apply("name").getOrElse(Json.Null).asString.getOrElse("")
          val id : String = resource.apply("id").getOrElse(Json.Null).asString.getOrElse("")
          val cols : Vector[String] = resource.apply("columns_field_name").getOrElse(Json.Null).asArray.get.map( col => col.asString.getOrElse(""))
          val link: String = obj.apply("permalink").getOrElse(Json.Null).asString.getOrElse("")
          Some(metaDataParams(name,id,cols,link))
      }
    }
    mdpVector.flatten
  }

  def getAllMetaDataParams():Vector[metaDataParams]= {
    val lmdp = ListBuffer[Vector[metaDataParams]]()
    while (offset <= maxOffset) {
      val response = sendRequest(offset)
      lmdp += getMetaDataParams(response.body)
      offset += limit
    }
    lmdp.toVector.flatten // Vector[metadataParams]
  }

  def getColumnPrediction(columnVal: Vector[String], colName:String):PredictionResults = {

    val requestB : String = Map("data" -> columnVal.toList).asJson.noSpaces
    //logger.info(s"classifying column: $colName") //DEBUG
    //logger.info (s"Data:$requestB") //DEBUG
    val result:HttpResponse[String] = Http(DATALOGUE_URL).postData(requestB) // (s""""$requestBody""""
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(READ_TIMEOUT)).asString
    val parsedObject = toJson(result.body).asObject.get
    val tag = parsedObject.apply("tag").getOrElse(Json.Null).asString.getOrElse("")
    val score = parsedObject.apply("score").getOrElse(Json.Null).asString.getOrElse("")
    PredictionResults(score,tag, colName)

  }
}