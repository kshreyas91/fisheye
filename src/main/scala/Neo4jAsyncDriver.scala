package Neo4jAsyncDriver

import java.util.concurrent.Executors

import com.typesafe.config.ConfigFactory
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.exceptions.TransientException
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Wraps all the Neo4j operations to make them asynchronous.
  */
class Neo4jAsyncDriver {

  val config = ConfigFactory.load()

  val syncDriver = GraphDatabase.driver("bolt://localhost:7687",
    AuthTokens.basic("neo4j", "demotest")
  )

  val log = LoggerFactory.getLogger(classOf[Neo4jAsyncDriver])

  // Thread pools that grows when more is needed, and that reuses idle threads
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  /**
    * Closes the driver session
    */
  def close() = syncDriver.close()

  /**
    * Runs a Cypher query in the Neo4j database
    *
    * @param cypher Cypher query to be run
    * @param params parameters in the cypher query
    * @return Future of List of Records containing the result of the query
    */
  def run(cypher: String, params: Map[String, Any] = Map.empty[String, Any]): Future[List[Record]] = Future {
    val session = syncDriver.session()
    try {
      val results = makeCypherRequest(session, cypher, params).list()
      (for (i <- 0 until results.size) yield results.get(i)).toList
    } finally {
      session.close()
    }
  }

  /**
    * Makes a call to the Neo4j and retries up to 10 times if the call fails for blocking reasons
    *
    * Will throw an exception after trying 'retry' amount of times
    *
    * @param session session in which the calls will be made
    * @param cypher cypher statement to be executed
    * @param params parameters for the cypher statment
    * @param retry number of retry left
    * @return
    */
  @tailrec
  private def makeCypherRequest(session: Session, cypher: String, params: Map[String, Any], retry: Int = 10)
  : StatementResult = {
    Try(session.run(cypher, paramsToValue(params))) match {
      case Success(res) => res
      case Failure(e: TransientException) if retry > 0 => makeCypherRequest(session, cypher, params, retry - 1)
      case Failure(e) => throw e
    }
  }

  /**
    * Runs a bunch of cypher statements as one transaction. If any one of the elements fails, the transaction rolls back
    *
    * @param cypherStatements Seqence of Cypher statements
    * @param params Sequence of parameters map. Each element of the sequence corresponds to a statement in the
    *               sequence of cypher statements
    * @return Future with a sequence of list of record. Each element in the sequence corresponds to the result of the
    *         cypher query at the same index as the result.
    */
  def runTransaction(cypherStatements: Seq[String], params: Seq[Map[String, Any]] = Seq.empty[Map[String, Any]])
  : Future[Seq[List[Record]]] = Future {

    val session = syncDriver.session()
    val tx = session.beginTransaction()
    val results = mutable.Seq.empty[List[Record]]

    for (i <- cypherStatements.indices){
      try {
        if (params.size > i) {
          results :+ tx.run(cypherStatements(i), paramsToValue(params(i))).list().toList
        } else {
          results :+ tx.run(cypherStatements(i)).list().toList
        }
      } catch {
        case e: AnyRef => tx.failure(); tx.close(); session.close(); throw e
      }
    }

    tx.success()
    tx.close()
    session.close()
    results
  }

  /**
    * Converts a Map of parameters to a Driver Value to be used by the driver.run internal call.
    *
    * @param parameters Map representing the parameters.
    * @return Value to be used with session.run.
    */
  private def paramsToValue(parameters: Map[String, Any]): Value = Values.value(
    toJavaMap(parameters.mapValues(elem => Values.value(elem)))
  )

  /**
    * When applied to a scala Map with import scala.collection.JavaConversions._, will convert to Java Map
    *
    * @param m map to be converted
    * @return Java Map
    */
  private def toJavaMap(m: java.util.Map[String, Value]): java.util.Map[String, Value] = m
}