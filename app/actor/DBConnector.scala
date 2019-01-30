package actor

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import play.api.Logger
import play.api.libs.json._

class DBConnector (val out: ActorRef) extends Actor with ActorLogging {
  private val logger = Logger("client")

  val driver: String = "org.postgresql.Driver"
  val url: String = "jdbc:postgresql://localhost:5432/pinmap"
  val username: String = "postgres"
  val password: String = "pinmap"
  val sqlTemplate: String =
    s"""
       |select x, y, id
       |  from tweets
       | where to_tsvector('english',text)@@to_tsquery('english',?)
       | offset ?
       | limit ?
     """.stripMargin
  val xColName: String = "x"
  val yColName: String = "y"
  val idColName: String = "id"

  override def receive: Receive = {
    case request : JsValue =>
      (request \ "cmd").asOpt[String] match {
        // Command
        case Some(command) =>
          command match {
            case "startDB" =>
              DBConnector.startDB()
            case "stopDB" =>
              DBConnector.stopDB()
          }
          out ! Json.toJson(Json.obj("cmd" -> command) ++ Json.obj("status" -> "ok"))
        // Query
        case None =>
          val tRequestReceived: Long = System.currentTimeMillis
          // 1 Parse query JSON
          val offset: Int = (request \ "offset").as[BigDecimal].intValue()
          val limit: Int = (request \ "limit").as[BigDecimal].intValue()
          val keyword: String = (request \ "keyword").as[String]

          val tRequestParsed: Long = System.currentTimeMillis

          /*System.out.println("DBConnector <== " + request.toString())
          System.out.println("offset: " + offset)
          System.out.println("size: " + limit)
          System.out.println("keyword: " + keyword)*/

          // 2 Query DB with query arguments
          Class.forName(driver)
          val connection: Connection = DriverManager.getConnection(url, username, password)
          val preparedStatement: PreparedStatement = connection.prepareStatement(sqlTemplate)
          preparedStatement.setString(1, keyword)
          preparedStatement.setInt(2, offset)
          preparedStatement.setInt(3, limit)
          //System.out.println("DBConnector ==> SQL")
          //System.out.println(preparedStatement)

          val tDBConnectionEstablished: Long = System.currentTimeMillis

          val resultSet: ResultSet = preparedStatement.executeQuery()

          val tQueryResultReceived: Long = System.currentTimeMillis

          System.out.println("DB results returned, time: " + (tQueryResultReceived - tDBConnectionEstablished)/1000.0 + "s")

          var qJsonArray: JsArray = Json.arr()
          //var i = 0
          while (resultSet.next) {
            //System.out.println("looping resultset :" + i)
            //i += 1
            var rsJson: JsObject = Json.obj()
            val x = resultSet.getDouble(xColName)
            val y = resultSet.getDouble(yColName)
            val id = resultSet.getBigDecimal(idColName)
            rsJson = rsJson ++ Json.obj(xColName -> JsNumber(BigDecimal.valueOf(x)))
            rsJson = rsJson ++ Json.obj(yColName -> JsNumber(BigDecimal.valueOf(y)))
            rsJson = rsJson ++ Json.obj(idColName -> JsNumber(id))
            qJsonArray = qJsonArray :+ rsJson
          }

          //System.out.println("DBConnector ==> ")
          //System.out.println(qJsonArray.value.length)

          //out ! JsObject(Seq("OK" -> JsString(s"Get your request!")))

          val tQueryResultParsed: Long = System.currentTimeMillis

          val responseJson: JsObject = Json.obj("data" -> qJsonArray,
            "tReqR" -> JsNumber(tRequestReceived),
            "tReqP" -> JsNumber(tRequestParsed),
            "tDBCE" -> JsNumber(tDBConnectionEstablished),
            "tQResR" -> JsNumber(tQueryResultReceived),
            "tQResP" -> JsNumber(tQueryResultParsed),
            "tQResS" -> JsNumber(System.currentTimeMillis)
          )

          out ! Json.toJson(responseJson)
      }
  }

  override def preStart(): Unit = {
    System.out.println("DBConnector starting ...")
  }
}

object DBConnector {
  def props(out :ActorRef) = Props(new DBConnector(out))

  def startDB(): Unit = {
    System.out.println("Starting DB ...")
  }

  def stopDB(): Unit = {
    System.out.println("Stopping DB ...")
    //val result = "sudo -u postgres pg_ctl -D /Library/PostgreSQL/9.6/data stop" !
    //println(result)
  }
}