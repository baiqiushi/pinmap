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
      // 1 Parse query JSON
      val offset: Int = (request \ "offset").as[BigDecimal].intValue()
      val limit: Int  = (request \ "limit").as[BigDecimal].intValue()
      val keyword: String = (request \ "keyword").as[String]

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
      val resultSet: ResultSet = preparedStatement.executeQuery()
      var qJsonArray: JsArray = Json.arr()
      while (resultSet.next) {
        var rsJson: JsObject = Json.obj()
        val x = resultSet.getBigDecimal(xColName)
        val y = resultSet.getBigDecimal(yColName)
        val id = resultSet.getBigDecimal(idColName)
        rsJson = rsJson ++ Json.obj(xColName -> JsNumber(x))
        rsJson = rsJson ++ Json.obj(yColName -> JsNumber(y))
        rsJson = rsJson ++ Json.obj(idColName -> JsNumber(id))
        qJsonArray = qJsonArray :+ rsJson
      }

      //System.out.println("DBConnector ==> ")
      //System.out.println(qJsonArray)

      //out ! JsObject(Seq("OK" -> JsString(s"Get your request!")))
      out ! Json.toJson(qJsonArray)
  }
}

object DBConnector {
  def props(out :ActorRef) = Props(new DBConnector(out))
}