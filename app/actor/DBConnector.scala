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

          val t1: Long = System.currentTimeMillis // t1 - request received

          // 1 Parse query JSON
          val offset: Int = (request \ "offset").as[BigDecimal].intValue()
          val limit: Int = (request \ "limit").as[BigDecimal].intValue()
          val keyword: String = (request \ "keyword").as[String]

          val t2: Long = System.currentTimeMillis // t2 - request parsed

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

          val t3: Long = System.currentTimeMillis // t3 - db connected

          val resultSet: ResultSet = preparedStatement.executeQuery()

          val t5: Long = System.currentTimeMillis // t5 - db result

          System.out.println("[DBConnector] DB done. T4 + T5 =  " + (t5 - t3)/1000.0 + "s")

          var resultJsonArray: JsArray = Json.arr()
          //var i = 0
          var T6_1 = 0.0
          var T6_2 = 0.0
          while (resultSet.next) {
            //System.out.println("looping resultset :" + i)
            //i += 1

            val t6_0 = System.currentTimeMillis // t6_0 - before get column

            val x = resultSet.getDouble(xColName)
            val y = resultSet.getDouble(yColName)
            val id = resultSet.getBigDecimal(idColName)

            val t6_1 = System.currentTimeMillis // t6_1 - column value got

            val recordJson = Json.obj(
              xColName -> BigDecimal.valueOf(x),
              yColName -> BigDecimal.valueOf(y),
              idColName -> JsNumber(id)
            )
            resultJsonArray = resultJsonArray :+ recordJson

            val t6_2 = System.currentTimeMillis // t6_2 - json object created

            T6_1 += t6_1 - t6_0
            T6_2 += t6_2 - t6_1
          }

          val t6: Long = System.currentTimeMillis // t6 - json built

          System.out.println("[DBConnector] JSON done. T6 = " + (t6 - t5)/1000.0 + "s")
          System.out.println("[DBConnector] In T6, get value  T6_1 = " + T6_1/1000.0 + "s")
          System.out.println("[DBConnector] In T6, build json T6_2 = " + T6_2/1000.0 + "s")
          //System.out.println(resultJsonArray.value.length)

          val responseJson: JsObject = Json.obj("data" -> resultJsonArray,
            "t1" -> JsNumber(t1),
            "T2" -> JsNumber(t2 - t1),
            "T3" -> JsNumber(t3 - t2),
            "T45" -> JsNumber(t5 - t3),
            "T6" -> JsNumber(t6 - t5),
            "t6" -> JsNumber(t6)
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