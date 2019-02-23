package actor

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.joda.time.{DateTime, Interval}
import org.joda.time.format.DateTimeFormat
import play.api.Logger
import play.api.libs.json._

import scala.collection.mutable.ListBuffer

class DBConnector (val out: ActorRef) extends Actor with ActorLogging {
  private val logger = Logger("client")

  val dateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")

  val driver: String = "org.postgresql.Driver"
  val url: String = "jdbc:postgresql://localhost:5432/pinmap"
  val username: String = "postgres"
  val password: String = "pinmap"
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
        case _ =>

          val t1: Long = System.currentTimeMillis // t1 - request received

          // 1 Parse query JSON
          val mode: Option[String] = (request \ "mode").asOpt[String] // offset, interval
          val sliceInterval: Option[Int] = (request \ "sliceInterval").asOpt[Int] // offset, # of days
          val keyword: String = (request \ "keyword").as[String]
          val start: Option[DateTime] = (request \ "start").asOpt[String] match {
            case Some(s) => Some(dateTimeFormat.parseDateTime(s))
            case None => None
          }
          val end: Option[DateTime] = (request \ "end").asOpt[String] match {
            case Some(e) => Some(dateTimeFormat.parseDateTime(e))
            case _ => None
          }
          val offset: Option[Int] = (request \ "offset").asOpt[Int]
          val limit: Option[Int] = (request \ "limit").asOpt[Int]

          val t2: Long = System.currentTimeMillis // t2 - request parsed

          /*System.out.println("DBConnector <== " + request.toString())
          System.out.println("offset: " + offset)
          System.out.println("size: " + limit)
          System.out.println("keyword: " + keyword)*/

          // 2 Query DB with query arguments
          Class.forName(driver)
          val connection: Connection = DriverManager.getConnection(url, username, password)

          val sqlTemplate: String = genSQLTemplate(mode, keyword, start, end, offset, limit)

          val preparedStatement: PreparedStatement = connection.prepareStatement(sqlTemplate)

          var resultSet: ResultSet = null

          var done: Boolean = false
          var thisInterval: Option[Interval] = None
          var thisOffset: Option[Int] = offset

          do {

            val (isDone, nextInterval, nextOffset) = rewriteQuery(preparedStatement, keyword, mode,
              sliceInterval, start, end, thisInterval, thisOffset, limit)
            done = isDone
            thisInterval = nextInterval
            thisOffset = nextOffset

            val t3: Long = System.currentTimeMillis // t3 - db connected

            resultSet = preparedStatement.executeQuery()

            val t5: Long = System.currentTimeMillis // t5 - db result

            System.out.println("[DBConnector] DB done. T4 + T5 =  " + (t5 - t3) / 1000.0 + "s")

            //System.out.println("DBConnector ==> SQL")
            //System.out.println(preparedStatement)

            // Two ways to return result
            val data = (request \ "byArray").asOpt[Boolean] match {

              case Some(true) =>
                // Return result by array - all coordinates of all records in one array
                genDataByArray(resultSet)

              case _ =>
                // Return result by JSON
                genDataByJson(resultSet)
            }

            val t6 = System.currentTimeMillis()
            System.out.println("[DBConnector] JSON done. T6 = " + (t6 - t5) / 1000.0 + "s")

            val responseJson: JsObject = Json.obj("data" -> data,
              "t1" -> JsNumber(t1),
              "T2" -> JsNumber(t2 - t1),
              "T3" -> JsNumber(t3 - t2),
              "T45" -> JsNumber(t5 - t3),
              "T6" -> JsNumber(t6 - t5),
              "t6" -> JsNumber(t6)
            )
            //System.out.println(responseJson)
            out ! Json.toJson(responseJson)

          } while (!done)
      }
  }

  private def genSQLTemplate(mode: Option[String], keyword: String, start: Option[DateTime], end: Option[DateTime],
                             offset: Option[Int], limit: Option[Int]) : String = {
    var sqlTemplate: String =
      s"""
         |select x, y, id
         |  from tweets
         | where to_tsvector('english',text)@@to_tsquery('english',?)
     """.stripMargin

    mode match {
      case Some(sliceMode) =>
        //TODO - if slicing mode, concatenate the sql template string
        ???
      case None =>
        if (!start.isEmpty)
          sqlTemplate += " and create_at >= ?"
        if (!end.isEmpty)
          sqlTemplate +=" and create_at < ?"
        if (!offset.isEmpty)
          sqlTemplate += " offset ?"
        if (!limit.isEmpty)
          sqlTemplate += " limit ?"
    }

    sqlTemplate
  }

  private def rewriteQuery(preparedStatement: PreparedStatement, keyword: String, mode: Option[String],
                           sliceInterval: Option[Int], start: Option[DateTime], end: Option[DateTime],
                           thisInterval: Option[Interval], thisOffset: Option[Int],
                           limit: Option[Int]): (Boolean, Option[Interval], Option[Int]) = {
    mode match {
      case Some(sliceMode) =>
        //TODO - if slicing mode, determine the nextInterval or nextOffset
        ???
      case None => // no slice
        var pIndex = 1
        preparedStatement.setString(pIndex, keyword)
        pIndex += 1
        if (!start.isEmpty) {
          preparedStatement.setString(pIndex, dateTimeFormat.print(start.get))
          pIndex += 1
        }
        if (!end.isEmpty) {
          preparedStatement.setString(pIndex, dateTimeFormat.print(end.get))
          pIndex += 1
        }
        if (!thisOffset.isEmpty) {
          preparedStatement.setInt(pIndex, thisOffset.get)
          pIndex += 1
        }
        if (!limit.isEmpty) {
          preparedStatement.setInt(pIndex, limit.get)
          pIndex += 1
        }

        (true, thisInterval, thisOffset)
    }
  }

  private def genDataByArray(resultSet: ResultSet): JsObject = {

    var T6_1 = 0.0
    var T6_2 = 0.0

    val coordinates: ListBuffer[Array[Double]] = new ListBuffer[Array[Double]]
    val ids: ListBuffer[Long] = new ListBuffer[Long]
    while (resultSet.next) {

      val t6_0 = System.currentTimeMillis // t6_0 - before get column

      val x = resultSet.getDouble(xColName)
      val y = resultSet.getDouble(yColName)
      val id = resultSet.getLong(idColName)

      val t6_1 = System.currentTimeMillis // t6_1 - column value got

      coordinates.append(Array(x, y))
      ids.append(id)

      val t6_2 = System.currentTimeMillis // t6_2 - json object created

      T6_1 += t6_1 - t6_0
      T6_2 += t6_2 - t6_1
    }

    val t6_1 = System.currentTimeMillis // t6_1 - before json create

    val data: JsObject = Json.obj("length" -> coordinates.length,
      "coordinates" -> coordinates,
      "ids" -> ids,
      "byArray" -> true
    )

    val t6_2 = System.currentTimeMillis // t6_2 - json object created

    T6_2 += t6_2 - t6_1

    System.out.println("[DBConnector] In T6, get value  T6_1 = " + T6_1/1000.0 + "s")
    System.out.println("[DBConnector] In T6, build json T6_2 = " + T6_2/1000.0 + "s")

    data
  }

  private def genDataByJson(resultSet: ResultSet): JsArray = {

    var resultJsonArray: JsArray = Json.arr()

    var T6_1 = 0.0
    var T6_2 = 0.0
    while (resultSet.next) {

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

    System.out.println("[DBConnector] In T6, get value  T6_1 = " + T6_1/1000.0 + "s")
    System.out.println("[DBConnector] In T6, build json T6_2 = " + T6_2/1000.0 + "s")
    //System.out.println(resultJsonArray.value.length)

    resultJsonArray
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