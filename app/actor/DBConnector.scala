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
  val datasetStart: DateTime = dateTimeFormat.parseDateTime("2017-01-24 00:00:00")
  val datasetEnd: DateTime = dateTimeFormat.parseDateTime("2017-09-09 00:00:00")
  val defaultSliceInterval: Int = 30

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
          val mode: Option[String] = (request \ "mode").asOpt[String] // offset, interval
          val sliceInterval: Option[Int] = (request \ "sliceInterval").asOpt[Int] // offset, # of days
          val excludes: Option[Boolean] = (request \ "excludes").asOpt[Boolean] // true, false

          val sqlTemplate: String = genSQLTemplate(keyword, start, end, offset, limit, mode, excludes)

          val t2: Long = System.currentTimeMillis // t2 - request parsed

          // 2 Establish connection to DB with prepared statement
          Class.forName(driver)
          val connection: Connection = DriverManager.getConnection(url, username, password)
          val preparedStatement: PreparedStatement = connection.prepareStatement(sqlTemplate)

          val t3: Long = System.currentTimeMillis // t3 - db connected

          // 3 Rewrite query for slicing queries and run the query
          var resultSet: ResultSet = null
          var done: Boolean = false
          var thisInterval: Option[Interval] = None
          var thisOffset: Option[Int] = Option(0)
          do {
            // Rewrite query
            val (isDone, nextInterval, nextOffset) = rewriteQuery(preparedStatement, keyword, start, end,
              offset, limit, mode, sliceInterval, thisInterval, thisOffset)
            done = isDone
            thisInterval = nextInterval
            thisOffset = nextOffset

            val t4: Long = System.currentTimeMillis // t4 - send db query

            resultSet = preparedStatement.executeQuery()

            val t5: Long = System.currentTimeMillis // t5 - db result

            println("[DBConnector] DB done. T4 + T5 =  " + (t5 - t4) / 1000.0 + "s")

            //println("DBConnector ==> SQL")
            //println(preparedStatement)
            //println()

            // Two ways to return result
            val (data, length) = (request \ "byArray").asOpt[Boolean] match {

              case Some(true) =>
                // Return result by array - all coordinates of all records in one array
                genDataByArray(resultSet)

              case _ =>
                // Return result by JSON
                genDataByJson(resultSet)
            }

            val t6 = System.currentTimeMillis()
            println("[DBConnector] JSON done. T6 = " + (t6 - t5) / 1000.0 + "s")

            val responseJson: JsObject = Json.obj("data" -> data,
              "t1" -> JsNumber(t1),
              "T2" -> JsNumber(t2 - t1),
              "T3" -> JsNumber(t3 - t2),
              "T45" -> JsNumber(t5 - t4),
              "T6" -> JsNumber(t6 - t5),
              "t6" -> JsNumber(t6)
            )
            out ! Json.toJson(responseJson)

            println("result length = " + length)

            if (!mode.isEmpty) {
              if(mode.get.toString == "offset" && length < sliceInterval.get.intValue) {
                done = true
              }
            }

          } while (!done)

          println("[DBConnector] ==> Query Done!")
      }
  }

  private def genSQLTemplate(keyword: String, start: Option[DateTime], end: Option[DateTime],
                             offset: Option[Int], limit: Option[Int], mode: Option[String],
                             excludes: Option[Boolean]) : String = {
    var sqlTemplate: String =
      s"""
         |select x, y, id
         |  from tweets
         | where to_tsvector('english',text)@@to_tsquery('english',?)
     """.stripMargin

    excludes match {
      case Some(exclude) =>
        if (exclude) {
          sqlTemplate += " and (width_bucket(x, -173.847656, -65.390625, 1920), " +
            "width_bucket(y, 17.644022, 70.377854, 1080)) " +
            "not in (select distinct bx, by from tweets_" + keyword + ")"
        }
    }

    mode match {
      case Some(sliceMode) =>
         sliceMode match {
           case "offset" =>
             if (!start.isEmpty)
               sqlTemplate += " and create_at >= ?"
             if (!end.isEmpty)
               sqlTemplate +=" and create_at < ?"
             sqlTemplate += " offset ?"
             sqlTemplate += " limit ?"
           case "interval" =>
               sqlTemplate += " and create_at >= ?"
               sqlTemplate +=" and create_at < ?"
         }
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

  private def rewriteQuery(preparedStatement: PreparedStatement, keyword: String,
                           start: Option[DateTime], end: Option[DateTime],
                           offset: Option[Int], limit: Option[Int],
                           mode: Option[String], sliceInterval: Option[Int],
                           thisInterval: Option[Interval], thisOffset: Option[Int]):
  (Boolean, Option[Interval], Option[Int]) = {
    var pIndex = 1
    preparedStatement.setString(pIndex, keyword)
    pIndex += 1

    mode match {
      case Some(sliceMode) =>
        sliceMode match {
          case "offset" =>
            if (!start.isEmpty) {
              preparedStatement.setString(pIndex, dateTimeFormat.print(start.get))
              pIndex += 1
            }
            if (!end.isEmpty) {
              preparedStatement.setString(pIndex, dateTimeFormat.print(end.get))
              pIndex += 1
            }
            preparedStatement.setInt(pIndex, thisOffset.get)
            pIndex += 1
            preparedStatement.setInt(pIndex, sliceInterval.get)
            pIndex += 1
            (false, thisInterval, Option(thisOffset.get + sliceInterval.get))
          case "interval" =>
            var isDone: Boolean = false
            var interval: Int = sliceInterval.getOrElse(defaultSliceInterval)
            var thisEnd: DateTime = null
            var thisStart: DateTime = null
            // This first time rewrite a mini query
            if (thisInterval.isEmpty) {
              // Calculate this end
              if (!end.isEmpty) {
                if (end.get.isBefore(datasetEnd)) {
                  thisEnd = end.get
                }
                else {
                  thisEnd = datasetEnd
                }
              }
              else {
                thisEnd = datasetEnd
              }
              // Calculate this start
              thisStart = thisEnd.minusDays(interval)
              if (thisStart.isBefore(start.getOrElse(datasetStart))) {
                thisStart = start.getOrElse(datasetStart)
                isDone = true
              }
              if (thisStart.isBefore(datasetStart)) {
                thisStart = datasetStart
                isDone = true
              }
            }
            // Not first time rewrite a mini query, this interval was calculated
            else {
              thisStart = thisInterval.get.getStart
              thisEnd = thisInterval.get.getEnd
            }

            pIndex += 1
            preparedStatement.setString(pIndex, dateTimeFormat.print(thisStart))
            pIndex += 1
            preparedStatement.setString(pIndex, dateTimeFormat.print(thisEnd))

            // Calculate interval for next mini query
            val nextEnd: DateTime = thisStart
            var nextStart: DateTime = nextEnd.minusDays(interval)
            if (nextStart.isBefore(start.getOrElse(datasetStart))) {
              nextStart = start.getOrElse(datasetStart)
            }
            if (nextStart.isBefore(datasetStart)) {
              nextStart = datasetStart
            }
            val nextInterval: Interval = new Interval(nextStart, nextEnd)

            (isDone, Option(nextInterval), thisOffset)
        }
      case None => // no slice
        if (!start.isEmpty) {
          preparedStatement.setString(pIndex, dateTimeFormat.print(start.get))
          pIndex += 1
        }
        if (!end.isEmpty) {
          preparedStatement.setString(pIndex, dateTimeFormat.print(end.get))
          pIndex += 1
        }
        if (!offset.isEmpty) {
          preparedStatement.setInt(pIndex, offset.get)
          pIndex += 1
        }
        if (!limit.isEmpty) {
          preparedStatement.setInt(pIndex, limit.get)
          pIndex += 1
        }
        (true, thisInterval, thisOffset)
    }
  }

  private def genDataByArray(resultSet: ResultSet): (JsObject, Int) = {

    var T6_1 = 0.0
    var T6_2 = 0.0
    var length = 0
    val coordinates: ListBuffer[Array[Double]] = new ListBuffer[Array[Double]]
    val ids: ListBuffer[Long] = new ListBuffer[Long]
    while (resultSet.next) {
      length += 1

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

    //println("[DBConnector] In T6, get value  T6_1 = " + T6_1/1000.0 + "s")
    //println("[DBConnector] In T6, build json T6_2 = " + T6_2/1000.0 + "s")

    (data, length)
  }

  private def genDataByJson(resultSet: ResultSet): (JsArray, Int) = {

    var resultJsonArray: JsArray = Json.arr()

    var T6_1 = 0.0
    var T6_2 = 0.0
    var length = 0
    while (resultSet.next) {
      length += 1

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

    //println("[DBConnector] In T6, get value  T6_1 = " + T6_1/1000.0 + "s")
    //println("[DBConnector] In T6, build json T6_2 = " + T6_2/1000.0 + "s")

    (resultJsonArray, length)
  }

  override def preStart(): Unit = {
    println("DBConnector starting ...")
  }
}

object DBConnector {
  def props(out :ActorRef) = Props(new DBConnector(out))

  def startDB(): Unit = {
    println("Starting DB ...")
  }

  def stopDB(): Unit = {
    System.out.println("Stopping DB ...")
    //val result = "sudo -u postgres pg_ctl -D /Library/PostgreSQL/9.6/data stop" !
    //println(result)
  }
}