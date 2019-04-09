package actor

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement, Timestamp}
import java.util.TimeZone
import sys.process._
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
  val hostname: String = "localhost"
  val url: String = s"jdbc:postgresql://$hostname:5432/pinmap"
  val username: String = "postgres"
  val password: String = "pinmap"
  val xColName: String = "x"
  val yColName: String = "y"
  val idColName: String = "id"
  val baseTableName: String = "ftweets"
  val datasetStart: DateTime = dateTimeFormat.parseDateTime("2017-01-24 00:00:00.000")
  val datasetEnd: DateTime = dateTimeFormat.parseDateTime("2017-09-10 00:00:00.000")
  TimeZone.setDefault(TimeZone.getTimeZone("GMT"))
  var defaultSliceInterval: Int = 30
  var sliceFirstInterval: Int = 3
  val defaultSliceOffset: Int = 5000
  var excludesWay: String = "bucket"
  var excludesBySubquery: Boolean = false
  var indexOnlyFlag: Boolean = true
  var countStar: Boolean = true

  override def receive: Receive = {
    case request : JsValue =>
      (request \ "cmd").asOpt[String] match {
        // Command
        case Some(command) =>
          MyLogger.info("[DBConnector] cmd received: " + request)
          command match {
            case "startDB" =>
              DBConnector.startDB()
            case "stopDB" =>
              DBConnector.stopDB()
          }
          out ! Json.toJson(Json.obj("cmd" -> command) ++ Json.obj("status" -> "ok"))
        // Query
        case _ =>

          MyLogger.info("[DBConnector] query received: " + request)

          val t1: Long = System.currentTimeMillis // t1 - request received

          // 1 Parse query JSON
          val keyword: Option[String] = (request \ "keyword").asOpt[String]
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
          sliceFirstInterval = (request \ "sliceFirstInterval").asOpt[Int].getOrElse(sliceFirstInterval)
          excludesWay = (request \ "excludesWay").asOpt[String].getOrElse(excludesWay)
          excludesBySubquery = (request \ "excludesBySubquery").asOpt[Boolean].getOrElse(excludesBySubquery)
          indexOnlyFlag = (request \ "indexOnly").asOpt[Boolean].getOrElse(indexOnlyFlag)
          val abandonData: Boolean = (request \ "abandonData").asOpt[Boolean].getOrElse(false)
          countStar = (request \ "countStar").asOpt[Boolean].getOrElse(false)

          var sqlTemplate: String = genSQLTemplate(keyword, start, end, offset, limit, mode, excludes)
          MyLogger.debug("sqlTemplate = " + sqlTemplate)
          val insertTemplate: String = genInsertSQLTemplate(keyword, start, end, offset, limit, mode, excludes)
          MyLogger.debug("insertTemplate = " + insertTemplate)

          val t2: Long = System.currentTimeMillis // t2 - request parsed

          // 2 Establish connection to DB with prepared statement
          Class.forName(driver)
          val connection: Connection = DriverManager.getConnection(url, username, password)

          // 2.1 If excludes ON, create temporary table
          if (excludes.getOrElse(false)) {
            val updateStatement: Statement = connection.createStatement
            val createTempTableSQL: String = genCreateTempTableSQLTemplate(keyword)
            MyLogger.debug("[DBConnector] create temporary table statement = " + createTempTableSQL)
            val success: Int = updateStatement.executeUpdate(createTempTableSQL)
            success match {
              case 0 =>
                MyLogger.debug("[DBConnector] create temporary table succeed.")
              case _ =>
                MyLogger.debug("[DBConnector] create temporary table fail.")
            }

            updateStatement.close
          }

          val queryStatement: PreparedStatement = connection.prepareStatement(sqlTemplate)

          val insertStatement: PreparedStatement = connection.prepareStatement(insertTemplate)

          val t3: Long = System.currentTimeMillis // t3 - db connected

          // 3 Rewrite query for slicing queries and run the query
          var resultSet: ResultSet = null
          var done: Boolean = false
          var thisInterval: Option[Interval] = None
          var thisOffset: Option[Int] = Option(0)
          do {
            // Rewrite query
            val (isDone, nextInterval, nextOffset) = rewriteQuery(queryStatement, keyword, start, end,
              offset, limit, mode, sliceInterval, thisInterval, thisOffset)

            MyLogger.debug("[DBConnector] query statement = " + queryStatement)

            // 3.1 If excludes ON, rewrite insert temporary table query
            if (excludes.getOrElse(false)) {
              rewriteQuery(insertStatement, keyword, start, end,
                offset, limit, mode, sliceInterval, thisInterval, thisOffset)
              MyLogger.debug("[DBConnector] insert statement = " + insertStatement)
            }

            done = isDone
            thisInterval = nextInterval
            thisOffset = nextOffset

            val t4: Long = System.currentTimeMillis // t4 - send db query

            resultSet = queryStatement.executeQuery()

            val t5: Long = System.currentTimeMillis // t5 - db result

            MyLogger.debug("[DBConnector] DB done. T4 + T5 =  " + (t5 - t4) / 1000.0 + "s")

            val (data, length) = countStar match {
              case true =>
                getCountStar(resultSet)
              case false =>
                // Two ways to return result
                val (tData, tLength) = (request \ "byArray").asOpt[Boolean] match {
                  case Some(true) =>
                    // Return result by array - all coordinates of all records in one array
                    genDataByArray (resultSet)

                  case _ =>
                    // Return result by JSON
                    genDataByJson (resultSet)
                }
                abandonData match {
                  case true =>
                    (Json.obj(), tLength)
                  case false =>
                    (tData, tLength)
                }
            }

            val T6 = System.currentTimeMillis() - t5
            MyLogger.debug("[DBConnector] JSON done. T6 = " + T6 / 1000.0 + "s")

            // 3.2 If excludes ON, insert the cell ids to the temporary table
            var T45i: Long = 0
            if (excludes.getOrElse(false)) {
              val t_insert_0: Long = System.currentTimeMillis
              val success: Int = insertStatement.executeUpdate()
              val t_insert_1: Long = System.currentTimeMillis
              T45i = t_insert_1 - t_insert_0
              MyLogger.debug("[DBConnector] insert into temporary table: " + success)
              MyLogger.debug("[DBConnector] T45i = " + T45i / 1000.0 + "s")
            }

            val t6 = System.currentTimeMillis()

            val responseJson: JsObject = Json.obj(
              "data" -> data,
              "length" -> length,
              "t1" -> JsNumber(t1),
              "T2" -> JsNumber(t2 - t1),
              "T3" -> JsNumber(t3 - t2),
              "T45" -> JsNumber(t5 - t4),
              "T6" -> JsNumber(T6),
              "t6" -> JsNumber(t6),
              "T45i" -> JsNumber(T45i)
            )
            out ! Json.toJson(responseJson)

            MyLogger.debug("[DBConnector] result length = " + length)

            if (!mode.isEmpty) {
              if(mode.get.toString == "offset" && length < sliceInterval.get.intValue) {
                done = true
              }
            }

          } while (!done)

          MyLogger.info("[DBConnector] ==> Query Done!")

          out ! Json.toJson(Json.obj("done" -> true))

          queryStatement.close()
          connection.close()
      }
  }

  private def genTempTableName(keyword: Option[String]) : String = {
    keyword match {
      case Some(kw) =>
        baseTableName + "_" + kw
      case None =>
        baseTableName + "_exclude"
    }
  }

  private def genExcludesStatement(keyword: Option[String], alias: String) : String = {

    val tempTableName = genTempTableName(keyword)

    excludesWay match {
      case "cid" =>
        s" $alias.cid not in (select distinct cid from $tempTableName) "
      case "bucket" =>
        s"""
           | (width_bucket($alias.x, -173.847656, -65.390625, 1920),
           | width_bucket($alias.y, 17.644022, 70.377854, 1080))
           | not in (select distinct bx, by from $tempTableName)
         """.stripMargin
    }
  }

  private def genCreateTempTableSQLTemplate(keyword: Option[String]): String = {

    val tempTableName = genTempTableName(keyword)

    excludesWay match {
      case "cid" =>
        s"""
           |create temp table $tempTableName
           | as select cid from $baseTableName where 1=2
         """.stripMargin
      case "bucket" =>
        s"""
           |create temp table $tempTableName
           | as select width_bucket(x, -173.847656, -65.390625, 1920) as bx,
           |           width_bucket(y, 17.644022, 70.377854, 1080) as by
           |  from $baseTableName where 1=2
         """.stripMargin
    }
  }

  private def genExcludingSubquery(keyword: Option[String], start: Option[DateTime], end: Option[DateTime],
                                     offset: Option[Int], limit: Option[Int], mode: Option[String],
                                     excludes: Option[Boolean]) : String = {
    var sqlTemplate: String =
      s"""
         |select id
         |  from $baseTableName t2
         | where 1=1
     """.stripMargin

    keyword match {
      case Some(kw) =>
        sqlTemplate += " and " + " to_tsvector('english', t2.text)@@to_tsquery('english', ?) "
      case None =>
    }

    sqlTemplate += " and " + genExcludesStatement(keyword, "t2")

    mode match {
      case Some(sliceMode) =>
        sliceMode match {
          case "offset" =>
            if (start.isDefined)
              sqlTemplate += " and t2.create_at >= ?"
            if (end.isDefined)
              sqlTemplate +=" and t2.create_at < ?"
            sqlTemplate += " offset ?"
            sqlTemplate += " limit ?"
          case "interval" =>
            sqlTemplate += " and t2.create_at >= ?"
            sqlTemplate +=" and t2.create_at < ?"
        }
      case None =>
        if (start.isDefined)
          sqlTemplate += " and t2.create_at >= ?"
        if (end.isDefined)
          sqlTemplate +=" and t2.create_at < ?"
        if (offset.isDefined)
          sqlTemplate += " offset ?"
        if (limit.isDefined)
          sqlTemplate += " limit ?"
    }

    sqlTemplate
  }

  private def genSQLTemplate(keyword: Option[String], start: Option[DateTime], end: Option[DateTime],
                             offset: Option[Int], limit: Option[Int], mode: Option[String],
                             excludes: Option[Boolean]) : String = {
    var sqlTemplate: String =
      s"""
         |select x, y, id
         |  from $baseTableName t1
         | where 1=1
     """.stripMargin

    if (countStar) {
      sqlTemplate = "select count(*) from (" + sqlTemplate
    }

    indexOnlyFlag match {
      case true =>
        sqlTemplate = "/*+ IndexOnlyScan(t1) */ " + sqlTemplate
      case false =>
        excludes.getOrElse(false) match {
          case true =>
            sqlTemplate = "/*+ BitmapScan(t1) IndexOnlyScan(t2) */ " + sqlTemplate
          case false =>
            sqlTemplate = "/*+ BitmapScan(t1) */ " + sqlTemplate
        }
    }

    if (excludes.getOrElse(false)) {
      excludesBySubquery match {
        case true =>
          sqlTemplate += " and t1.id in (" +
            genExcludingSubquery(keyword, start, end, offset, limit, mode, excludes) + ")"
        case false =>
          keyword match {
            case Some(kw) =>
              sqlTemplate += " and " + " to_tsvector('english', t1.text)@@to_tsquery('english', ?) "
            case None =>
          }

          sqlTemplate += " and " + genExcludesStatement(keyword, "t1")

          mode match {
            case Some(sliceMode) =>
              sliceMode match {
                case "offset" =>
                  if (start.isDefined)
                    sqlTemplate += " and t1.create_at >= ?"
                  if (end.isDefined)
                    sqlTemplate += " and t1.create_at < ?"
                  sqlTemplate += " offset ?"
                  sqlTemplate += " limit ?"
                case "interval" =>
                  sqlTemplate += " and t1.create_at >= ?"
                  sqlTemplate += " and t1.create_at < ?"
              }
            case None =>
              if (start.isDefined)
                sqlTemplate += " and t1.create_at >= ?"
              if (end.isDefined)
                sqlTemplate += " and t1.create_at < ?"
              if (offset.isDefined)
                sqlTemplate += " offset ?"
              if (limit.isDefined)
                sqlTemplate += " limit ?"
          }
      }
    }
    else {
      keyword match {
        case Some(kw) =>
          sqlTemplate += " and " + " to_tsvector('english', t1.text)@@to_tsquery('english', ?) "
        case None =>
      }

      mode match {
        case Some(sliceMode) =>
          sliceMode match {
            case "offset" =>
              if (start.isDefined)
                sqlTemplate += " and t1.create_at >= ?"
              if (end.isDefined)
                sqlTemplate += " and t1.create_at < ?"
              sqlTemplate += " offset ?"
              sqlTemplate += " limit ?"
            case "interval" =>
              sqlTemplate += " and t1.create_at >= ?"
              sqlTemplate += " and t1.create_at < ?"
          }
        case None =>
          if (start.isDefined)
            sqlTemplate += " and t1.create_at >= ?"
          if (end.isDefined)
            sqlTemplate += " and t1.create_at < ?"
          if (offset.isDefined)
            sqlTemplate += " offset ?"
          if (limit.isDefined)
            sqlTemplate += " limit ?"
      }
    }

    if (countStar) {
      sqlTemplate = sqlTemplate + ") tc"
    }
    sqlTemplate
  }

  //TODO - Combine this function with genSQLTemplate by extracting common part
  private def genInsertSQLTemplate(keyword: Option[String], start: Option[DateTime], end: Option[DateTime],
                                   offset: Option[Int], limit: Option[Int], mode: Option[String],
                                   excludes: Option[Boolean]) : String = {

    val tempTableName = genTempTableName(keyword)

    var insertSQLTemplate: String =
      excludesWay match {
        case "cid" =>
          s"""
             |insert into $tempTableName
             |select distinct
             |       cid
             |  from $baseTableName t2
             | where 1=1
           """.stripMargin
        case "bucket" =>
          s"""
             |insert into $tempTableName
             |select distinct
             |       width_bucket(x, -173.847656, -65.390625, 1920) as bx,
             |       width_bucket(y, 17.644022, 70.377854, 1080) as by
             |  from $baseTableName t2
             | where 1=1
           """.stripMargin
      }

    insertSQLTemplate = "/*+ IndexOnlyScan(t2) */ " + insertSQLTemplate

    keyword match {
      case Some(kw) =>
        insertSQLTemplate += " and " + " to_tsvector('english', t2.text)@@to_tsquery('english', ?) "
      case None =>
    }

    if (excludes.getOrElse(false)) {
      insertSQLTemplate += " and " + genExcludesStatement(keyword, "t2")
    }

    mode match {
      case Some(sliceMode) =>
        sliceMode match {
          case "offset" =>
            if (!start.isEmpty)
              insertSQLTemplate += " and t2.create_at >= ?"
            if (!end.isEmpty)
              insertSQLTemplate +=" and t2.create_at < ?"
            insertSQLTemplate += " offset ?"
            insertSQLTemplate += " limit ?"
          case "interval" =>
            insertSQLTemplate += " and t2.create_at >= ?"
            insertSQLTemplate +=" and t2.create_at < ?"
        }
      case None =>
        if (!start.isEmpty)
          insertSQLTemplate += " and t2.create_at >= ?"
        if (!end.isEmpty)
          insertSQLTemplate +=" and t2.create_at < ?"
        if (!offset.isEmpty)
          insertSQLTemplate += " offset ?"
        if (!limit.isEmpty)
          insertSQLTemplate += " limit ?"
    }

    insertSQLTemplate
  }

  private def rewriteQuery(preparedStatement: PreparedStatement, keyword: Option[String],
                           start: Option[DateTime], end: Option[DateTime],
                           offset: Option[Int], limit: Option[Int],
                           mode: Option[String], sliceInterval: Option[Int],
                           thisInterval: Option[Interval], thisOffset: Option[Int]):
  (Boolean, Option[Interval], Option[Int]) = {
    var pIndex = 1
    keyword match{
      case Some(kw) =>
        preparedStatement.setString(pIndex, kw)
        pIndex += 1
      case None =>
    }

    mode match {
      case Some(sliceMode) =>
        sliceMode match {
          case "offset" =>
            if (!start.isEmpty) {
              preparedStatement.setTimestamp(pIndex, new Timestamp(start.get.getMillis))
              pIndex += 1
            }
            if (!end.isEmpty) {
              preparedStatement.setTimestamp(pIndex, new Timestamp(end.get.getMillis))
              pIndex += 1
            }
            preparedStatement.setInt(pIndex, thisOffset.get)
            pIndex += 1
            preparedStatement.setInt(pIndex, sliceInterval.getOrElse(defaultSliceOffset))
            pIndex += 1
            (false, thisInterval, Option(thisOffset.get + sliceInterval.getOrElse(defaultSliceOffset)))
          case "interval" =>
            var isDone: Boolean = false
            var interval: Int = sliceInterval.getOrElse(defaultSliceInterval)
            var thisEnd: DateTime = null
            var thisStart: DateTime = null
            // This first time rewrite a mini query
            if (thisInterval.isEmpty) {
              //the first query uses a different interval
              interval = sliceFirstInterval
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
              if (thisStart.isBefore(start.getOrElse(datasetStart)) || thisStart.isEqual(start.getOrElse(datasetStart))) {
                thisStart = start.getOrElse(datasetStart)
                isDone = true
              }
              if (thisStart.isBefore(datasetStart) || thisStart.isEqual(datasetStart)) {
                thisStart = datasetStart
                isDone = true
              }
            }
            // Not first time rewrite a mini query, this interval was calculated
            else {
              thisStart = thisInterval.get.getStart
              thisEnd = thisInterval.get.getEnd
            }

            preparedStatement.setTimestamp(pIndex, new Timestamp(thisStart.getMillis))
            pIndex += 1
            preparedStatement.setTimestamp(pIndex, new Timestamp(thisEnd.getMillis))
            pIndex += 1

            // Calculate interval for next mini query
            interval = defaultSliceInterval
            val nextEnd: DateTime = thisStart
            var nextStart: DateTime = nextEnd.minusDays(interval)
            if (nextStart.isBefore(start.getOrElse(datasetStart)) || thisStart.isEqual(start.getOrElse(datasetStart))) {
              nextStart = start.getOrElse(datasetStart)
            }
            if (nextStart.isBefore(datasetStart) || thisStart.isEqual(datasetStart)) {
              nextStart = datasetStart
            }
            val nextInterval: Interval = new Interval(nextStart, nextEnd)
            if (nextEnd.isBefore(start.getOrElse(datasetStart)) || nextEnd.isEqual(start.getOrElse(datasetStart))) {
              isDone = true
            }
            if (nextEnd.isBefore(datasetStart) || nextEnd.isEqual(datasetStart)) {
              isDone = true
            }

            (isDone, Option(nextInterval), thisOffset)
        }
      case None => // no slice
        if (!start.isEmpty) {
          preparedStatement.setTimestamp(pIndex, new Timestamp(start.get.getMillis))
          pIndex += 1
        }
        if (!end.isEmpty) {
          preparedStatement.setTimestamp(pIndex, new Timestamp(end.get.getMillis))
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

  private def getCountStar(resultSet: ResultSet) : (JsObject, Int) = {
    resultSet.next()
    val count : Int = resultSet.getInt(1)
    (Json.obj(), count)
  }

  override def preStart(): Unit = {
    println("DBConnector starting ...")
  }
}

object DBConnector {
  //val osPassword: String = "3979"
  //val startPostgresCMD: String = "sudo -S -u postgres pg_ctl -D /Library/PostgreSQL/9.6/data start"
  //val stopPostgresCMD: String = "sudo -S -u postgres pg_ctl -D /Library/PostgreSQL/9.6/data stop"
  val osPassword: String = "root3979"
  val startPostgresCMD: String = "sudo -S systemctl start postgresql-9.6"
  val stopPostgresCMD: String = "sudo -S systemctl stop postgresql-9.6"

  def props(out :ActorRef) = Props(new DBConnector(out))

  def startDB(): Unit = {
    println("Starting DB ...")
    val result = s"echo $osPassword" #| startPostgresCMD !

    println("command result: " + result)
  }

  def stopDB(): Unit = {
    println("Stopping DB ...")
    val result = s"echo $osPassword" #| stopPostgresCMD !

    println("command result: " + result)
  }
}

object MyLogger {

  private val DEBUG = true

  def debug(msg: String): Unit = {
    if (DEBUG) {
      println(msg)
    }
  }

  def info(msg: String): Unit = {
    println(msg)
  }
}