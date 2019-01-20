package actor

import java.io.{File, FileInputStream}

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import play.api.Logger
import play.api.libs.json._

class PointsLoader (val out: ActorRef, val pointsDataFile: File) extends Actor with ActorLogging {

  private val logger = Logger("client")
  val pointsData: JsArray = PointsLoader.loadPointsData(pointsDataFile)

  override def receive: Receive = {
    case request : JsValue =>
      val offset = (request \ "offset").as[BigDecimal].intValue()
      val size  = (request \ "size").as[BigDecimal].intValue()
      logger.info("PoinstsLoader <== " + request.toString())
      System.out.println("PoinstsLoader <== " + request.toString())
      System.out.println("offset: " + offset)
      System.out.println("size: " + size)

      val delta = pointsData.value.slice(offset, offset + size)
      System.out.println(delta.slice(0,1))

      //out ! JsObject(Seq("OK" -> JsString(s"Get your request!")))
      out ! Json.toJson(delta)
  }
}

object PointsLoader {
  def props(out: ActorRef, pointsDataFile: File) = Props(new PointsLoader(out, pointsDataFile))

  def loadPointsData(file: File): JsArray = {
    val stream = new FileInputStream(file)
    val json = Json.parse(stream).as[JsArray]
    stream.close()
    json
  }
}
