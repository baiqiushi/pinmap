package controllers

import javax.inject._
import java.io.{File, FileInputStream}

import play.api.mvc._
import play.api.libs.json.{JsValue, Json, _}
import play.api.libs.streams.ActorFlow
import play.api.libs.ws.WSClient
import play.api.{Configuration, Environment, Logger}
import actor.PointsLoader
import akka.actor.ActorSystem
import akka.stream.Materializer

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(cc: ControllerComponents,
                               val wsClient: WSClient,
                               val config: Configuration,
                               val environment: Environment)
                              (implicit system: ActorSystem, mat: Materializer) extends AbstractController(cc) {

  val pointsDataFilePath: String = config.getString("pointsDataFile").getOrElse("/public/data/pointsData.txt")
  val pointsDataFile: File = environment.getFile(pointsDataFilePath)

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }


  def ws = WebSocket.accept[JsValue, JsValue] { request =>
    ActorFlow.actorRef { out =>
      PointsLoader.props(out, pointsDataFile)
    }
  }
}

object HomeController {
}
