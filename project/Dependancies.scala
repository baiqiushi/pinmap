import sbt._
import play.sbt.PlayImport._

object Dependencies {

  val pinmapDependencies: Seq[ModuleID] = Seq(
    ws, // Play's web services module
    "org.webjars" % "bootstrap" % "3.3.6",
    "org.webjars" % "flot" % "0.8.0",
    "org.webjars" % "angularjs" % "1.5.0",
    "org.eclipse.jetty.websocket" % "websocket-client" % "9.4.7.v20170914",
    // map module
    "org.webjars" % "leaflet" % "0.7.7",
    "org.webjars" % "angular-leaflet-directive" % "0.8.2",
    "org.webjars.bower" % "json-bigint" % "0.0.0",
    "org.webjars.bower" % "bootstrap-toggle" % "2.2.2",
    // mapresultcache module
    "org.webjars.bower" % "hashmap" % "2.0.5",
    // timeseries module
    "org.webjars.bower" % "crossfilter" % "1.3.11",
    "org.webjars.bower" % "dc.js" % "1.7.5",
    "org.webjars" % "d3js" % "3.5.16",
    // sidebar module
    "org.webjars" % "font-awesome" % "4.5.0",
    "org.webjars.bower" % "bootstrap-vertical-tabs" % "1.2.1",
    // other module
    "com.fasterxml.jackson.core" % "jackson-core" % "2.9.4",
    // Added jquery-ui for showing/hiding the time series histogram.
    "org.webjars.bower" % "jquery-ui" % "1.12.1",
    // draw chart module
    "org.webjars.bower" % "chart.js" % "2.7.2",
    // Added jquery-ui theme for decorating auto-complete menu
    "org.webjars" % "jquery-ui-themes" % "1.12.1"
  )
}
