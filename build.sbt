import Dependencies._

name := "pinmap"
 
version := "1.0" 
      
//lazy val `pinmap` = (project in file(".")).enablePlugins(PlayScala)
lazy val `pinmap` = (project in file(".")).
  settings(Commons.playSettings: _*).
  settings(
    libraryDependencies ++= pinmapDependencies
  ).
  settings(
    mappings in Universal ++=
      (baseDirectory.value / "public" / "data" * "*" get) map
        (x => x -> ("public/data/" + x.getName))
  ).enablePlugins(PlayScala)

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"
      
resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
      
scalaVersion := "2.12.2"

libraryDependencies ++= Seq( jdbc , ehcache , ws , specs2 % Test , guice )
//libraryDependencies ++= pinmapDependencies

unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )  

      