lazy val root = (project in file(".")).
  settings(
    name := "MovieStatistics",
    version := "1.0",
    scalaVersion := "2.12.2",
    mainClass in Compile := Some("ImdbInfo")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "io.spray" %%  "spray-json" % "1.3.4",
  "com.typesafe" % "config" % "1.2.1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


//baseAssemblySettings

//jarName in assembly :="Movie-Statistics-assembly.jar"

/*mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}*/




