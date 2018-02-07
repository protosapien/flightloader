name := "FlightsLoader"

version := "1.1"

scalaVersion := "2.11.11"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Xlint")

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0",
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
)

mainClass in assembly := some("com.datasciencegroup.flights.FlightsLoader")

assemblyJarName := "FlightsLoader_1.1.jar"

val meta = """META.INF(.)*""".r
  assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case n if n.startsWith("reference.conf") => MergeStrategy.concat
    case n if n.endsWith(".conf") => MergeStrategy.concat
    case meta(_) => MergeStrategy.discard
    case x => MergeStrategy.first
}





    