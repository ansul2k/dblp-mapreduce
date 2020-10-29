name := "ansul_goenka_hw2"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  // Typesafe Config
  "com.typesafe" % "config" % "1.4.0",

  //Hadoop
  "org.apache.hadoop" % "hadoop-client" % "3.3.0",

  //junit testing framework
  "com.novocode" % "junit-interface" % "0.11" % Test,

  //scala XML
  "org.scala-lang.modules" %% "scala-xml" % "2.0.0-M2",

  //slf4
  "org.slf4j" % "slf4j-nop" % "1.8.0-beta4" % Test

)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}