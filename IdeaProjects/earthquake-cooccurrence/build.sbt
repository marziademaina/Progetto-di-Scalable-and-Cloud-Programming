name := "earthquake-cooccurrence"

version := "1.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
)

// Crea un fat JAR con assembly
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

// Nome del JAR di output
assembly / assemblyJarName := "earthquake-cooccurrence-assembly-1.0.jar"
