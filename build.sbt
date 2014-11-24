name := "let_see_spark"

version := "1.0"

scalaVersion := "2.10.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.1.0"

transitiveClassifiers in Global := Seq(Artifact.SourceClassifier)

ideaExcludeFolders += ".idea"

ideaExcludeFolders += ".idea_modules"
