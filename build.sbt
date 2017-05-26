name := "lambdaconf-beam"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % "0.3.1",
  "com.spotify" %% "scio-test" % "0.3.1" % "test",
  "com.sksamuel.avro4s" %% "avro4s-core" % "1.6.4",
  "org.slf4j" % "slf4j-nop" % "1.7.25",
  "org.typelevel" %% "discipline" % "0.7.3" % "test",
  "org.typelevel" %% "cats-kernel-laws" % "0.9.0" % "test"
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
