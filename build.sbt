name := "lambdaconf-beam"

scalaVersion := "2.11.11"

// latest snapshot for scala 2.11
val avro4s = ProjectRef(uri("https://github.com/kanterov/avro4s.git"), "avro4s-core")

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % "0.3.1",
  "com.spotify" %% "scio-test" % "0.3.1" % "test",
  "org.slf4j" % "slf4j-nop" % "1.7.25",
  "org.typelevel" %% "discipline" % "0.7.3" % "test",
  "org.typelevel" %% "cats-kernel-laws" % "0.9.0" % "test"
)

dependsOn(avro4s)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
