name := "lambdaconf-beam"

scalaVersion := "2.11.11"

resolvers += Resolver.bintrayRepo("kanterov", "maven")

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % "0.3.1",
  "com.spotify" %% "scio-test" % "0.3.1" % "test",
  "com.sksamuel.avro4s" %% "avro4s-core" % "1.6.5-kanterov",
  "org.slf4j" % "slf4j-nop" % "1.7.25",
  "org.typelevel" %% "discipline" % "0.7.3" % "test",
  "org.typelevel" %% "cats-kernel-laws" % "0.9.0" % "test"
)

commands ++= Seq(
  AvroUtils.avroRead,
  AvroUtils.avroSchema
)

addCommandAlias("unit-1", ";clean-out; runMain us.lambdaconf.TrackCountJob")
addCommandAlias("test-unit-1", "test-only us.lambdaconf.PlayTrackCountTest")

addCommandAlias("unit-2", ";clean-out; runMain us.lambdaconf.GenreJob")
addCommandAlias("test-unit-2", "test-only us.lambdaconf.GenreJobTest")

addCommandAlias("unit-3", ";test-unit-3")
addCommandAlias("test-unit-3", ";test-only us.lambdaconf.WindowedContentHoursTest")

cancelable in Global := true

val cleanOut = TaskKey[Unit]("clean-out")
cleanOut := {
  (file("out") * "*").get.foreach { file =>
    streams.value.log.info(s"Removing $file")
    sbt.IO.delete(file)
  }
}

run in Compile := {
  cleanOut.value
  (run in Compile).evaluated
}

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
