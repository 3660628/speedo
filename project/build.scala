import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._
import de.heikoseeberger.sbtheader._

object SpeeDOBuild extends Build {
  // configuration to assembly jar and run without Yarn
  val AkkaConfig = config("akka") extend(Compile)

  lazy val speedo = (project in file("."))
    .enablePlugins(AutomateHeaderPlugin)
    .configs(AkkaConfig)
    .settings(
      version := "1.0",
      scalaVersion := "2.11.7",
      crossScalaVersions := Seq("2.11.7", "2.10.6"),
      scalacOptions ++= Seq("-target:jvm-1.7", "-deprecation", "-unchecked", "-feature"),
      updateOptions := updateOptions.value.withCachedResolution(true).withLatestSnapshots(false),
      libraryDependencies ++= Seq(
        "com.htc.speedo" % "caffe-jni" % "0.1" % "compile,akka",
        "com.typesafe.akka" %% "akka-remote" % "2.3.14" % "compile,akka",
        "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "akka,provided",
        "com.twitter" %% "scalding-args" % "0.15.0" % "compile,akka",
        "com.twitter" %% "storehaus-redis" % "0.13.0" % "compile,akka",

        "org.specs2" %% "specs2-junit" % "3.3.1" % "test",
        "com.typesafe.akka" %% "akka-testkit" % "2.3.14" % "test"
      ),
      libraryDependencies ++= {
        CrossVersion.partialVersion(scalaVersion.value) match {
          // if scala 2.11+ is used, add dependency on scala-xml module
          case Some((2, scalaMajor)) if scalaMajor >= 11 =>
              Seq("org.scala-lang.modules" %% "scala-xml" % "1.0.3" % "compile,akka")
          case _ => Nil
        }
      },
      resolvers ++= Seq(
        "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases",
        Resolver.mavenLocal
      ),
      testFrameworks := Seq(sbt.TestFrameworks.Specs2),
      test in assembly := {},
      mainClass in (Compile, run) := Some("com.htc.speedo.akka.AkkaUtil"),
      mainClass in assembly := Some("com.htc.speedo.yarn.AppClient"),
      assemblyJarName in assembly := "SpeeDO-yarn-" + version.value + ".jar",
      HeaderPlugin.autoImport.headers := Map(
        "scala" -> license.Apache2_0("2016", "HTC Corporation")
      )
    )
    .settings(inConfig(AkkaConfig)(Classpaths.configSettings ++ Defaults.configTasks ++ baseAssemblySettings ++ Seq(
      compile := (compile in Compile).value,
      test := {},
      mainClass in assembly := Some("com.htc.speedo.akka.AkkaUtil"),
      assemblyJarName in assembly := "SpeeDO-akka-" + version.value + ".jar",
      assemblyMergeStrategy in assembly := {
        case PathList("org", "apache", xs @ _*) => MergeStrategy.first
        case x => (assemblyMergeStrategy in assembly).value(x)
      }
    )): _*)
}
