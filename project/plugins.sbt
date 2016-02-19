scalacOptions ++= Seq("-unchecked","-deprecation", "-feature")

resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.1")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "1.5.1")
