logLevel := Level.Warn

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.6")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

def outworkersPattern: Patterns = {
  val pattern = "[organisation]/[module](_[scalaVersion])(_[sbtVersion])/[revision]/[artifact]-[revision](-[classifier]).[ext]"

  Patterns(
    pattern :: Nil,
    pattern :: Nil,
    isMavenCompatible = true
  )
}

resolvers ++= Seq(
  Resolver.bintrayRepo("websudos", "oss-releases"),
  Resolver.url(
    "Maven Ivy Websudos",
    url(Resolver.DefaultMavenRepositoryRoot)
  )(outworkersPattern),
  Resolver.url(
    "Outworkers OSS",
    url("http://dl.bintray.com/websudos/oss-releases")
  )(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.websudos" %% "phantom-sbt" % "1.27.0")
