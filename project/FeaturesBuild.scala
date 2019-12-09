import sbt._
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._
import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform._
import com.typesafe.sbt.packager.universal.UniversalPlugin
import com.websudos.phantom.sbt.PhantomSbtPlugin
import com.websudos.phantom.sbt.PhantomSbtPlugin.autoImport._

object FeaturesBuild extends Build {
  import FeaturesPackage._

  lazy val buildResolvers: Seq[MavenRepository] = Seq(
    "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
    "Typesafe repository releases"  at "http://repo.typesafe.com/typesafe/releases/",
    "Sonatype repo"                 at "https://oss.sonatype.org/content/groups/scala-tools/",
    "Sonatype releases"             at "https://oss.sonatype.org/content/repositories/releases",
    "Sonatype snapshots"            at "https://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype staging"              at "http://oss.sonatype.org/content/repositories/staging",
    "Java.net Maven2 Repository"    at "http://download.java.net/maven/2/",
    "Twitter Repository"            at "http://maven.twttr.com",
    "spray repo"                    at "http://repo.spray.io",
    "spark lens"                    at "https://dl.bintray.com/spark-packages/maven/",
    "redshift"                      at "https://s3.amazonaws.com/redshift-maven-repository/release",
    Resolver.bintrayRepo("websudos", "oss-releases"),
    Resolver.mavenLocal
  )

  lazy val basicSettings: Seq[Setting[_]] = Seq(
    organization  := "com.paytm.map.features",
    version       := "1.38.0",
    scalaVersion  := "2.11.11",
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    javacOptions  ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions   ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    resolvers     ++= buildResolvers
  )

  lazy val assemblySettings: Seq[Setting[_]] = Seq(
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) =>
        MergeStrategy.discard
      case PathList("com", "google", xs @ _*) =>
        MergeStrategy.last
      case PathList("com", "esotericsoftware", "minlog", xs @ _ *) =>
        MergeStrategy.last
      case PathList("io", "netty", xs @ _*) =>
        MergeStrategy.last
      case PathList("javax", "xml", xs @ _*) =>
        MergeStrategy.last
      case PathList("org", "apache", "commons", xs @ _*) =>
        MergeStrategy.last
      case PathList("org", "apache", "hadoop", "yarn", xs @ _*) =>
        MergeStrategy.last
      case PathList("org", "apache", "spark", xs @ _*) =>
        MergeStrategy.last
      case PathList("org", "fusesource", xs @ _*) =>
        MergeStrategy.last
      case PathList("org", "datanucleus", xs @ _*) =>
        MergeStrategy.last
      case oldDependency =>
        MergeStrategy.last
    },
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("com.google.**" -> "com.paytm.@1")
        .inAll
    ),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    test in assembly := {}
  )

  lazy val testSettings: Seq[Setting[_]] = Seq(
    fork := true,
    parallelExecution in Test := false
  )

  lazy val scalariformSettings: Seq[Setting[_]] =
    SbtScalariform.scalariformSettings ++ Seq(
      ScalariformKeys.preferences := ScalariformKeys.preferences.value
          .setPreference(AlignSingleLineCaseStatements, true)
          .setPreference(AlignArguments, true)
          .setPreference(DoubleIndentClassDeclaration, true)
          .setPreference(SpacesAroundMultiImports, false)
    )

  lazy val mapfeatures = Project("mapfeatures", file("mapfeatures"), settings = Defaults.coreDefaultSettings)
    .settings(libraryDependencies ++= FeaturesDependencies.features)
    .settings(basicSettings: _*)
    .settings(assemblySettings: _*)
    .settings(packageSettings: _*)
    .settings(scalariformSettings: _*)
    .settings(name := "mapfeatures")
    .settings(assemblyJarName in assembly := name.value.toLowerCase + "_" + version.value + ".jar")
    .settings(testSettings: _*)
    .enablePlugins(UniversalPlugin)

}
