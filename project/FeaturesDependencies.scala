import sbt._

import scala.collection.GenTraversableOnce

object FeaturesDependencies {
  implicit class Exclude(module: ModuleID) {
    import ExclusionRules._

    def sparkNetworkCommonExclude: ModuleID = module
      .excludeAll(sparkNetworkCommonRule)

    def guavaExclude: ModuleID = module
      .excludeAll(guavaRule)

    def logbackExclude: ModuleID = module
      .excludeAll(logbackClassicRule)
      .excludeAll(logbackCoreRule)

    def commonsLoggingExclude: ModuleID = module
      .excludeAll(commonsLoggingRule)

    def kafkaExclusions: ModuleID = module
      .excludeAll(slf4jSimpleRule)
      .excludeAll(jmxriRule)
      .excludeAll(jmxTools)
      .excludeAll(joptSimple)
  }

  implicit def getModulesFromObject(obj: AnyRef): Seq[ModuleID] = {
    obj.getClass.getDeclaredFields.flatMap { f =>
      f.setAccessible(true)
      f.get(obj) match {
        case m: ModuleID => Seq(m)
        case c: GenTraversableOnce[_] => c.asInstanceOf[Seq[ModuleID]]
        case _ if f.getName == "MODULE$" => Seq.empty // Internal field from Scala objects
      }
    }
  }

  object ExclusionRules {
    val commonsLoggingRule     = ExclusionRule("commons-logging", "commons-logging")
    val guavaRule              = ExclusionRule("com.google.guava", "guava")
    val jacksonRule            = ExclusionRule("com.fasterxml.jackson.core", "jackson-databind")
    val jmxriRule              = ExclusionRule("com.sun.jmx", "jmxri")
    val jmxTools               = ExclusionRule("com.sun.jdmk", "jmxtools")
    val joptSimple             = ExclusionRule("net.sf.jopt-simple", "jopt-simple")
    val logbackClassicRule     = ExclusionRule("ch.qos.logback", "logback-classic")
    val logbackCoreRule        = ExclusionRule("ch.qos.logback", "logback-core")
    val slf4jSimpleRule        = ExclusionRule("org.slf4j", "slf4j-simple")
    val sparkNetworkCommonRule = ExclusionRule("org.apache.spark", "spark-network-common_2.11")
    val sparkSql               = ExclusionRule("org.apache.spark", "spark-sql_2.11")
    val spark                  = ExclusionRule(organization = "org.apache.spark")
  }

  object Version {
    val ElasticsearchSparkVersion = "6.0.1"
    val ElasticsearchVersion      = "6.1.2"
    val JodaTimeVersion           = "2.9.1"
    val AmazonRedshiftVersion     = "1.2.10.1009"
    val SparkVersion              = "2.3.1"
    val SparkCsvVersion           = "1.4.0"
    val ScalaMockVersion          = "3.2"
    val ScalaTestVersion          = "2.2.6"
    val SparkAvroVersion          = "4.0.0"
    val SparkRedshiftVersion      = "2.0.1"
    val SparkBaseTestVersion      = "2.3.1_0.10.0"
    val ApacheCommonsVersion      = "3.4"
    val GuavaVersion              = "19.0"
    val Json4sVersion             = "3.5.3"
    val TypesafeConfig            = "1.2.1"
    val LoggingVersion            = "3.5.0"
    val EmbeddedESVersion         = "2.4.2"
    val GeohashVersion            = "1.3.0"
    val MySqlJdbcVersion          = "5.1.6"

    val DATASTAX_VERSION          = "2.4.0"

    // Monitoring
    val DatadogMetricsVersion     = "1.1.2"
    val DropwizardMetricsVersion  = "3.1.2"
    val AwsVersion                = "1.11.105"
    val AwsSDKVersion             = "1.11.240"
  }

  object Compile {
    import Version._
    import ExclusionRules._

    object Config {
      val typesafeConfig = "com.typesafe" % "config" % TypesafeConfig
    }

    object Csv {
      val sparkCsv = "com.databricks" %% "spark-csv" % SparkCsvVersion
    }

    object JodaTime {
      val jodaTime = "joda-time" % "joda-time" % JodaTimeVersion
    }

    object Guava {
      val guava = "com.google.guava" % "guava" % GuavaVersion
    }

    object Json4s {
      val json4s = "org.json4s" %% "json4s-native" % Json4sVersion
    }

    object Monitoring {
      val dropwizardMetrics = "io.dropwizard.metrics"   % "metrics-core"               % DropwizardMetricsVersion excludeAll(guavaRule)
      val dropWizard        = "org.coursera"            % "dropwizard-metrics-datadog" % DatadogMetricsVersion excludeAll(guavaRule)
    }

    object Spark {
      val sparkCore  = "org.apache.spark" %% "spark-core"  % SparkVersion % "provided" excludeAll(sparkNetworkCommonRule, guavaRule)
      val sparkSql   = "org.apache.spark" %% "spark-sql"   % SparkVersion % "provided" excludeAll(sparkNetworkCommonRule, guavaRule)
      val sparkMLLib = "org.apache.spark" %% "spark-mllib" % SparkVersion % "provided" excludeAll(sparkNetworkCommonRule, guavaRule)
      val sparkHive  = "org.apache.spark" %% "spark-hive"  % SparkVersion % "provided" excludeAll(sparkNetworkCommonRule, guavaRule)
      val sparkAvro  = "com.databricks"   %% "spark-avro"  % SparkAvroVersion
      val sparkRedshift = "com.databricks" %% "spark-redshift" % SparkRedshiftVersion
      val logging       = "com.typesafe.scala-logging" %% "scala-logging" % LoggingVersion
    }

    object AwsLibraries {
      val aws            = "com.amazonaws"       % "aws-java-sdk-emr"             % AwsVersion
      val awsSDK         = "com.amazonaws"       % "aws-java-sdk"                 % AwsSDKVersion
      val awsSTS         = "com.amazonaws"       % "aws-java-sdk-sts"             % AwsSDKVersion
      val awsSSM         = "com.amazonaws"       % "aws-java-sdk-ssm"             % AwsSDKVersion
      val awsAthena      = "com.amazonaws"       % "aws-java-sdk-athena"          % AwsSDKVersion
      val amazonRedshift = "com.amazon.redshift" % "redshift-jdbc42"              % AmazonRedshiftVersion
    }

    object Elasticsearch {
      val elasticsearch      = "org.elasticsearch" % "elasticsearch" % ElasticsearchVersion excludeAll(guavaRule)
      val elasticsearchClient = "org.elasticsearch.client" % "transport" % ElasticsearchVersion excludeAll(guavaRule)
      val elasticsearchRestClient = "org.elasticsearch.client" % "elasticsearch-rest-client" % ElasticsearchVersion excludeAll(guavaRule)
      val elasticsearchHighLevelRestClient = "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % ElasticsearchVersion excludeAll(guavaRule)
      val elasticsearchSpark = "org.elasticsearch" %% "elasticsearch-spark-20" % ElasticsearchSparkVersion
    }

    object Test {
      val apacheCommons      = "org.apache.commons" % "commons-lang3"               % ApacheCommonsVersion    % "test" excludeAll(guavaRule)// needed to run inside IDE
      val sparkNetworkCommon = "org.apache.spark"  %%  "spark-network-common"       % SparkVersion            % "test" excludeAll(guavaRule)
      val sparkBaseTest      = "com.holdenkarau"   %% "spark-testing-base"          % SparkBaseTestVersion    % "test" excludeAll(guavaRule)
      val scalaMock          = "org.scalamock"     %% "scalamock-scalatest-support" % ScalaMockVersion        % "test" excludeAll(guavaRule)// BSD
      val scalaTest          = "org.scalatest"     %% "scalatest"                   % ScalaTestVersion        % "test" excludeAll(guavaRule)// ApacheV2
      val embeddedES         = "pl.allegro.tech" % "embedded-elasticsearch"         % EmbeddedESVersion       % "test"
    }

    object Cassandra {
      val sparkCassandra          = "com.datastax.spark"   %% "spark-cassandra-connector" % DATASTAX_VERSION excludeAll(guavaRule)
    }

    object Geospatial {
      val geohash = "ch.hsr" % "geohash" % GeohashVersion
    }

    object sparkLens {
      val sparkLens = "qubole" % "sparklens" % "0.3.0-s_2.11"
    }

    object Jdbc {
      val jdbcDriver = "mysql" % "mysql-connector-java" % MySqlJdbcVersion
    }
  }


  import Compile._
  
  val features: Seq[ModuleID] = JodaTime ++ Spark ++ AwsLibraries ++ Monitoring ++ Test ++ Guava ++ Config ++ Elasticsearch ++ Geospatial ++ sparkLens ++ Jdbc ++ Cassandra
}
