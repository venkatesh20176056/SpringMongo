import java.util.UUID

import sbt._
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._
import com.typesafe.sbt.SbtNativePackager._
import com.typesafe.sbt.SbtNativePackager.autoImport._
import NativePackagerHelper._

/**
  * http://www.scala-sbt.org/sbt-native-packager/formats/universal.html
  */
object FeaturesPackage {
  object AzkabanEnvironments {
    val StagingEnvironment = "staging"
    val ProdEnvironment    = "prod"

    val allowedEnvironments = Seq(
      StagingEnvironment,
      ProdEnvironment
    )

    def isValidEnvironment(environment: String): Boolean =
      allowedEnvironments.contains(environment)
  }

  object AirflowBinPackage {
    import AzkabanEnvironments._

    /**
      * Package Settings:
      */
    def packageAirflowBinSettings(environment: String): Seq[Setting[_]] = Seq(
      name        in Universal := name.value.toLowerCase + "_bin_" + version.value + "_" + environment,
      packageName in Universal := name.value.toLowerCase + "_bin_" + version.value + "_" + environment,
      mappings in Universal <+= (assembly in Compile) map { fatJar =>
        fatJar -> fatJar.getName
      },
      mappings in Universal ++= directory("airflow/bin"),
      mappings in Universal ++= directory("airflow/resources"),
      mappings in Universal ++= contentOf("airflow/settings/" + environment)
    )

    /**
      * SBT Command: packageAirflowBin.
      *
      * Essentially lifts project into a temporary project with packageAzkabanBinSettings and executes packageBin.
      */
    lazy val packageAirflowBinCommand: Command = Command.single(
      name      = "packageAirflowBin",
      briefHelp = ("Packages mapfeatures_bin_<version>_<environment>.zip", "Deployment Environment"),
      detail    = "Packages an zipped archive of an assembled fat JAR and airflow/bin files."
    ) { (currentState, currentEnvironment) =>
      require(isValidEnvironment(currentEnvironment),
        s"$currentEnvironment is not a valid environment. Allowed environments: ${allowedEnvironments.mkString(", ")}.")

      val extractedState = Project.extract(currentState)

      val newState = extractedState
        .append(packageAirflowBinSettings(currentEnvironment), currentState)
      val (commandState, _) = Project.extract(newState).runTask(packageBin in Universal, newState)

      commandState
    }
  }

  object AzkabanBinPackage {
    import AzkabanEnvironments._

    /**
      * Package Settings:
      */
    def packageAzkabanBinSettings(environment: String): Seq[Setting[_]] = Seq(
      name        in Universal := name.value.toLowerCase + "_bin_" + version.value + "_" + environment,
      packageName in Universal := name.value.toLowerCase + "_bin_" + version.value + "_" + environment,
      mappings in Universal <+= (assembly in Compile) map { fatJar =>
        fatJar -> fatJar.getName
      },
      mappings in Universal ++= directory("azkaban/bin"),
      mappings in Universal ++= directory("azkaban/resources"),
      mappings in Universal ++= contentOf("azkaban/settings/" + environment)
    )

    /**
      * SBT Command: packageAzkabanBin.
      *
      * Essentially lifts project into a temporary project with packageAzkabanBinSettings and executes packageBin.
      */
    lazy val packageAzkabanBinCommand: Command = Command.single(
      name      = "packageAzkabanBin",
      briefHelp = ("Packages mapfeatures_bin_<version>_<environment>.zip", "Deployment Environment"),
      detail    = "Packages an zipped archive of an assembled fat JAR and azkaban/bin files."
    ) { (currentState, currentEnvironment) =>
      require(isValidEnvironment(currentEnvironment),
        s"$currentEnvironment is not a valid environment. Allowed environments: ${allowedEnvironments.mkString(", ")}.")

      val extractedState = Project.extract(currentState)

      val newState = extractedState
        .append(packageAzkabanBinSettings(currentEnvironment), currentState)
      val (commandState, _) = Project.extract(newState).runTask(packageBin in Universal, newState)

      commandState
    }
  }

  object AirflowPackage {
    import AzkabanEnvironments._

    /**
      * SBT Command: packageAzkabanFlow.
      *
      * Packages Azkaban flow files for a specific environment into a zipped archive.
      */

    val commandName = "packageAirflow"

    lazy val packageFlow: Command = Command.single(
      name      = commandName,
      briefHelp = ("Packages mapfeatures_flow_<version>_<environment>.zip", "Deployment Environment"),
      detail    = "Packages an zipped archive of azkaban/flow files."
    ) { (currentState, currentEnvironment) =>
      require(isValidEnvironment(currentEnvironment),
        s"$currentEnvironment is not a valid environment. Allowed environments: ${allowedEnvironments.mkString(", ")}.")

      val extractedState = Project.extract(currentState)
      val extractedLogger        = extractedState.get(sLog)
      val extractedNameString    = extractedState.get(name)
      val extractedVersionString = extractedState.get(version)
      val extractedTargetFile    = extractedState.get(target)
      val extractedTempDir       = extractedState.get(taskTemporaryDirectory)

      val packageName = extractedNameString.toLowerCase + "_airflow_" + extractedVersionString + "_" + currentEnvironment
      val tempName    = UUID.randomUUID().toString
      val zipName     = packageName + ".zip"

      extractedLogger.info(s"Packaging Azkaban Flow: $packageName.")

      val zipFile = extractedTargetFile / "universal" / zipName
      IO.delete(zipFile)

      val tempDir        = extractedTempDir / tempName
      val zipDir         = tempDir          / packageName
      val azkabanFlowDir = zipDir           / "airflow" / "map_features"

      try {
        IO.createDirectories(Seq(zipDir) ++
          Seq(azkabanFlowDir))

        extractedLogger.info(s"Created temporary directory at ${zipDir.absolutePath}.")

        val flowDir = file("airflow") / "map_features"
        val confDir = file("airflow") / "conf" / currentEnvironment

        IO.copyDirectory(
          source = flowDir,
          target = azkabanFlowDir,
          overwrite = true
        )

        extractedLogger.info(s"Copied flow in ${flowDir.getAbsolutePath}.")
        IO.copyDirectory(
          source = confDir,
          target = azkabanFlowDir,
          overwrite = true
        )

        extractedLogger.info(s"Copied configurations in ${confDir.getAbsolutePath}.")

        IO.zip(Path.allSubpaths(tempDir), zipFile)

        extractedLogger.info(s"Zipped as ${zipFile.name} to ${zipFile.absolutePath}.")
      } finally {
        IO.delete(tempDir)
        extractedLogger.info(s"Deleted temporary directory at ${zipDir.absolutePath}.")
      }

      currentState
    }
  }

  trait FlowPackage {
    import AzkabanEnvironments._

    /**
      * SBT Command: packageAzkabanFlow.
      *
      * Packages Azkaban flow files for a specific environment into a zipped archive.
      */

      val commandName : String
      val flowPath : String

    lazy val packageFlow: Command = Command.single(
      name      = commandName,
      briefHelp = ("Packages mapfeatures_flow_<version>_<environment>.zip", "Deployment Environment"),
      detail    = "Packages an zipped archive of azkaban/flow files."
    ) { (currentState, currentEnvironment) =>
      require(isValidEnvironment(currentEnvironment),
        s"$currentEnvironment is not a valid environment. Allowed environments: ${allowedEnvironments.mkString(", ")}.")

      val extractedState = Project.extract(currentState)
      val extractedLogger        = extractedState.get(sLog)
      val extractedNameString    = extractedState.get(name)
      val extractedVersionString = extractedState.get(version)
      val extractedTargetFile    = extractedState.get(target)
      val extractedTempDir       = extractedState.get(taskTemporaryDirectory)

      val packageName = extractedNameString.toLowerCase + "_" + flowPath + "_" + extractedVersionString + "_" + currentEnvironment
      val tempName    = UUID.randomUUID().toString
      val zipName     = packageName + ".zip"

      extractedLogger.info(s"Packaging Azkaban Flow: $packageName.")

      val zipFile = extractedTargetFile / "universal" / zipName
      IO.delete(zipFile)

      val tempDir        = extractedTempDir / tempName
      val zipDir         = tempDir          / packageName
      val azkabanFlowDir = zipDir           / "azkaban" / flowPath

      try {
        IO.createDirectories(Seq(zipDir) ++
          Seq(azkabanFlowDir))

        extractedLogger.info(s"Created temporary directory at ${zipDir.absolutePath}.")

        val flowDir = file("azkaban") / flowPath
        val confDir = file("azkaban") / "conf" / currentEnvironment

        IO.copyDirectory(
          source = flowDir,
          target = azkabanFlowDir,
          overwrite = true
        )

        extractedLogger.info(s"Copied flow in ${flowDir.getAbsolutePath}.")
        IO.copyDirectory(
          source = confDir,
          target = azkabanFlowDir,
          overwrite = true
        )

        extractedLogger.info(s"Copied configurations in ${confDir.getAbsolutePath}.")

        IO.zip(Path.allSubpaths(tempDir), zipFile)

        extractedLogger.info(s"Zipped as ${zipFile.name} to ${zipFile.absolutePath}.")
      } finally {
        IO.delete(tempDir)
        extractedLogger.info(s"Deleted temporary directory at ${zipDir.absolutePath}.")
      }

      currentState
    }
  }

  object RawFlowPackage extends FlowPackage {
    val commandName = "packageRawFlow"
    val flowPath = "rawflow"
  }

  object SnapshotFlowPackage extends FlowPackage {
    val commandName = "packageSnapshotFlow"
    val flowPath = "snapshotflow"
  }

  object ModelFlowPackage extends FlowPackage {
    val commandName = "packageModelFlow"
    val flowPath = "modelflow"
  }

  object CampaignFlowPackage extends FlowPackage {
    val commandName = "packageCampaignFlow"
    val flowPath = "campaignflow"
  }

  object ExportFlowPackage extends FlowPackage {
    val commandName = "packageExportFlow"
    val flowPath = "exportflow"
  }

  object MeasurementFlowPackage extends FlowPackage {
    val commandName = "packageMeasurementFlow"
    val flowPath = "measurementflow"
  }

  object RedshiftFlowPackage extends FlowPackage {
    val commandName = "packageRedshiftFlow"
    val flowPath = "redshiftflow"
  }

  object SchemaMigrationFlowPackage extends FlowPackage {
    val commandName = "packageSchemaMigrationFlow"
    val flowPath = "schemamigration"
  }

  object LookAlikeFlowPackage extends FlowPackage {
    val commandName = "packageLookAlikeFlow"
    val flowPath = "lookalikeflow"
  }

  object MerchantFlowPackage extends FlowPackage {
    val commandName = "packageMerchantFlow"
    val flowPath = "merchantflow"
  }

  object ChannelFlowPackage extends FlowPackage {
    val commandName = "packageChannelFlow"
    val flowPath = "channelflow"
  }

  object AnalysisFlowPackage extends FlowPackage {
    val commandName = "packageAnalysisFlow"
    val flowPath = "analysisflow"
  }

  object BackFillPackage extends FlowPackage {
    val commandName = "packageBackFillFlow"
    val flowPath = "backfill"
  }

  object DevicePushFlow extends FlowPackage {
    val commandName = "packageDevicePushFlow"
    val flowPath = "devicepushflow"
  }

  import AzkabanBinPackage._
  import AirflowBinPackage._

  /**
    * SBT Command: packageFeatures.
    */
  lazy val packageFeatures: Seq[Setting[_]] =
    addCommandAlias("packageFeaturesStaging", "; packageAzkabanBin staging ; packageRawFlow staging ; " +
      "packageSnapshotFlow staging ; packageExportFlow staging ; packageCampaignFlow staging ; " +
      "packageRedshiftFlow staging ; packageMeasurementFlow staging ; packageSchemaMigrationFlow staging ; " +
      "packageLookAlikeFlow staging; packageModelFlow staging ; packageMerchantFlow staging ; packageChannelFlow staging ; packageAnalysisFlow staging ; packageDevicePushFlow staging") ++
    addCommandAlias("packageFeaturesProd", "; packageAzkabanBin prod ; packageRawFlow prod ; " +
      "packageSnapshotFlow prod ; packageExportFlow prod ; packageCampaignFlow prod; packageMeasurementFlow prod ; " +
      "packageRedshiftFlow prod ; packageSchemaMigrationFlow prod ; packageLookAlikeFlow prod; packageModelFlow prod; " +
      "packageMerchantFlow prod ; packageChannelFlow prod ; packageAnalysisFlow prod ; packageDevicePushFlow prod")

  lazy val packageSettings: Seq[Setting[_]] =
    Seq(commands ++= Seq(packageAzkabanBinCommand, RawFlowPackage.packageFlow, SnapshotFlowPackage.packageFlow,
      ExportFlowPackage.packageFlow, CampaignFlowPackage.packageFlow, packageAirflowBinCommand,
      AirflowPackage.packageFlow, MeasurementFlowPackage.packageFlow, RedshiftFlowPackage.packageFlow,
      SchemaMigrationFlowPackage.packageFlow, LookAlikeFlowPackage.packageFlow, ModelFlowPackage.packageFlow, 
      MerchantFlowPackage.packageFlow, ChannelFlowPackage.packageFlow, AnalysisFlowPackage.packageFlow, BackFillPackage.packageFlow, DevicePushFlow.packageFlow)) ++ packageFeatures
}


