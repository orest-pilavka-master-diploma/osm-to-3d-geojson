package com.students.lnu

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import org.apache.spark.storage.StorageLevel
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.auto._

import java.io.{File, FileReader, InputStream, InputStreamReader}
import scala.reflect.ClassTag

case class CommonConfig(
    defaultStorageLevel: String,
    workflows: Option[String] = None,
) {
  val sparkStorageLevel: StorageLevel = StorageLevel.fromString(defaultStorageLevel)
  val enabledWorkflows: Set[String]   = workflows.map(_.split(",")).getOrElse(Array.empty[String]).map(_.trim).filterNot(_.isBlank).toSet
}

case class OsmConfig(osmInPath: String)

case class AdminPlaceConfig(disputedViewPath: String, adminPlaceOut: String, adminPlaceGroundOut: String)

object AppConfig {
  private val applicationConfMissed = new IllegalStateException("the default application.conf doesn't exists")

  def defaultConfig: Config = ConfigSource.defaultApplication.config().getOrElse(throw applicationConfMissed)

  def readConfig(stream: InputStream): Config = ConfigFactory.parseReader(new InputStreamReader(stream), ConfigParseOptions.defaults())

  def readConfig(file: File): Config = ConfigFactory.parseReader(new FileReader(file), ConfigParseOptions.defaults())

  def readWithDefault[A](path: Option[String])(implicit tag: ClassTag[A], reader: ConfigReader[A]): A = {
    val configs = path
      .map(new File(_))
      .map(file => Seq(readConfig(file), defaultConfig))
      .getOrElse(Seq(defaultConfig))

    read(configs)
  }

  def readWithDefault[A](stream: InputStream)(implicit tag: ClassTag[A], reader: ConfigReader[A]): A = {
    val config = readConfig(stream)
    read(Seq(config, defaultConfig))
  }

  def read[A](stream: InputStream)(implicit tag: ClassTag[A], reader: ConfigReader[A]): A = {
    val config = readConfig(stream)
    read(Seq(config))
  }

  def read[A](path: String)(implicit tag: ClassTag[A], reader: ConfigReader[A]): A = {
    val config = readConfig(new File(path))
    read(Seq(config))
  }

  def read[A](configs: Seq[Config])(implicit tag: ClassTag[A], reader: ConfigReader[A]): A = {
    val combinedConfig = ConfigSource.fromConfig {
      configs.reduceLeft[Config] {
        case (acc, config) =>
          acc.withFallback(config)
      }
    }

    combinedConfig.loadOrThrow[A]
  }

  implicit val storageLevelConfigReader: ConfigReader[StorageLevel] =
    ConfigReader.fromStringOpt(level => Some(StorageLevel.fromString(level)))
}
