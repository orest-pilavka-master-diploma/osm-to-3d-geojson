package com.students.lnu

import com.students.lnu.common.spark.{SparkBuilder, SparkConfig}
import org.apache.spark.sql.SparkSession
import pureconfig.generic.auto._

object Main extends WorkflowManager {
  case class MainConfig(sparkConfig: SparkConfig)

  def main(args: Array[String]): Unit = {
    val config                              = AppConfig.readWithDefault[MainConfig](args.headOption)
    implicit val sparkSession: SparkSession = SparkBuilder.build(config.sparkConfig)

    val workflows = buildWorkflows(args.headOption)
    run(workflows)
  }

}
