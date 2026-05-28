package com.students.lnu.common

import com.students.lnu.{AppConfig, CommonConfig}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession
import pureconfig.generic.auto._

import java.time.{Duration, LocalDateTime}

trait Workflow extends StrictLogging {
  def processors: Seq[Processor]

  val name: String = this.getClass.getSimpleName

  def run(tables: SharedProcessorData = SharedProcessorData(tables = Map.empty))(implicit spark: SparkSession): SharedProcessorData = {
    val start = LocalDateTime.now()
    spark.sparkContext.setJobDescription(name)

    val res = processors.foldLeft(tables) {
      case (data, processor) => processor.process(data)
    }

    logger.info(s"${this.getClass.getName} processing time: ${Duration.between(start, LocalDateTime.now()).toSeconds} sec.")
    spark.sparkContext.setJobDescription(s"$name - Done")

    res
  }
}

trait WorkflowProvisioner[T <: Workflow] {
  val workflowName: String

  def apply(configFile: Option[String]): Option[T] = {
    if (isWorkflowEnabled(configFile)) {
      Some(provide(configFile))
    } else {
      None
    }
  }

  def provide(configFile: Option[String]): T

  private def isWorkflowEnabled(arguments: Option[String]): Boolean = {
    val enabledWorkflows = AppConfig.readWithDefault[CommonConfigHolder](arguments).common.enabledWorkflows
    enabledWorkflows.isEmpty || enabledWorkflows(workflowName)
  }

  private case class CommonConfigHolder(common: CommonConfig)
}