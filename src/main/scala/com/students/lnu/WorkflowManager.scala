package com.students.lnu

import com.students.lnu.common.{SharedProcessorData, Workflow}
import com.students.lnu.compiler.building.BuildingWorkflow
import com.students.lnu.geojson.building3d.GeoJsonBuilding3DWorkflow
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

import java.time.{Duration, LocalDateTime}

trait WorkflowManager extends StrictLogging {
  def buildWorkflows(args: Option[String]): Seq[Workflow] = {
    val workflows: Seq[Workflow] = Seq(
      BuildingWorkflow(args),
      GeoJsonBuilding3DWorkflow(args),
    ).flatten

    logger.info(s"Workflows to be executed: ${workflows.map(_.name).mkString(", ")}")

    workflows
  }

  def run(workflows: Seq[Workflow])(implicit spark: SparkSession): Unit = {
    logger.info("Start OSM-TO-3D-GEOJSON job...")
    val start = LocalDateTime.now()

    try {
      workflows.foldLeft(SharedProcessorData(Map.empty)) {
        case (sharedData, workflow) => workflow.run(sharedData)
      }

      logger.info(s"Application processing time: ${Duration.between(start, LocalDateTime.now()).toSeconds} sec.")
    } finally {
      spark.stop()
    }
  }
}
