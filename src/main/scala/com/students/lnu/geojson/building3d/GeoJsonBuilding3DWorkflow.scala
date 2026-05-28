package com.students.lnu.geojson.building3d

import com.students.lnu.AppConfig
import com.students.lnu.common.{Processor, Workflow, WorkflowProvisioner}
import pureconfig.generic.auto._

class GeoJsonBuilding3DWorkflow(geoJsonBuilding3DWorkflowConfig: GeoJsonBuilding3DWorkflowConfig) extends Workflow {
  override def processors: Seq[Processor] = Seq(
    GeoJsonBuilding3DBuilder(geoJsonBuilding3DWorkflowConfig)
  )

  override val name: String = "geojson-building"
}

object GeoJsonBuilding3DWorkflow extends WorkflowProvisioner[GeoJsonBuilding3DWorkflow] {
  override def provide(appConfigFile: Option[String]): GeoJsonBuilding3DWorkflow = {
    val geoJsonBuilding3DWorkflowConfig: GeoJsonBuilding3DWorkflowConfig = AppConfig.readWithDefault[GeoJsonBuilding3DWorkflowConfig](appConfigFile)

    new GeoJsonBuilding3DWorkflow(geoJsonBuilding3DWorkflowConfig)
  }

  override val workflowName: String = "building"
}

