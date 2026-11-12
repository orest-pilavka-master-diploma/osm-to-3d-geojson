package com.students.lnu.compiler.building

import com.students.lnu.AppConfig
import com.students.lnu.common.{Processor, Workflow, WorkflowProvisioner}
import com.students.lnu.compiler.admin.{AdminHierarchyReader, AdminPlaceService}
import com.students.lnu.compiler.datasource.osm.OsmSourceReader
import com.students.lnu.compiler.datasource.overture.OvertureSourceReader
import pureconfig.generic.auto._

class BuildingWorkflow(appConfig: BuildingWorkflowConfig, env: BuildingEnvironment) extends Workflow {

  // Overture reading and OSM/Overture merging only run when `overture.enabled = true`.
  private val overtureProcessors: Seq[Processor] =
    if (appConfig.overture.enabled) {
      Seq(
        OvertureSourceReader(appConfig.common, appConfig.overture),
        BuildingSourceMergeProcessor(appConfig.common, appConfig.overture)
      )
    } else {
      Seq.empty
    }

  val processors: Seq[Processor] = Seq(
    OsmSourceReader(appConfig.common, appConfig.osm),
    AdminHierarchyReader(appConfig.adminPlace),
    BuildingGeometryProcessor(appConfig.common)
  ) ++ overtureProcessors ++ Seq(
    BuildingAdminProcessor(env),
    BuildingAddressProcessor(),
    BuildingHeightProcessor(appConfig.buildingLevelsModel),
    BuildingModelProcessor(appConfig.common)
  )

  override val name: String = "building"
}

object BuildingWorkflow extends WorkflowProvisioner[BuildingWorkflow] {
  override def provide(appConfigFile: Option[String]): BuildingWorkflow = {
    val buildingConfig: BuildingWorkflowConfig = AppConfig.readWithDefault[BuildingWorkflowConfig](appConfigFile)
    val env = new BuildingEnvironment {
      override val adminPlaceService: AdminPlaceService.Service = AdminPlaceService(buildingConfig.adminPlace).adminPlaceService
    }
    new BuildingWorkflow(buildingConfig, env)
  }

  override val workflowName: String = "building"
}
