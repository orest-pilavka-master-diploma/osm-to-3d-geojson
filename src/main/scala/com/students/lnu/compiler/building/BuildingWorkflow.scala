package com.students.lnu.compiler.building

import com.students.lnu.AppConfig
import com.students.lnu.common.{Processor, Workflow, WorkflowProvisioner}
import com.students.lnu.compiler.admin.{AdminHierarchyReader, AdminPlaceService}
import com.students.lnu.compiler.datasource.osm.OsmSourceReader
import pureconfig.generic.auto._

class BuildingWorkflow(appConfig: BuildingWorkflowConfig, env: BuildingEnvironment) extends Workflow {
  val processors: Seq[Processor] = Seq(
    OsmSourceReader(appConfig.common, appConfig.osm),
    AdminHierarchyReader(appConfig.adminPlace),
    BuildingGeometryProcessor(appConfig.common),
    BuildingAdminProcessor(env),
    BuildingAddressProcessor(),
    BuildingHeightProcessor(),
    BuildingModelProcessor()
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
