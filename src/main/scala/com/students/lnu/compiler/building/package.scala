package com.students.lnu.compiler

import com.students.lnu.common.SourceType
import com.students.lnu.ml.BuildingLevelsModelConfig
import com.students.lnu.model.building.{Building, Building3DModel}
import com.students.lnu.{AdminPlaceConfig, CommonConfig, OsmConfig, OvertureConfig}

package object building {

  case class BuildingWorkflowConfig(
      common: CommonConfig,
      osm: OsmConfig,
      adminPlace: AdminPlaceConfig,
      buildingLevelsModel: BuildingLevelsModelConfig,
      overture: OvertureConfig = OvertureConfig()
  )

  /** Tags that track where a building (and its floor count) came from. */
  object BuildingSourceTags {
    val sourceTag: String       = "3D:source"
    val levelsOriginTag: String = "3D:levels:origin"
    val overtureIdTag: String   = "overture:id"
    val Osm: String             = "osm"
    val Overture: String        = "overture"
    val OsmOverture: String     = "osm+overture"
  }

  case object BuildingTable       extends SourceType[Building]
  case object Building3DModelTable extends SourceType[Building3DModel]
}
