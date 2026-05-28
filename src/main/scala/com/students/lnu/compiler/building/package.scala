package com.students.lnu.compiler

import com.students.lnu.common.SourceType
import com.students.lnu.model.building.{Building, Building3DModel}
import com.students.lnu.{AdminPlaceConfig, CommonConfig, OsmConfig}

package object building {
  case class BuildingWorkflowConfig(common: CommonConfig, osm: OsmConfig, adminPlace: AdminPlaceConfig)

  case object BuildingTable       extends SourceType[Building]
  case object Building3DModelTable extends SourceType[Building3DModel]
}
