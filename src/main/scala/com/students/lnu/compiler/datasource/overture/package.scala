package com.students.lnu.compiler.datasource

import com.students.lnu.common.SourceType
import com.students.lnu.model.building.OvertureBuilding

package object overture {

  /** Flat projection of an Overture `building` parquet row; geometry is WKB. */
  case class OvertureRawBuilding(
      id: String,
      geometry: Array[Byte],
      height: Option[Double],
      numFloors: Option[Int],
      buildingClass: Option[String],
      subtype: Option[String],
      name: Option[String],
      roofShape: Option[String],
      facadeMaterial: Option[String]
  )

  case object OvertureBuildingTable extends SourceType[OvertureBuilding]
}
