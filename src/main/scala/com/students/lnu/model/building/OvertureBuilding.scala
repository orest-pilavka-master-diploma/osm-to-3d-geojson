package com.students.lnu.model.building

import com.students.lnu.model.common.wrapper.MultiPolygonWrapper

/**
  * A building from the Overture Maps `buildings` theme (type `building`),
  * reduced to the attributes used for merging with OSM.
  */
case class OvertureBuilding(
    overtureId: String,
    geometry: MultiPolygonWrapper,
    height: Option[Double] = None,
    numFloors: Option[Int] = None,
    buildingClass: Option[String] = None,
    subtype: Option[String] = None,
    name: Option[String] = None,
    roofShape: Option[String] = None,
    facadeMaterial: Option[String] = None
)
