package com.students.lnu.model.building

import com.students.lnu.model.admin.AdminPlaceRef
import com.students.lnu.model.common.wrapper.MultiPolygonWrapper

case class Building(
    id: Long,
    originalId: String,
    geometry: MultiPolygonWrapper,
    tags: Map[String, String] = Map.empty,
    adminPlaces: List[AdminPlaceRef] = List.empty
)
