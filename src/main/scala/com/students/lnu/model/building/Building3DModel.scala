package com.students.lnu.model.building

import com.students.lnu.model.common.wrapper.MultiPolygonWrapper

case class Building3DModel(
    id: Long,
    originalId: String,
    geometry: MultiPolygonWrapper,
    addressString: String,
    height: Int
)
