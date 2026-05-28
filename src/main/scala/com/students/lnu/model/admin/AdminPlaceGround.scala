package com.students.lnu.model.admin

import com.students.lnu.model.common.wrapper.MultiPolygonWrapper
import play.api.libs.json.{Format, Json}

case class AdminPlaceGround(adminPlaceId: String,
                            areaId: Long,
                            isoCountryCode: String,
                            isoSubCountryCode: List[String],
                            adminLevel: Int,
                            area: MultiPolygonWrapper)

object AdminPlaceGround {
  implicit val adminPlaceGroundFormat: Format[AdminPlaceGround] = Json.format[AdminPlaceGround]
}
