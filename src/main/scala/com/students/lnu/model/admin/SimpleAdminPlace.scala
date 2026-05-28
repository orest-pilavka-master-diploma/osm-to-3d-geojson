package com.students.lnu.model.admin

import play.api.libs.json.{Format, Json}

case class SimpleAdminPlace(
    adminPlaceId: String,
    adminLevel: Int,
    parents: List[AdminPlaceRef],
    name: Option[String]
)

object SimpleAdminPlace {
  implicit val simpleAdminPlaceFormat: Format[SimpleAdminPlace] = Json.format[SimpleAdminPlace]
}
