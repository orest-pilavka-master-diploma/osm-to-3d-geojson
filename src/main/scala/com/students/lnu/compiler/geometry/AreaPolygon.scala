package com.students.lnu.compiler.geometry

import com.students.lnu.model.common.wrapper.MultiPolygonWrapper


case class AreaPolygon(areaId: Long,
                       area: MultiPolygonWrapper, /*MultiPolygonUDT*/
                       tags: Map[String, String],
                       derivedFrom: String) {
  def isEmptyGeometry: Boolean = area.isEmpty
  def nonEmptyGeometry: Boolean = area.nonEmpty
}