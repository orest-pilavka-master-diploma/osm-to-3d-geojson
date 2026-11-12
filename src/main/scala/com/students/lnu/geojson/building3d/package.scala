package com.students.lnu.geojson

package object building3d {
  case class GeoJsonBuilding3DWorkflowConfig(
      outPath: String,
      tiledOutput: Boolean = true
  )
}
