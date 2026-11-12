package com.students.lnu.compiler.building

import com.students.lnu.CommonConfig
import com.students.lnu.common.{Geohash, Processor, SharedProcessorData}
import com.students.lnu.compiler.building.BuildingModelProcessor.buildingToBuilding3DModel
import com.students.lnu.model.building.{Building, Building3DModel}
import com.students.lnu.model.common.wrapper.MultiPolygonWrapper
import org.apache.spark.sql.SparkSession

class BuildingModelProcessor(config: CommonConfig) extends Processor() {
  override def process(data: SharedProcessorData)(implicit spark: SparkSession): SharedProcessorData = {
    import spark.implicits._

    val geohashPrecision = config.geohashPrecision

    val buildings       = data(BuildingTable)
    val building3DModel = buildings.map(buildingToBuilding3DModel(_, geohashPrecision))

    data + (Building3DModelTable -> building3DModel)
  }
}

object BuildingModelProcessor {
  def apply(config: CommonConfig): BuildingModelProcessor = new BuildingModelProcessor(config)

  def buildingToBuilding3DModel(building: Building, geohashPrecision: Int): Building3DModel = {
    Building3DModel(
      id = building.id,
      originalId = building.originalId,
      geometry = building.geometry,
      addressString = building.tags.getOrElse(BuildingAddressProcessor.tagCity, "") + ", " + building.tags
        .getOrElse(BuildingAddressProcessor.tagStreet, "") + ", " + building.tags.getOrElse(BuildingAddressProcessor.tagHouseNumber, ""),
      height = building.tags(BuildingHeightProcessor.heightTag).toInt,
      sourceOfHeight = building.tags(BuildingHeightProcessor.levelsSourceTag),
      geohash = geohashOf(building.geometry, geohashPrecision),
      dataSource = building.tags.getOrElse(BuildingSourceTags.sourceTag, BuildingSourceTags.Osm)
    )
  }

  /**
    * Deterministically computes a geohash for a building geometry, anchored on the
    * most left (minimum longitude) and most top (maximum latitude) coordinate of all
    * rings in the geometry. Using a fixed anchor point keeps the value stable across runs.
    */
  private def geohashOf(geometry: MultiPolygonWrapper, precision: Int): String = {
    val coordinates = geometry.polygons.flatMap(polygon => polygon.outer ++ polygon.holes.flatten)

    if (coordinates.isEmpty) {
      ""
    } else {
      val minLongitude = coordinates.map(_.longitude).min
      val maxLatitude  = coordinates.map(_.latitude).max
      Geohash.encode(maxLatitude, minLongitude, precision)
    }
  }
}
