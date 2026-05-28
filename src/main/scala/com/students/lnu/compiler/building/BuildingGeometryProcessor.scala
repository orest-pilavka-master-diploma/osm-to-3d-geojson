package com.students.lnu.compiler.building

import com.students.lnu.CommonConfig
import com.students.lnu.common.{Processor, SharedProcessorData}
import com.students.lnu.compiler.building.BuildingGeometryProcessor.{isBuilding, windowSpec}
import com.students.lnu.compiler.createAreaIdUdf
import com.students.lnu.compiler.datasource.osm.{OsmRelation, OsmWay}
import com.students.lnu.compiler.geometry.{AreaPolygonCollector, isValidAreaPolygonUdf}
import com.students.lnu.model.admin.AdminPlaceRef
import com.students.lnu.model.building.Building
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number, typedLit}

class BuildingGeometryProcessor(config: CommonConfig) extends Processor {

  override def process(data: SharedProcessorData)(implicit spark: SparkSession): SharedProcessorData = {
    import spark.implicits._

    val osmBuildings = new AreaPolygonCollector(data, config)
      .collect(isBuilding, isBuilding, withTags = true)
      .where(isValidAreaPolygonUdf(col("area")))
      .select(
        row_number().over(windowSpec).as("id"),
        createAreaIdUdf(col("derivedFrom"), col("areaId")).as("originalId"),
        col("area").as("geometry"),
        col("tags"),
        typedLit[List[AdminPlaceRef]](List.empty[AdminPlaceRef]) as "adminPlaces"
      )
      .as[Building]

    data + (BuildingTable -> osmBuildings)
  }
}

object BuildingGeometryProcessor {
  def apply(config: CommonConfig) = new BuildingGeometryProcessor(config)

  val windowSpec: WindowSpec = Window.orderBy("areaId")

  def isBuilding(way: OsmWay): Boolean           = way.tags.contains("building")
  def isBuilding(relation: OsmRelation): Boolean = relation.tags.contains("building")
}
