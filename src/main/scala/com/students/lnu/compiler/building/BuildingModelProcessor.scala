package com.students.lnu.compiler.building

import com.students.lnu.common.{Processor, SharedProcessorData}
import com.students.lnu.compiler.building.BuildingModelProcessor.buildingToBuilding3DModel
import com.students.lnu.model.building.{Building, Building3DModel}
import org.apache.spark.sql.SparkSession

case class BuildingModelProcessor() extends Processor() {
  override def process(data: SharedProcessorData)(implicit spark: SparkSession): SharedProcessorData = {
    import spark.implicits._

    val buildings       = data(BuildingTable)
    val building3DModel = buildings.map(buildingToBuilding3DModel)

    val temp = building3DModel.collect()

    data + (Building3DModelTable -> building3DModel)
  }
}

object BuildingModelProcessor {
  def buildingToBuilding3DModel(building: Building): Building3DModel = {
    Building3DModel(
      id = building.id,
      originalId = building.originalId,
      geometry = building.geometry,
      addressString = building.tags.getOrElse(BuildingAddressProcessor.tagCity, "") + ", " + building.tags
        .getOrElse(BuildingAddressProcessor.tagStreet, "") + ", " + building.tags.getOrElse(BuildingAddressProcessor.tagHouseNumber, ""),
      height = building.tags(BuildingHeightProcessor.heightTag).toInt
    )
  }
}
