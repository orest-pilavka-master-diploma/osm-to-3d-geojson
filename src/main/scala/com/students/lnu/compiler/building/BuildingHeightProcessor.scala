package com.students.lnu.compiler.building

import com.students.lnu.common.{Processor, SharedProcessorData}
import com.students.lnu.compiler.building.BuildingHeightProcessor.heightTag
import com.students.lnu.model.building.Building
import org.apache.spark.sql.{Dataset, SparkSession}

case class BuildingHeightProcessor() extends Processor {
  private val buildingLevelsTag = "building:levels"

  override def process(data: SharedProcessorData)(implicit spark: SparkSession): SharedProcessorData = {
    import spark.implicits._

    val buildingWithAddress: Dataset[Building] = data(BuildingTable)

    val buildingWithHeight = buildingWithAddress
      .filter(_.tags.contains(buildingLevelsTag))
      .map(addHeight)

    data + (BuildingTable -> buildingWithHeight)
  }

  private def addHeight(building: Building): Building = {
    val levels = building.tags(buildingLevelsTag).toInt
    val height = levels * 3

    building.copy(tags = building.tags ++ Map(heightTag -> height.toString))
  }

}

object BuildingHeightProcessor {
  val heightTag: String = "3D:height"
}
