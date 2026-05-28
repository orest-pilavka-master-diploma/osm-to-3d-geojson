package com.students.lnu.compiler.admin

import com.students.lnu.AdminPlaceConfig
import com.students.lnu.common.{Processor, SharedProcessorData}
import com.students.lnu.model.admin.{AdminHierarchyTable, AdminPlace, AdminPlaceGround, AdminPlaceGroundTable}
import org.apache.spark.sql.SparkSession

case class AdminHierarchyReader(config: AdminPlaceConfig) extends Processor {
  override def process(data: SharedProcessorData)(implicit spark: SparkSession): SharedProcessorData = {
    import spark.implicits._
    val groundAdmins = spark.read
      .parquet(config.adminPlaceGroundOut).as[AdminPlaceGround]

    val adminPlaces = spark.read
      .parquet(config.adminPlaceOut).as[AdminPlace]


    data + (AdminHierarchyTable -> adminPlaces) + (AdminPlaceGroundTable -> groundAdmins)
  }
}
