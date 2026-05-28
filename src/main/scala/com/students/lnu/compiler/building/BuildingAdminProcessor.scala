package com.students.lnu.compiler.building

import com.students.lnu.common.{Processor, SharedProcessorData}
import com.students.lnu.compiler.admin.relation.AdminToPolygonalFeatureAssigner
import com.students.lnu.model.admin.{AdminPlaceGround, AdminPlaceGroundTable}
import com.students.lnu.model.building.Building
import com.students.lnu.model.common.feature.FeatureRef
import com.students.lnu.model.common.wrapper.MultiPolygonWrapper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{coalesce, typedlit, udf}

class BuildingAdminProcessor(env: BuildingEnvironment) extends Processor {
  private val toFeatureAdminPlacesUdf: UserDefinedFunction =
    udf((adminPlaces: List[AdminPlaceGround]) => env.adminPlaceService.resolveFeatureAdminPlaces(adminPlaces))

  override def process(data: SharedProcessorData)(implicit spark: SparkSession): SharedProcessorData = {
    import spark.implicits._

    val polygonalFeatures = data(BuildingTable)
      .select(
        $"id",
        $"geometry"
      )
      .as[FeatureRef[MultiPolygonWrapper]]

    val areasAdminPlaces = AdminToPolygonalFeatureAssigner.assign(data(AdminPlaceGroundTable), polygonalFeatures)

    val displayAreasWithAdmins = data(BuildingTable)
      .join(areasAdminPlaces.alias("aa"), $"featureId" === $"id", "left")
      .select(
        $"id",
        $"originalId",
        $"geometry",
        $"tags",
        toFeatureAdminPlacesUdf(coalesce($"aa.adminPlaces", typedlit(List.empty[AdminPlaceGround]))) as "adminPlaces"
      )
      .as[Building]

    data + (BuildingTable -> displayAreasWithAdmins)
  }
}

object BuildingAdminProcessor {
  def apply(env: BuildingEnvironment) = new BuildingAdminProcessor(env)
}
