package com.students.lnu.compiler.admin.relation

import com.students.lnu.compiler.admin.FeatureRefAdminPlace
import com.students.lnu.compiler.geometry.multiPolygonToWktUdf
import com.students.lnu.model.admin.AdminPlaceGround
import com.students.lnu.model.common.feature.FeatureRef
import org.apache.spark.sql.functions.{collect_list, expr, struct}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

trait AdminToFeatureAssigner[T] {
  def assign(groundPlaces: Dataset[AdminPlaceGround], features: Dataset[FeatureRef[T]])(
      implicit spark: SparkSession): Dataset[FeatureRefAdminPlace] = {
    import spark.implicits._

    val groundAdminAreas     = provideGroundAdminAreas(groundPlaces)
    val featuresWithGeometry = provideFeaturesWithGeometries(features)

    val featureToAdmin =
      groundAdminAreas
        .join(featuresWithGeometry, featureToAdminJoinExpr, "inner")
        .select(
          $"featureId",
          $"adminPlaceId",
          $"areaId",
          $"isoCountryCode",
          $"isoSubCountryCode",
          $"adminLevel",
          $"area"
        )

    val featureRefAdminPlace = featureToAdmin
      .groupBy("featureId")
      .agg(
        collect_list(
          struct("adminPlaceId", "areaId", "isoCountryCode", "isoSubCountryCode", "adminLevel", "area")
        ) as "adminPlaces"
      )
      .as[FeatureRefAdminPlace]

    featureRefAdminPlace
  }

  protected def provideGroundAdminAreas(groundPlaces: Dataset[AdminPlaceGround])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.udf.register("areaToWktPolygon", multiPolygonToWktUdf)

    groundPlaces.select(
      $"adminPlaceId",
      $"areaId",
      $"isoCountryCode",
      $"isoSubCountryCode",
      $"adminLevel",
      $"area",
      expr("ST_GeomFromWKT(areaToWktPolygon(area))").as("polygon")
    )
  }

  protected def provideFeaturesWithGeometries(features: Dataset[FeatureRef[T]])(implicit spark: SparkSession): DataFrame
  protected val featureToAdminJoinExpr: Column
}
