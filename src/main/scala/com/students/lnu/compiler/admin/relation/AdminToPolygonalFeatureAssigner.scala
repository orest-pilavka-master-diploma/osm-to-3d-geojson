package com.students.lnu.compiler.admin.relation

import com.students.lnu.compiler.geometry.multiPolygonToWktUdf
import com.students.lnu.model.common.feature.FeatureRef
import com.students.lnu.model.common.wrapper.MultiPolygonWrapper
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

object AdminToPolygonalFeatureAssigner extends AdminToFeatureAssigner[MultiPolygonWrapper] {
  override protected def provideFeaturesWithGeometries(features: Dataset[FeatureRef[MultiPolygonWrapper]])(
      implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.udf.register("areaToWktPolygon", multiPolygonToWktUdf)

    features
      .select(
        $"id" as "featureId",
        expr("ST_GeomFromWKT(areaToWktPolygon(geometry))") as "featureGeometry"
      )
  }

  override protected val featureToAdminJoinExpr: Column = expr("ST_Area(ST_Intersection(polygon, featureGeometry)) > 0.5 * ST_Area(featureGeometry)")
}
