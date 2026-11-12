package com.students.lnu.compiler.building

import com.students.lnu.common.{Processor, SharedProcessorData}
import com.students.lnu.compiler.building.BuildingHeightProcessor.{heightTag, levelsSourceTag, levelsTag}
import com.students.lnu.ml.{BuildingFeatureExtractor, BuildingLevelsModel, BuildingLevelsParser}
import com.students.lnu.ml.BuildingLevelsModelConfig
import com.students.lnu.model.building.Building
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * Computes a `3D:height` for every input building. Buildings that already carry
 * a real `building:levels` OSM tag keep their parsed floor count; buildings
 * without it get their floor count predicted by the building-levels ML model
 * (see [[BuildingLevelsModel]]) when prediction is enabled, or use the configured
 * fallback otherwise. Height is always `levels * 3`.
 *
 * Guarantees: NO building is dropped — every input building survives to the
 * output [[BuildingTable]] exactly once, and labeled buildings are never
 * overwritten with a prediction.
 *
 * Note: this processor ALWAYS loads the model via [[BuildingLevelsModel.load]]
 * and never trains, regardless of `cfg.mode`. The `mode` field is only
 * meaningful for the standalone [[com.students.lnu.ml.BuildingLevelsTrainerApp]].
 */
case class BuildingHeightProcessor(cfg: BuildingLevelsModelConfig) extends Processor with StrictLogging {
  private val buildingLevelsTag = "building:levels"

  private val heightFactor = 3

  override def process(data: SharedProcessorData)(implicit spark: SparkSession): SharedProcessorData = {
    import spark.implicits._

    val buildings: Dataset[Building] = data(BuildingTable)


    val predictions: Map[Long, Double] =
      if (cfg.predictionEnabled) predict(buildings)
      else {
        logger.info(
          s"Building-level prediction is disabled; using fallback levels (${cfg.fallbackLevels}) " +
            "for unlabeled buildings."
        )
        Map.empty
      }

    // Map every building exactly once, keeping labeled levels as-is and applying
    // predictions / fallback to unlabeled ones.
    val result: Dataset[Building] = buildings.map(enrich(_, predictions))

    data + (BuildingTable -> result)
  }

  private def predict(buildings: Dataset[Building])(implicit spark: SparkSession): Map[Long, Double] = {
    val featuresDF = BuildingFeatureExtractor.extract(buildings, cfg)

    Try(BuildingLevelsModel.load(cfg)) match {
      case Success(model) =>
        val predicted = BuildingLevelsModel.predict(model, featuresDF)

        predicted
          .filter(predicted("levels").isNull)
          .select("id", "prediction")
          .collect()
          .map(row => row.getLong(0) -> row.getDouble(1))
          .toMap

      case Failure(ex) =>
        logger.warn(
          s"Could not load building-levels model from '${cfg.modelPath}': ${ex.getMessage}. " +
            s"Falling back to default levels (${cfg.fallbackLevels}) for unlabeled buildings.",
          ex
        )
        Map.empty
    }
  }

  /** Adds height + provenance tags to a single building without dropping it. */
  private def enrich(building: Building, predictions: Map[Long, Double]): Building = {
    val realLevels = BuildingLevelsParser.parse(building.tags.getOrElse(buildingLevelsTag, null))

    val overtureOnly = building.tags.get(BuildingSourceTags.sourceTag).contains(BuildingSourceTags.Overture)

    val (intLevels, source) = realLevels match {
      case Some(levels) =>
        // Labeled building: keep the real parsed levels, never overwrite.
        // Levels filled in from Overture are reported as such.
        (clamp(levels), building.tags.getOrElse(BuildingSourceTags.levelsOriginTag, "osm"))

      case None if overtureOnly =>
        // The ML model is trained on OSM only, so Overture buildings never get a prediction.
        (math.max(1, cfg.fallbackLevels), "fallback")

      case None =>
        predictions.get(building.id) match {
          case Some(pred) => (clamp(pred), "predicted")
          case None       => (math.max(1, cfg.fallbackLevels), "fallback")
        }
    }

    val height = intLevels * heightFactor

    building.copy(
      tags = building.tags ++ Map(
        heightTag       -> height.toString,
        levelsTag       -> intLevels.toString,
        levelsSourceTag -> source
      )
    )
  }

  /** Rounds to the nearest integer floor count and clamps to a minimum of 1. */
  private def clamp(levels: Double): Int = math.max(1, math.round(levels).toInt)
}

object BuildingHeightProcessor {
  val heightTag: String       = "3D:height"
  val levelsTag: String       = "3D:levels"
  val levelsSourceTag: String = "3D:levels:source"
}
