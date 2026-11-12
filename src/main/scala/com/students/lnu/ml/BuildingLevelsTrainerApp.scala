package com.students.lnu.ml

import com.students.lnu.common.SharedProcessorData
import com.students.lnu.common.spark.{SparkBuilder, SparkConfig}
import com.students.lnu.compiler.building.{BuildingGeometryProcessor, BuildingTable}
import com.students.lnu.compiler.datasource.osm.OsmSourceReader
import com.students.lnu.model.building.Building
import com.students.lnu.{AppConfig, CommonConfig, OsmConfig}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.{Dataset, SparkSession}
import pureconfig.generic.auto._

/**
 * Standalone entry point for the building-levels ML model. It is intentionally
 * kept separate from [[com.students.lnu.Main]]: it does not wire any prediction
 * into the compiler pipeline. It loads a `Dataset[Building]` by reusing the same
 * OSM reader + geometry processor the real workflow uses (so full tags including
 * `building:levels` are preserved), extracts features, and then either trains a
 * fresh model or loads an existing one and evaluates it, depending on config.
 */
object BuildingLevelsTrainerApp extends StrictLogging {

  /** Config for the trainer; mirrors the sections used by [[com.students.lnu.Main]]. */
  case class TrainerConfig(
      sparkConfig: SparkConfig,
      osm: OsmConfig,
      common: CommonConfig,
      buildingLevelsModel: BuildingLevelsModelConfig
  )

  def main(args: Array[String]): Unit = {
    val config                              = AppConfig.readWithDefault[TrainerConfig](args.headOption)
    implicit val sparkSession: SparkSession = SparkBuilder.build(config.sparkConfig)

    if (config.osm.osmInPath == null || config.osm.osmInPath.isBlank || config.osm.osmInPath == "NA") {
      logger.error(
        "TODO: osm.osm-in-path is unset. Point it at the OSM parquet source, e.g. " +
          "data/input/small-lviv-part.osm.pbf.*.parquet (under the repo parent data/input)."
      )
      return
    }

    val buildings  = loadBuildings(config)
    val featuresDF = BuildingFeatureExtractor.extract(buildings, config.buildingLevelsModel)

    config.buildingLevelsModel.mode match {
      case "train" =>
        logger.info("Mode=train: training a new building-levels model.")
        BuildingLevelsModel.train(featuresDF, config.buildingLevelsModel)

      case "load" =>
        logger.info("Mode=load: loading existing model and evaluating.")
        val model     = BuildingLevelsModel.load(config.buildingLevelsModel)
        val predicted = BuildingLevelsModel.predict(model, featuresDF)
        evaluate(predicted)

      case other =>
        logger.error(s"Unknown building-levels-model mode '$other'; expected 'train' or 'load'.")
    }
  }

  /**
   * Builds a `Dataset[Building]` the same way the real BuildingWorkflow does:
   * read OSM parquet, then run the geometry processor over an empty shared data
   * map and read the resulting [[BuildingTable]].
   */
  private def loadBuildings(config: TrainerConfig)(implicit spark: SparkSession): Dataset[Building] = {
    val empty      = SharedProcessorData(Map.empty)
    val withOsm    = OsmSourceReader(config.common, config.osm).process(empty)
    val withBuildings = BuildingGeometryProcessor(config.common).process(withOsm)
    withBuildings(BuildingTable)
  }

  /** Evaluates loaded-model predictions on labeled rows and logs RMSE/MAE/R2. */
  private def evaluate(predicted: org.apache.spark.sql.DataFrame): Unit = {
    val labeled = predicted.filter(predicted("levels").isNotNull)
    Seq("rmse", "mae", "r2").foreach { metric =>
      val evaluator = new RegressionEvaluator()
        .setLabelCol("levels")
        .setPredictionCol("prediction")
        .setMetricName(metric)
      logger.info(s"Evaluation ${metric.toUpperCase} = ${evaluator.evaluate(labeled)}")
    }
  }
}
