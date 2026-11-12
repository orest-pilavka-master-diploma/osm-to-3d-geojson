package com.students.lnu.ml

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.DataFrame

/**
 * MLlib RandomForest pipeline that predicts a building's number of floors
 * (`building:levels`) from the features produced by [[BuildingFeatureExtractor]].
 *
 * The pipeline indexes + one-hot encodes the categorical `buildingType`,
 * assembles it together with the numeric features into a single `features`
 * vector, and fits a [[RandomForestRegressor]] against the `levels` label.
 */
object BuildingLevelsModel extends StrictLogging {

  /** Numeric feature columns fed straight into the assembler. */
  private val NumericFeatureColumns: Array[String] = Array(
    "area",
    "perimeter",
    "compactness",
    "lengthWidthRatio",
    "rectangularity",
    "numVertices",
    "hasName",
    "neighborCount",
    "avgNeighborLevels",
    "neighborLevelsKnownCount",
    "builtDensity",
    "avgNeighborArea"
  )

  /** Assembler input columns in order: numeric features + the one-hot vector. */
  private val AssemblerInputColumns: Array[String] =
    NumericFeatureColumns :+ "buildingTypeVec"

  /** Builds the untrained StringIndexer -> OneHotEncoder -> VectorAssembler -> RF pipeline. */
  def buildPipeline(cfg: BuildingLevelsModelConfig): Pipeline = {
    val indexer = new StringIndexer()
      .setInputCol("buildingType")
      .setOutputCol("buildingTypeIdx")
      .setHandleInvalid("keep")

    val encoder = new OneHotEncoder()
      .setInputCol("buildingTypeIdx")
      .setOutputCol("buildingTypeVec")

    val assembler = new VectorAssembler()
      .setInputCols(AssemblerInputColumns)
      .setOutputCol("features")

    val regressor = new RandomForestRegressor()
      .setLabelCol("levels")
      .setFeaturesCol("features")
      .setNumTrees(cfg.numTrees)
      .setMaxDepth(cfg.maxDepth)
      .setSeed(cfg.seed)

    new Pipeline().setStages(Array(indexer, encoder, assembler, regressor))
  }

  /**
   * Trains the pipeline on the labeled rows of `featuresDF`, logs RMSE/MAE/R2 on
   * a held-out split along with the feature importances, saves the model to
   * `cfg.modelPath` (overwriting), and returns the fitted model.
   */
  def train(featuresDF: DataFrame, cfg: BuildingLevelsModelConfig): PipelineModel = {
    val labeled = featuresDF.filter(featuresDF("levels").isNotNull)

    val Array(trainDF, testDF) =
      labeled.randomSplit(Array(cfg.trainRatio, 1.0 - cfg.trainRatio), cfg.seed)

    val model      = buildPipeline(cfg).fit(trainDF)
    val predictions = model.transform(testDF)

    logMetrics(predictions)
    logFeatureImportances(model)

    model.write.overwrite().save(cfg.modelPath)
    logger.info(s"Saved building-levels model to ${cfg.modelPath}")

    model
  }

  /** Loads a previously trained pipeline model from `cfg.modelPath`. */
  def load(cfg: BuildingLevelsModelConfig): PipelineModel =
    PipelineModel.load(cfg.modelPath)

  /** Applies the model, yielding the input columns plus a `prediction` column. */
  def predict(model: PipelineModel, featuresDF: DataFrame): DataFrame =
    model.transform(featuresDF)

  /** Logs RMSE, MAE and R2 of the predictions against the `levels` label. */
  private def logMetrics(predictions: DataFrame): Unit = {
    Seq("rmse", "mae", "r2").foreach { metric =>
      val evaluator = new RegressionEvaluator()
        .setLabelCol("levels")
        .setPredictionCol("prediction")
        .setMetricName(metric)
      val value = evaluator.evaluate(predictions)
      logger.info(s"Evaluation ${metric.toUpperCase} = $value")
    }
  }

  /** Maps the fitted RF feature importances to the assembler input names and logs them. */
  private def logFeatureImportances(model: PipelineModel): Unit = {
    val rfModel = model.stages
      .collectFirst { case rf: RandomForestRegressionModel => rf }
      .getOrElse(throw new IllegalStateException("No RandomForestRegressionModel in pipeline stages"))

    val importances = rfModel.featureImportances.toArray

    // The assembler flattens buildingTypeVec into several vector slots, so the
    // importance array is longer than AssemblerInputColumns. We name each named
    // input column and group the remaining one-hot slots under buildingTypeVec.
    val namedCount = NumericFeatureColumns.length
    val numeric    = NumericFeatureColumns.zip(importances.take(namedCount))
    val categorical =
      if (importances.length > namedCount)
        Seq("buildingTypeVec" -> importances.drop(namedCount).sum)
      else Seq.empty

    val ranked = (numeric ++ categorical).sortBy(-_._2)

    logger.info("Feature importances (desc):")
    ranked.foreach { case (name, importance) =>
      logger.info(f"  $name%-26s $importance%.6f")
    }
  }
}
