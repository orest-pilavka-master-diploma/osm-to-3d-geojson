package com.students.lnu

package object ml {

  case class BuildingLevelsModelConfig(
      mode: String = "train",
      modelPath: String,
      predictionEnabled: Boolean = true,
      neighborhoodRadiusMeters: Double = 100.0,
      trainRatio: Double = 0.8,
      numTrees: Int = 100,
      maxDepth: Int = 10,
      seed: Long = 42,
      // Floor count assigned to unlabeled buildings when the model cannot be
      // loaded (graceful fallback). Defaulted so existing callers / configs
      // (e.g. the standalone trainer) stay backward compatible.
      fallbackLevels: Int = 1,
  )
}
