package com.students.lnu.common.spark

import com.typesafe.scalalogging.StrictLogging
import org.apache.sedona.spark.SedonaContext
import org.apache.spark.sql.SparkSession

class SparkLocalBuilder(config: SparkConfig) extends SparkBuilder with StrictLogging {
  def build(implicit kryoClassRegister: KryoClassRegister = FallbackKryoClassRegister): SparkSession = {
    logger.info("Running Spark job in local mode...")

    val sparkBuilder = createSparkSession(config)
      .master(config.master)
      .config("spark.local.dir", "/tmp/spark.local.dir/")
      .config("spark.checkpoint.dir", "checkpoints")

    val spark = sparkBuilder.getOrCreate()
    SedonaContext.create(spark)
  }
}
