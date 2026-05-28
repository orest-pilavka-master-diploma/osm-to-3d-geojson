package com.students.lnu.common.spark

import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

trait SparkBuilder {

  def build(implicit kryoClassRegister: KryoClassRegister = FallbackKryoClassRegister): SparkSession

  protected def createSparkSession(config: SparkConfig)(
      implicit kryoClassRegister: KryoClassRegister = FallbackKryoClassRegister): SparkSession.Builder =
    SparkSession
      .builder()
      .appName(config.appName)
      .config(createCustomKryoConfig)
      .config("spark.sql.autoBroadcastJoinThreshold", 1000000)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
  //      .config("spark.kryo.registrationRequired", "true")

  private def createCustomKryoConfig(implicit kryoClassRegister: KryoClassRegister = FallbackKryoClassRegister) = {
    val conf = new SparkConf()
    conf.registerKryoClasses(kryoClassRegister.classes)
  }

}

object SparkBuilder {
  def build(config: SparkConfig)(implicit kryoClassRegister: KryoClassRegister = FallbackKryoClassRegister): SparkSession = {
    new SparkLocalBuilder(config).build()
  }
}
