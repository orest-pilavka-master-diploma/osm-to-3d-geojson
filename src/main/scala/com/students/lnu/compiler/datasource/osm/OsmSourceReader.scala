package com.students.lnu.compiler.datasource.osm

import com.students.lnu.{CommonConfig, OsmConfig}
import com.students.lnu.common.{Processor, SharedProcessorData}
import com.students.lnu.compiler.datasource.osm.OsmSourceReader.{toNodeIds, toRelationType, toTagMap}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Polygon}

import scala.io.Source

class OsmSourceReader(commonConfig: CommonConfig, osmConfig: OsmConfig) extends Processor with Serializable {

  private val factory = new GeometryFactory()

  override def process(data: SharedProcessorData)(implicit spark: SparkSession): SharedProcessorData = {
    import spark.implicits._


    val nodes: Dataset[OsmNode] = spark.read
      .parquet(s"${osmConfig.osmInPath}/*osm.pbf.node.parquet")
      .drop("timestamp", "changeset", "uid", "user_sid", "version")
      .withColumnRenamed("id", "nodeId")
      .withColumn("tags", toTagMap(col("tags")))
      .as[OsmNode]
      .persist(commonConfig.sparkStorageLevel)

    val ways: Dataset[OsmWay] =
      spark.read
        .parquet(s"${osmConfig.osmInPath}/*osm.pbf.way.parquet")
        .drop("version", "timestamp", "changeset", "uid", "user_sid")
        .withColumn("nodeIds", toNodeIds(col("nodes")))
        .withColumn("tags", toTagMap(col("tags")))
        .withColumnRenamed("id", "wayId")
        .drop("nodes")
        .as[OsmWay]
        .persist(commonConfig.sparkStorageLevel)

    val relations: Dataset[OsmRelation] = spark.read
      .parquet(s"${osmConfig.osmInPath}/*osm.pbf.relation.parquet")
      .drop("version", "timestamp", "changeset", "uid", "user_sid")
      .withColumn("tags", toTagMap(col("tags")))
      .withColumn("type", toRelationType(col("tags")))
      .withColumnRenamed("id", "relationId")
      .as[OsmRelation]
      .persist(commonConfig.sparkStorageLevel)


    data + (OsmNodeTable -> nodes) + (OsmWayTable -> ways) + (OsmRelationTable -> relations)
  }
}

object OsmSourceReader {
  def apply(commonConfig: CommonConfig, osmConfig: OsmConfig): OsmSourceReader =
    new OsmSourceReader(commonConfig, osmConfig)

  val toNodeIds: UserDefinedFunction = udf((nodes: Array[OsmNodeRef]) => nodes.map(_.nodeId))
  val toTagMap: UserDefinedFunction = udf((tags: Array[OsmTag]) => tags.map(t => t.key -> t.value).toMap)
  val toRelationType: UserDefinedFunction = udf((tags: Map[String, String]) => tags.getOrElse("type", "NA"))
}