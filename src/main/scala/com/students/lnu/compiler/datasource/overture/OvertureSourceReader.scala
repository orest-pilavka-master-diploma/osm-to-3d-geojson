package com.students.lnu.compiler.datasource.overture

import com.students.lnu.common.{Processor, SharedProcessorData}
import com.students.lnu.compiler.building.BuildingTable
import com.students.lnu.compiler.datasource.overture.OvertureSourceReader.{optionalColumn, parseGeometry, hasPath}
import com.students.lnu.model.building.OvertureBuilding
import com.students.lnu.model.common.wrapper.MultiPolygonWrapper
import com.students.lnu.{BoundingBoxConfig, CommonConfig, OvertureConfig}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions.{col, lit, max, min}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.WKBReader

import scala.util.Try

/**
  * Reads Overture Maps buildings (GeoParquet, WKB geometry) into [[OvertureBuildingTable]].
  *
  * Must run after the OSM building geometries are collected: the extent of the OSM
  * buildings is used to restrict the (usually much larger) Overture dataset to the
  * area of interest, unless an explicit bbox is configured.
  */
class OvertureSourceReader(commonConfig: CommonConfig, config: OvertureConfig) extends Processor with StrictLogging {

  override def process(data: SharedProcessorData)(implicit spark: SparkSession): SharedProcessorData = {
    import spark.implicits._

    val raw = spark.read.parquet(config.overtureInPath)

    val bbox = config.bbox.orElse(osmExtent(data(BuildingTable)))
    val spatiallyFiltered = bbox match {
      case Some(box) if hasPath(raw.schema, "bbox.xmin") =>
        logger.info(s"Reading Overture buildings inside $box")
        raw.where(
          col("bbox.xmax") >= box.minLon && col("bbox.xmin") <= box.maxLon &&
            col("bbox.ymax") >= box.minLat && col("bbox.ymin") <= box.maxLat
        )
      case _ =>
        logger.warn("No bbox filter applied to Overture buildings (no extent or no bbox column).")
        raw
    }

    val flat: Dataset[OvertureRawBuilding] = spatiallyFiltered
      .select(
        col("id").cast(StringType).as("id"),
        col("geometry").cast(BinaryType).as("geometry"),
        optionalColumn(raw, "height", DoubleType, "height"),
        optionalColumn(raw, "num_floors", IntegerType, "numFloors"),
        optionalColumn(raw, "class", StringType, "buildingClass"),
        optionalColumn(raw, "subtype", StringType, "subtype"),
        optionalColumn(raw, "names.primary", StringType, "name"),
        optionalColumn(raw, "roof_shape", StringType, "roofShape"),
        optionalColumn(raw, "facade_material", StringType, "facadeMaterial")
      )
      .as[OvertureRawBuilding]

    val buildings: Dataset[OvertureBuilding] = flat.flatMap { row =>
      parseGeometry(row.geometry).map { geometry =>
        OvertureBuilding(
          overtureId = row.id,
          geometry = geometry,
          height = row.height.filter(h => !h.isNaN && h > 0),
          numFloors = row.numFloors.filter(_ > 0),
          buildingClass = row.buildingClass,
          subtype = row.subtype,
          name = row.name,
          roofShape = row.roofShape,
          facadeMaterial = row.facadeMaterial
        )
      }
    }

    data + (OvertureBuildingTable -> buildings.persist(commonConfig.sparkStorageLevel))
  }

  private def osmExtent(buildings: Dataset[com.students.lnu.model.building.Building])(
      implicit spark: SparkSession): Option[BoundingBoxConfig] = {
    import spark.implicits._
    val padding = 0.001
    val row = buildings
      .map { b =>
        val e = b.geometry.toJTS.getEnvelopeInternal
        (e.getMinX, e.getMinY, e.getMaxX, e.getMaxY)
      }
      .toDF("minX", "minY", "maxX", "maxY")
      .agg(min("minX"), min("minY"), max("maxX"), max("maxY"))
      .head()
    if (row.isNullAt(0)) None
    else Some(BoundingBoxConfig(row.getDouble(0) - padding, row.getDouble(1) - padding, row.getDouble(2) + padding, row.getDouble(3) + padding))
  }
}

object OvertureSourceReader {

  def apply(commonConfig: CommonConfig, config: OvertureConfig): OvertureSourceReader =
    new OvertureSourceReader(commonConfig, config)

  /** True if the (possibly nested, dot separated) field exists in the schema. */
  def hasPath(schema: StructType, path: String): Boolean = {
    def loop(dataType: DataType, parts: List[String]): Boolean = (dataType, parts) match {
      case (_, Nil) => true
      case (struct: StructType, head :: tail) =>
        struct.fields.find(_.name == head).exists(f => loop(f.dataType, tail))
      case _ => false
    }
    loop(schema, path.split('.').toList)
  }

  /** Selects the column if it is present, otherwise a typed null, so older/newer Overture schemas both load. */
  def optionalColumn(df: DataFrame, path: String, dataType: DataType, alias: String): Column = {
    val column: Column = if (hasPath(df.schema, path)) col(path) else lit(null)
    column.cast(dataType).as(alias)
  }

  /** Decodes WKB into a valid, non-empty (multi)polygon; anything else is skipped. */
  def parseGeometry(wkb: Array[Byte]): Option[MultiPolygonWrapper] = {
    if (wkb == null || wkb.isEmpty) None
    else {
      Try(new WKBReader(new GeometryFactory()).read(wkb)).toOption
        .filter(g => !g.isEmpty && g.isValid)
        .flatMap(g => MultiPolygonWrapper.fromJTS(g).toOption)
        .filter(_.nonEmpty)
    }
  }
}
