package com.students.lnu.geojson

import au.id.jazzy.play.geojson.{Feature, FeatureCollection, LngLat}
import au.id.jazzy.play.geojson._
import org.apache.spark.sql.Dataset
import play.api.libs.json.Json

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.LocalDateTime

/**
  * Writes geohash-grouped buildings to disk as one GeoJSON FeatureCollection per
  * geohash cell, laid out in a prefix-nested directory tree under a timestamped
  * version directory.
  *
  * Layout example for geohash `u8e2p1`:
  * {{{
  * <outPath>/v20260507120000/u/u8/u8e/u8e2/u8e2p/u8e2p1.geojson
  * }}}
  *
  * The geohash -> path mapping is kept pure ([[geohashToRelativePath]]) so it can later
  * be reused inside a Spark `foreachPartition` / `partitionBy` without redesign.
  */
object TiledGeoJsonWriter {
  private val geoJsonExtension = ".geojson"

  /**
    * Build the prefix-nested relative path for a geohash, with one directory per
    * geohash character up to the full geohash and a `<geohash>.geojson` leaf file.
    *
    * Pure function: no side effects, deterministic for a given geohash.
    * Example: `u8e2p1` -> `u/u8/u8e/u8e2/u8e2p/u8e2p1.geojson`.
    */
  def geohashToRelativePath(geohash: String): Path = {
    require(geohash.nonEmpty, "geohash must not be empty")

    val dirSegments = (1 until geohash.length).map(end => geohash.substring(0, end))
    val leaf        = geohash + geoJsonExtension

    val segments = dirSegments :+ leaf
    Paths.get(segments.head, segments.tail: _*)
  }

  /** Name of the version directory for the current run, e.g. `v20260507120000`. */
  def versionDirName(now: LocalDateTime = LocalDateTime.now()): String =
    "v" + now.format(FileWriter.formatter)

  /**
    * Write one GeoJSON file per occupied geohash cell.
    *
    * Tiles are written with a distributed `Dataset.foreach` — no `collect` of the
    * geometry-bearing models to the driver. Models are converted to features lazily,
    * inside the per-cell loop, via the supplied `toFeature` function.
    *
    * NOTE: because `foreach` runs on executors, each tile file is written with
    * `java.nio.file.Files` on the node that processes that cell. This matches the
    * project's local[*] usage; on a multi-node cluster the tiles would land on
    * executor-local disks (use a shared/dfs path in that case).
    *
    * @param groupedByGeohash dataset of (geohash, models) pairs; only occupied cells.
    * @param toFeature        converts a single model to a GeoJSON feature (must be serializable).
    * @param outPath          configured base output directory.
    * @param version          version directory name.
    * @tparam A               the per-model type held in each cell (e.g. Building3DModel).
    * @return the version directory path that was written to.
    */
  def write[A](
      groupedByGeohash: Dataset[(String, Seq[A])],
      toFeature: A => Feature[LngLat],
      outPath: String,
      version: String = versionDirName()
  ): Path = {
    val versionDir = Paths.get(outPath, version)
    Files.createDirectories(versionDir)

    // Stable primitives captured by the executor-side closure (serializable).
    val outPathStr  = outPath
    val versionName = version

    groupedByGeohash.foreach { (tile: (String, Seq[A])) =>
      val (geohash, models) = tile
      val features          = models.map(toFeature)

      val tilePath = Paths.get(outPathStr, versionName).resolve(geohashToRelativePath(geohash))
      Option(tilePath.getParent).foreach(Files.createDirectories(_))

      val featureCollection: FeatureCollection[LngLat] = FeatureCollection(features.toList)
      val content: String                              = Json.toJson(featureCollection).toString()

      Files.write(
        tilePath,
        content.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING
      )
      ()
    }

    versionDir
  }
}
