package com.students.lnu.ml

import com.students.lnu.model.common.wrapper.MultiPolygonWrapper
import org.locationtech.jts.geom.{Coordinate, Geometry, LinearRing, MultiPolygon, Polygon}

/**
 * Dependency-free forward projection from WGS84 geographic coordinates
 * (longitude/latitude in degrees) to UTM zone 35N (EPSG:32635) metric
 * easting/northing.
 *
 * The shaded Sedona jar does not bundle GeoTools EPSG referencing classes, so
 * `ST_Transform` is unusable here. This implements the standard transverse
 * Mercator forward formulas directly so that area/perimeter/distance features
 * can be computed in meters instead of degrees.
 */
object MetricCrs {

  // UTM zone 35N parameters.
  private val CentralMeridianDeg = 27.0
  private val K0                 = 0.9996
  private val FalseEasting       = 500000.0
  private val FalseNorthing      = 0.0 // northern hemisphere

  // WGS84 ellipsoid.
  private val A = 6378137.0
  private val F = 1.0 / 298.257223563

  private val E2  = F * (2.0 - F)       // first eccentricity squared
  private val Ep2 = E2 / (1.0 - E2)     // second eccentricity squared

  private val Lambda0 = math.toRadians(CentralMeridianDeg)

  /**
   * Projects a WGS84 lon/lat point to UTM 35N metric (easting, northing).
   */
  def project(lon: Double, lat: Double): (Double, Double) = {
    val phi    = math.toRadians(lat)
    val lambda = math.toRadians(lon)

    val sinPhi = math.sin(phi)
    val cosPhi = math.cos(phi)
    val tanPhi = math.tan(phi)

    val n = A / math.sqrt(1.0 - E2 * sinPhi * sinPhi)
    val t = tanPhi * tanPhi
    val c = Ep2 * cosPhi * cosPhi
    val a = cosPhi * (lambda - Lambda0)

    val m = A * (
      (1.0 - E2 / 4.0 - 3.0 * E2 * E2 / 64.0 - 5.0 * E2 * E2 * E2 / 256.0) * phi
        - (3.0 * E2 / 8.0 + 3.0 * E2 * E2 / 32.0 + 45.0 * E2 * E2 * E2 / 1024.0) * math.sin(2.0 * phi)
        + (15.0 * E2 * E2 / 256.0 + 45.0 * E2 * E2 * E2 / 1024.0) * math.sin(4.0 * phi)
        - (35.0 * E2 * E2 * E2 / 3072.0) * math.sin(6.0 * phi)
    )

    val easting = FalseEasting + K0 * n * (
      a
        + (1.0 - t + c) * math.pow(a, 3) / 6.0
        + (5.0 - 18.0 * t + t * t + 72.0 * c - 58.0 * Ep2) * math.pow(a, 5) / 120.0
    )

    val northing = FalseNorthing + K0 * (
      m + n * tanPhi * (
        a * a / 2.0
          + (5.0 - t + 9.0 * c + 4.0 * c * c) * math.pow(a, 4) / 24.0
          + (61.0 - 58.0 * t + t * t + 600.0 * c - 330.0 * Ep2) * math.pow(a, 6) / 720.0
      )
    )

    (easting, northing)
  }

  /**
   * Returns a new geometry with every coordinate reprojected from WGS84
   * lon/lat (x = longitude, y = latitude) to UTM 35N metric coordinates.
   * Supports Polygon and MultiPolygon; other geometry types are returned
   * unchanged.
   */
  def projectGeometry(g: Geometry): Geometry = g match {
    case polygon: Polygon           => projectPolygon(polygon)
    case multiPolygon: MultiPolygon => projectMultiPolygon(multiPolygon)
    case other                      => other
  }

  private def projectMultiPolygon(multiPolygon: MultiPolygon): MultiPolygon = {
    val polygons = (0 until multiPolygon.getNumGeometries)
      .map(i => projectPolygon(multiPolygon.getGeometryN(i).asInstanceOf[Polygon]))
      .toArray
    MultiPolygonWrapper.geometryFactory.createMultiPolygon(polygons)
  }

  private def projectPolygon(polygon: Polygon): Polygon = {
    val shell = projectRing(polygon.getExteriorRing.getCoordinates)
    val holes = (0 until polygon.getNumInteriorRing)
      .map(i => projectRing(polygon.getInteriorRingN(i).getCoordinates))
      .toArray
    MultiPolygonWrapper.geometryFactory.createPolygon(shell, holes)
  }

  private def projectRing(coordinates: Array[Coordinate]): LinearRing = {
    val projected = coordinates.map { c =>
      val (easting, northing) = project(c.x, c.y)
      new Coordinate(easting, northing)
    }
    MultiPolygonWrapper.geometryFactory.createLinearRing(projected)
  }
}
