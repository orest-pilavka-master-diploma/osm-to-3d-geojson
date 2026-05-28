package com.students.lnu.geojson

import au.id.jazzy.play.geojson._
import org.locationtech.jts.geom.{
  Coordinate,
  Geometry => JtsGeometry,
  LineString => JtsLineString,
  MultiLineString => JtsMultiLineString,
  MultiPoint => JtsMultiPoint,
  MultiPolygon => JtsMultiPolygon,
  Point => JtsPoint,
  Polygon => JtsPolygon
}

object GeoJsonImplicits {

  implicit class JtsGeometryGeometry(geo: JtsGeometry) {
    def toGeometry: Geometry[LngLat] = geo match {
      case point: JtsPoint                     => point.toPlayPoint
      case multiPoint: JtsMultiPoint           => multiPoint.toPlayMultiPoint
      case lineString: JtsLineString           => lineString.toPlayLineString
      case multiLineString: JtsMultiLineString => multiLineString.toPlayMultiLineString
      case polygon: JtsPolygon                 => polygon.toPlayPolygon
      case multiPolygon: JtsMultiPolygon       => multiPolygon.toPlayMultiPolygon
      case _                                   => throw new IllegalStateException("Unsupported geometry type")
    }

    private def convertCoordinates(coordinates: Array[Coordinate]): Seq[LngLat] =
      coordinates.map(coord => LngLat(coord.x, coord.y)).toList

    private implicit class RichJtsPoint(point: JtsPoint) {
      def toPlayPoint: Point[LngLat] = Point(LngLat(point.getX, point.getY))
    }

    private implicit class RichJtsMultiPoint(multiPoint: JtsMultiPoint) {
      def toPlayMultiPoint: MultiPoint[LngLat] =
        MultiPoint(convertCoordinates(multiPoint.getCoordinates).asInstanceOf[scala.collection.immutable.Seq[LngLat]])
    }

    private implicit class RichJtsLineString(lineString: JtsLineString) {
      def toPlayLineString: LineString[LngLat] =
        LineString(convertCoordinates(lineString.getCoordinates).asInstanceOf[scala.collection.immutable.Seq[LngLat]])
    }

    private implicit class RichJtsMultiLineString(multiLineString: JtsMultiLineString) {
      def toPlayMultiLineString: MultiLineString[LngLat] = {
        val lineStrings = (0 until multiLineString.getNumGeometries).map { i =>
          convertCoordinates(multiLineString.getGeometryN(i).asInstanceOf[JtsLineString].getCoordinates)
        }
        MultiLineString(lineStrings.asInstanceOf[scala.collection.immutable.Seq[scala.collection.immutable.Seq[LngLat]]])
      }
    }

    private implicit class RichJtsPolygon(polygon: JtsPolygon) {
      def toPlayPolygon: Polygon[LngLat] = {
        val exteriorRing = convertCoordinates(polygon.getExteriorRing.getCoordinates)
        val interiorRings = (0 until polygon.getNumInteriorRing).map { i =>
          convertCoordinates(polygon.getInteriorRingN(i).getCoordinates)
        }.toList
        Polygon((Seq(exteriorRing) ++ interiorRings).asInstanceOf[scala.collection.immutable.Seq[scala.collection.immutable.Seq[LngLat]]])
      }
    }

    private implicit class RichJtsMultiPolygon(multiPolygon: JtsMultiPolygon) {
      def toPlayMultiPolygon: MultiPolygon[LngLat] = {
        val polygons = (0 until multiPolygon.getNumGeometries).map { i =>
          multiPolygon.getGeometryN(i).asInstanceOf[JtsPolygon].toPlayPolygon.coordinates
        }
        MultiPolygon(polygons)
      }
    }
  }
}
