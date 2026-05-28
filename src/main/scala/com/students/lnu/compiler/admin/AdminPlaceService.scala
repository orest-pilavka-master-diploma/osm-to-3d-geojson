package com.students.lnu.compiler.admin

import com.github.tototoshi.csv.CSVReader
import com.students.lnu.AdminPlaceConfig
import com.students.lnu.model.admin.{AdminPlaceGround, AdminPlaceRef}
import com.students.lnu.tools.FileUtils.toSource

import scala.util.{Failure, Success, Using}

trait AdminPlaceService {
  val adminPlaceService: AdminPlaceService.Service
}

object AdminPlaceService {
  trait Service extends Serializable {
    def resolveFeatureAdminPlace(adminPlaces: List[AdminPlaceGround]): Option[AdminPlaceRef]
    def resolveFeatureAdminPlaces(adminPlaces: List[AdminPlaceGround]): List[AdminPlaceRef]
  }

  def apply(adminPlaceConfig: AdminPlaceConfig): AdminPlaceService = new AdminPlaceService() {
    override val adminPlaceService: Service = AdminPlaceServiceImpl(adminPlaceConfig)
  }
}

class AdminPlaceServiceImpl(val mapping: Map[Set[String], DisputedView]) extends AdminPlaceService.Service {

  override def resolveFeatureAdminPlace(adminPlaces: List[AdminPlaceGround]): Option[AdminPlaceRef] = {
    if (adminPlaces.nonEmpty) {
      val countriesToAdmin = adminPlaces.groupBy(_.isoCountryCode)

      mapping
        .get(countriesToAdmin.keySet)
        .flatMap(dv => toLowestAdminPlace(countriesToAdmin(dv.isoCountryCode)))
        .orElse(toLowestAdminPlace(countriesToAdmin.values.head))
        .map(apg => AdminPlaceRef(apg.adminPlaceId, apg.adminLevel))
    } else {
      None
    }
  }

  override def resolveFeatureAdminPlaces(adminPlaces: List[AdminPlaceGround]): List[AdminPlaceRef] = {
    if (adminPlaces.isEmpty) {
      return List.empty
    }
    val countriesToAdmin = adminPlaces.groupBy(_.isoCountryCode)
    mapping
      .get(countriesToAdmin.keySet)
      .flatMap(dv => countriesToAdmin.get(dv.isoCountryCode))
      .getOrElse(adminPlaces)
      .map(apg => AdminPlaceRef(apg.adminPlaceId, apg.adminLevel))
  }

  private def toLowestAdminPlace(adminPlaces: List[AdminPlaceGround]): Option[AdminPlaceGround] = adminPlaces.sortBy(_.adminLevel).reverse.headOption
}

object AdminPlaceServiceImpl {
  def apply(adminPlaceConfig: AdminPlaceConfig): AdminPlaceServiceImpl = {
    val disputedAreas = readConfig(adminPlaceConfig.disputedViewPath).map { line =>
      (line.head.split(";").toSet, DisputedView(line(1)))
    }.toMap

    new AdminPlaceServiceImpl(disputedAreas)
  }

  private def readConfig(path: String): Seq[List[String]] =
    Using(CSVReader.open(toSource(path)))(_.all()) match {
      case Success(lines) => lines
      case Failure(e)     => throw e
    }
}
