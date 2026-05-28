package com.students.lnu.compiler.building

import com.students.lnu.common.{Processor, SharedProcessorData}
import com.students.lnu.compiler.building.BuildingAddressProcessor.{getAdminPlaceIdWithAllHierarchyElementsDf, getBuildingWithAdminNames, tagCity, tagHouseNumber, tagStreet}
import com.students.lnu.model.admin.{AdminHierarchyTable, AdminPlace, SimpleAdminPlace}
import com.students.lnu.model.building.Building
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import sttp.client3.{HttpClientSyncBackend, UriContext, basicRequest}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

case class BuildingAddressProcessor() extends Processor {

  override def process(data: SharedProcessorData)(implicit spark: SparkSession): SharedProcessorData = {
    import spark.implicits._

    val buildingsWithAdminItemId = data(BuildingTable).flatMap { building =>
      building.adminPlaces.map { adminPlace =>
        (adminPlace.adminPlaceId, building)
      }
    }.toDF("adminItemId", "building")

    val adminIdWithObject      = getAdminPlaceIdWithAllHierarchyElementsDf(data)
    val buildingWithAdminNames = getBuildingWithAdminNames(buildingsWithAdminItemId, adminIdWithObject)

    val buildingWithAddress = buildingWithAdminNames.flatMap {
      case (building, adminNames) => buildAddress(building, adminNames)
    }

    data + (BuildingTable -> buildingWithAddress)
  }

  private def buildAddress(building: Building, adminNames: Seq[String]): Option[Building] = {
    val street = building.tags.get("addr:street")
    street.map { value =>
      val houseNumber = building.tags.getOrElse("addr:housenumber", "unknown")
      val city        = adminNames.lift(4).getOrElse("unknown")

      building.copy(tags = building.tags ++ Map(tagCity -> city, tagStreet -> value, tagHouseNumber -> houseNumber))
    }
  }
}

object BuildingAddressProcessor {
  val tagCity: String = "3D:city"
  val tagStreet: String = "3D:street"
  val tagHouseNumber: String = "3D:houseNumber"

  private def getBuildingWithAdminNames(buildingsWithAdminItemId: DataFrame, adminIdWithObject: DataFrame)(
      implicit spark: SparkSession): Dataset[(Building, Seq[String])] = {
    import spark.implicits._

    buildingsWithAdminItemId
      .join(adminIdWithObject, Seq("adminItemId"))
      .groupBy("building.id")
      .agg(
        flatten(collect_list("adminPlaces")).as("adminPlaces"),
        first("building").as("building")
      )
      .select("building", "adminPlaces")
      .as[(Building, Seq[SimpleAdminPlace])]
      .map {
        case (building, admins) =>
          val correctAdmins = admins.distinct.sortBy(_.adminLevel).flatMap(_.name)
          (building, correctAdmins)
      }
  }

  private def getAdminPlaceIdWithAllHierarchyElementsDf(data: SharedProcessorData)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val adminPlaces = data(AdminHierarchyTable).map(toSimpleAdminPlace)
    val adminPlacesWithId = adminPlaces
      .map(adminPlace => adminPlace.adminPlaceId -> adminPlace)
      .toDF("adminPlaceId", "adminPlace")

    adminPlaces
      .map(adminPlace => adminPlace.adminPlaceId -> (adminPlace.adminPlaceId +: adminPlace.parents.map(_.adminPlaceId)))
      .toDF("adminItemId", "adminHierarchyIds")
      .withColumn("adminHierarchyId", explode($"adminHierarchyIds"))
      .drop("adminHierarchyIds")
      .join(adminPlacesWithId, adminPlacesWithId("adminPlaceId") === $"adminHierarchyId")
      .select($"adminItemId", $"adminPlace")
      .groupBy("adminItemId")
      .agg(collect_list("adminPlace").as("adminPlaces"))
      .withColumn("adminPlaces", sortByAdminLevel($"adminPlaces"))
  }

  val sortByAdminLevel: UserDefinedFunction = udf((adminPlaces: Seq[SimpleAdminPlace]) => {
    adminPlaces.sortBy(_.adminLevel)
  })

  private def toSimpleAdminPlace(adminPlace: AdminPlace): SimpleAdminPlace = {
    SimpleAdminPlace(
      adminPlaceId = adminPlace.adminPlaceId,
      adminLevel = adminPlace.adminLevel,
      parents = adminPlace.parents,
      name = adminPlace.tags.get("name")
    )
  }
}
