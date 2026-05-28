package com.students.lnu

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

package object compiler {
  val createAreaIdUdf: UserDefinedFunction = udf((derivedFrom: String, areaId: Long) => s"osm-$derivedFrom-$areaId")

}
