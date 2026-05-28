package com.students.lnu.compiler

import com.students.lnu.model.common.wrapper.MultiPolygonWrapper
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

package object geometry {
  val isValidAreaPolygonUdf: UserDefinedFunction = udf((area: MultiPolygonWrapper) => area.nonEmpty && area.toJTS.isValid)
  val multiPolygonToWktUdf: UserDefinedFunction  = udf((area: MultiPolygonWrapper) => area.toJTS.toText)
}
