package com.students.lnu.model

import com.students.lnu.common.SourceType

package object admin {
  case object AdminHierarchyTable   extends SourceType[AdminPlace]
  case object AdminPlaceGroundTable extends SourceType[AdminPlaceGround]
}
