package com.students.lnu.model.common.wrapper

case class WayLineWrapper(wayLineId: Long, coordinates: Seq[OrderedCoordinateWrapper], tags: Map[String, String])
