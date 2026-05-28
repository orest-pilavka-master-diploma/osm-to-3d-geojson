package com.students.lnu.geojson

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object FileWriter {
  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  def write(content: Array[Byte], outPath: String):Unit = {

    val fileName = "buildings-3d-v" + LocalDateTime.now().format(formatter) + ".geojson"

    Files.write(
      Paths.get(outPath + "/" + fileName),
      content,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
  }

}
