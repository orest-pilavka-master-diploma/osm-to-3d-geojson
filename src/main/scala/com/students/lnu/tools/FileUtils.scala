package com.students.lnu.tools

import com.github.tototoshi.csv.CSVReader

import java.io.{FileInputStream, InputStream}
import scala.io.Source
import scala.util.{Failure, Success, Using}

object FileUtils {

  def toInputStream(path: String): InputStream =
    Option(getClass.getClassLoader.getResourceAsStream(path))
      .getOrElse(new FileInputStream(path))

  def toSource(path: String): Source = Source.fromInputStream(toInputStream(path))

  def readConfigCSV(path: String): Seq[Map[String, String]] = {
    Using(CSVReader.open(Source.fromInputStream(toInputStream(path))))(_.allWithHeaders()) match {
      case Success(lines) => lines
      case Failure(e)     => throw e
    }
  }
}
