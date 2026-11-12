package com.students.lnu.ml

import scala.util.Try

/**
 * Parses the raw OSM `building:levels` tag value into a numeric floor count.
 *
 * Handles the common messy encodings found in OSM data: multi-value tags such
 * as "3;4" or "3,4" (the first token wins), values with a trailing unit or
 * stray characters, and fractional counts like "1.5". Anything that cannot be
 * interpreted as a number (null, blank, "ground", garbage) yields None.
 */
object BuildingLevelsParser {

  def parse(raw: String): Option[Double] = {
    if (raw == null) {
      None
    } else {
      val trimmed = raw.trim
      if (trimmed.isEmpty) {
        None
      } else {
        val firstToken = trimmed
          .split("[;,]", 2)
          .head
          .trim

        val numericPrefix = stripTrailingNonNumeric(firstToken)

        if (numericPrefix.isEmpty) None
        else Try(numericPrefix.toDouble).toOption
      }
    }
  }

  private def stripTrailingNonNumeric(value: String): String = {
    value.reverse.dropWhile(c => !(c.isDigit || c == '.')).reverse
  }
}
