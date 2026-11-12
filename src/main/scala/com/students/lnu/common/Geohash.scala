package com.students.lnu.common

/**
  * Minimal, dependency-free implementation of the standard geohash encoding
  * (base32, "0123456789bcdefghjkmnpqrstuvwxyz").
  *
  * Encoding is fully deterministic for a given (latitude, longitude, precision),
  * so the same input always yields the same geohash.
  */
object Geohash {
  private val Base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

  /**
    * Encode a coordinate into a geohash string.
    *
    * @param latitude  latitude in degrees, range [-90, 90]
    * @param longitude longitude in degrees, range [-180, 180]
    * @param precision number of geohash characters to produce (must be > 0)
    */
  def encode(latitude: Double, longitude: Double, precision: Int): String = {
    require(precision > 0, s"geohash precision must be positive, got: $precision")

    var latInterval = (-90.0, 90.0)
    var lonInterval = (-180.0, 180.0)

    val geohash = new StringBuilder(precision)
    var isEven  = true
    var bit     = 0
    var ch      = 0

    while (geohash.length < precision) {
      if (isEven) {
        val mid = (lonInterval._1 + lonInterval._2) / 2
        if (longitude >= mid) {
          ch |= bitsMask(bit)
          lonInterval = (mid, lonInterval._2)
        } else {
          lonInterval = (lonInterval._1, mid)
        }
      } else {
        val mid = (latInterval._1 + latInterval._2) / 2
        if (latitude >= mid) {
          ch |= bitsMask(bit)
          latInterval = (mid, latInterval._2)
        } else {
          latInterval = (latInterval._1, mid)
        }
      }

      isEven = !isEven

      if (bit < 4) {
        bit += 1
      } else {
        geohash.append(Base32.charAt(ch))
        bit = 0
        ch = 0
      }
    }

    geohash.toString()
  }

  private val bits = Array(16, 8, 4, 2, 1)

  private def bitsMask(bit: Int): Int = bits(bit)
}
