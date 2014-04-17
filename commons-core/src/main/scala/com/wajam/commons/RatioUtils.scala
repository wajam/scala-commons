package com.wajam.commons

object RatioUtils {

  /**
   * For all possible value of a long, allow only a certain percentage of those value to be processed.
   *
   * Possible use case: Incremental release of a feature to a percentage of users. ex: valueIncluded(userId, 0.05) would
   * return true for only 0.05 of the userId, allowing the feature to be branched according to this function output.
   *
   * @param value The value to be branch upon
   * @param ratio The % of value to be included (i.e. that will pass through)
   */
  def valueIncluded(value: Long, ratio: Double): Boolean = {
    require(0.0 <= ratio && ratio <= 1.0)

    value % 100 + 1 <= (ratio * 100).toInt
  }
}
