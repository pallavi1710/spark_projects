package com.spark.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Created by pallavik on 5/20/20.
 */
object DateUtil {
  /**
   * Checks if Date is in correct format.
   *
   * @param rawDate
   * @param format
   * @return true if valid date is as in format
   */
  def isValidDate(rawDate : String, format : String) : Boolean = {
    try {
      val formattedDate: DateTime = DateTimeFormat.forPattern(format).parseDateTime(rawDate)
      if(formattedDate.getYear < 1000 || formattedDate.getYear >= 9999) {
        return false
      } else {
        return true
      }
      true
    } catch {
      case e: Exception => false
    }
  }

}
