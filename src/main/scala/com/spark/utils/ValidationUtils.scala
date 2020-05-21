package com.spark.utils

import org.apache.spark.sql.functions.udf

/**
 * Created by pallavik on 5/20/20.
 */
object ValidationUtils {
  val maxLong = Long.MaxValue
  val minLong = Long.MinValue
  val validateIsNull = udf(isNull(_: String, _: Any, _: String))
  val isNull = udf((arg: String) => {
    isNullFun(arg)
  })

  def isNullFun(arg:String) : Boolean = {
    !(Option(arg).isDefined && !arg.isEmpty)
  }


  def updateErrorMessage(errorMessage: String, customMessage: String, separator : String = "|") : String = {
    if (Option(errorMessage).getOrElse("").isEmpty)
      customMessage
    else if (Option(customMessage).getOrElse("").isEmpty)
      errorMessage
    else
      errorMessage + separator + customMessage
  }


  val updateErrorMessageUDF = udf(updateErrorMessage(_: String,_:String))

  def isNull(errorMessageCol: String, arg: Any, errorMessage: String): String = {
    var outputErrorMessage: String = ""
    if (arg == null) {
      outputErrorMessage = errorMessage + " cannot be null"
    }
    else arg match {
      case argValue: String =>
        if (argValue.trim.isEmpty)
          outputErrorMessage = errorMessage + " cannot be empty"
      case colLongValue: Long =>
        if (colLongValue == 0) outputErrorMessage = errorMessage + " cannot be null/empty or 0"
        if (maxLong < colLongValue) outputErrorMessage = errorMessage + " cannot be more than Long.Max"
        if (minLong > colLongValue) outputErrorMessage = errorMessage + " cannot be less than Long.Min"
      case _ =>
    }
    concatString("|", errorMessageCol, outputErrorMessage)
  }

  def concatString(sep: String, xs: String*): String = xs.filter(s => s != null && s.trim.nonEmpty).mkString(sep)
}
