package com.spark.utils

import com.spark.models.{DataTypeEnum, Message, ValidationMetaData}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}

import scala.util.Try

/**
 * Created by pallavik on 5/20/20.
 */
object DataValidator {
  /*
   * This methods validates Dataset[Row] ~ DataFrame by parsing validationMetaDataMap
   * @param
   *      input dataset for validation
   *      error message column name
   * @returns
   *      validated data frame with updated error message
   */
  //TODO: add testing
  //TODO: add to implicits
  def apply(inputDataset:Dataset[Row],
            validationMetaDataMap: Map[String,ValidationMetaData],
            errorMessageColumnName : String = "errorMessage"): Dataset[Row] = {
    val schema = inputDataset.schema
    val encoder = RowEncoder.apply(schema)
    val validationRequiredColumns : Seq[String] = schema
      .filter(structFiled => validationMetaDataMap.contains(structFiled.name))
      .map(_.name)

    inputDataset.map{ case row : Row =>
      val errorMessage : String = row.getAs[String](errorMessageColumnName)
      val errorMessageIndex = row.fieldIndex(errorMessageColumnName)
      val finalErrorMessage : String = validationRequiredColumns.foldLeft(errorMessage){ case (errorMsg,columnName)=>
        val data = row.getAs(columnName)
        val validationMetaData = validationMetaDataMap.get(columnName).get
        val columnErrorMessage =  apply(data,validationMetaData)
        ValidationUtils.concatString("|", errorMsg,columnErrorMessage)
      }
      import scala.collection.mutable.ListBuffer
      val rowObject = new ListBuffer[Any]
      for (i <- 0 until row.length) {
        if(i == errorMessageIndex){
          rowObject += finalErrorMessage
        }
        else{
          rowObject += row.get(i)
        }
      }
      Row.fromSeq(rowObject)
    }(encoder)
  }

  def apply(data:Seq[Any],validationMetaData: ValidationMetaData) : String = {
    var errorMessage : String = ""
    data.find(d => {
      errorMessage = apply(d,validationMetaData)
      Option(errorMessage).getOrElse("").nonEmpty
    })
    errorMessage
  }

  def apply(data:Any, validationMetaData: ValidationMetaData) : String = {
    val columnName = validationMetaData.columnName
    if(data == null || data.toString.isEmpty){
      if(validationMetaData.isNull){
        // data is NULL - FAILED
        columnName + Message.nullError
      }
      else{
        // NULL validation not required => NULL are accepted - NO ERROR - NO further validation required
        ""
      }
    }
    else{
      // data is valid
      data match {
        case argValue: String =>
          checkForDataTypeValidation(argValue,validationMetaData)
        case colLongValue: Long =>
          checkForNumberValue(columnName,colLongValue, validationMetaData.minValue,validationMetaData.maxValue)
        case colDoubleValue : Double =>
          checkForNumberValue(columnName,colDoubleValue, validationMetaData.minValue,validationMetaData.maxValue)
        case _ => ""
      }
    }
  }

  def checkForDataTypeValidation(data:String, validationMetaData: ValidationMetaData) : String = {
    val columnName = validationMetaData.columnName
    validationMetaData.dataType match {
      case DataTypeEnum.STRING =>
        val lengthErrorMessage = checkForStringLength(columnName,data, validationMetaData.minLength,validationMetaData.size)
        val validOptionErrorMessage = if(validationMetaData.validOptions != null) {
          val validOptions : Seq[String] = validationMetaData.validOptions.map(_.toUpperCase)
          checkForValidOptions(columnName,data,validOptions)
        } else {
          ""
        }
        ValidationUtils.concatString("|",lengthErrorMessage, validOptionErrorMessage)
      case DataTypeEnum.LONG =>
        if(Try(data.toLong).toOption.isDefined){
          checkForNumberValue(columnName,data.toLong, validationMetaData.minValue,validationMetaData.maxValue)
        }
        else{
          columnName + Message.numberFormatError
        }
      case DataTypeEnum.INTEGER =>
        if(Try(data.toInt).toOption.isDefined){
          checkForNumberValue(columnName,data.toInt,
            validationMetaData.minValue,validationMetaData.maxValue)
        }
        else{
          columnName + Message.numberFormatError
        }
      case DataTypeEnum.DOUBLE =>
        if(Try(data.toDouble).toOption.isDefined){
          checkForNumberValue(columnName,data.toDouble, validationMetaData.minValue,validationMetaData.maxValue)
        }
        else{
          columnName + Message.numberFormatError
        }
      case DataTypeEnum.FLOAT =>
        if(Try(data.toFloat).toOption.isDefined){
          checkForNumberValue(columnName,data.toFloat, validationMetaData.minValue,validationMetaData.maxValue)
        }
        else{
          columnName + Message.numberFormatError
        }
      case DataTypeEnum.DATE =>
        val dateFormat = validationMetaData.dateFormat
        if (DateUtil.isValidDate (data, dateFormat) ) {
          ""
        }
        else {
          columnName + Message.dateFormatError + dateFormat + ")"
        }
      case DataTypeEnum.BOOLEAN =>
        val validOptions : Seq[String] = validationMetaData.validOptions.map(_.toUpperCase)
        checkForValidOptions(columnName,data,validOptions)

      case _ => ""
    }
  }

  def checkForValidOptions(columnName:String,data:String, validOptions:Seq[String]) : String = {
    if(validOptions.contains(data.trim.toUpperCase)){
      ""
    }
    else{
      columnName + Message.validOptionsError
    }
  }

  // Using formatting, otherwise large Double values may get printed in scientific format.
  def checkForNumberValue(columnName:String,data:Double, minValue:Option[Double],
                          maxValue:Option[Double]) : String = {
    data match{
      case x if minValue.isDefined && x < minValue.get => columnName + Message.valueLessThanError + f"${minValue.get}%1.2f"
      case y if maxValue.isDefined && y > maxValue.get => columnName + Message.valueMoreThanError + f"${maxValue.get}%1.2f"
      case _ => ""
    }
  }

  def checkForStringLength(columnName:String, data:String,minLength:Long,maxLength:Long) : String = {
    data.length match{
      case x if maxLength != -1l && (minLength>x || x>maxLength) => columnName + Message.stringLengthError + maxLength
      case _ => ""
    }
  }
}
