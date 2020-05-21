package com.spark.models

/**
 * Created by pallavik on 5/20/20.
 */
case class ValidationMetaData(
                               columnName : String = null,
                               isNull: Boolean = false, // true = "not null", false = nullable
                               dataType:DataTypeEnum.Value = DataTypeEnum.STRING,
                               // String
                               size : Long = -1l,
                               minLength : Long = 0l,
                               // NUMBER
                               minValue : Option[Double] = None,
                               maxValue : Option[Double] = None,
                               precision : Integer = 0,
                               // Date
                               dateFormat : String = null,
                               //Boolean format
                               validOptions : Seq[String] = null, // similar to IN clause 1/0 or Y/N or T/F
                               isDataSeq : Boolean = false
                             )
