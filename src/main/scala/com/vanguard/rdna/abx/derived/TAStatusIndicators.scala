package com.vanguard.rdna.abx
package derived

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{regexp_extract, trim}
import org.apache.spark.sql.types.IntegerType

object TAStatusIndicators extends DateActiveGroupByIndicatorColumns(acctId, statusCode, beginDate, endDate) {
  val rpo: IndicatorColumn = IndicatorColumn(rpoIndicator, Seq(4))
  val escheated: IndicatorColumn = IndicatorColumn(escheatedIndicator, Seq(5))

  private def removeLeadingZeros(c: Column): Column = regexp_extract(trim(c), "^0*(\\d+?)$", 1).cast(IntegerType)
  override def apply(df: DataFrame): DataFrame = super.apply(df.withColumn(statusCode := removeLeadingZeros))
}
