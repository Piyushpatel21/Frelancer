package com.vanguard.rdna.abx.derived

import com.vanguard.rdna.abx.{acctId, beginTimestamp, endTimestamp, escheatedIndicator, rpoIndicator, rstrnTypCode}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.trim

object VBRstrnCodeIndicators extends DateActiveGroupByIndicatorColumns(acctId, rstrnTypCode, beginTimestamp, endTimestamp) {
  val rpo: IndicatorColumn = IndicatorColumn(rpoIndicator, Seq("RPO"))
  val esch: IndicatorColumn = IndicatorColumn(escheatedIndicator, Seq("ESCH"))

  override def apply(df: DataFrame): DataFrame = super.apply(df.withColumn(inputCol := trim))
}