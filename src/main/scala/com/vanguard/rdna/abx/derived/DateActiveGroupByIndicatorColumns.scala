package com.vanguard.rdna.abx.derived

import com.vanguard.rdas.rc.spark.{GroupByIndicatorColumns, TableColumn}
import com.vanguard.rdna.abx.isActiveRecord
import org.apache.spark.sql.DataFrame

abstract class DateActiveGroupByIndicatorColumns(groupByCol: TableColumn, inputCol: TableColumn, private[derived] val begin: TableColumn, private[derived] val end: TableColumn)
extends GroupByIndicatorColumns(groupByCol, inputCol) {
  override def apply(df: DataFrame): DataFrame = super.apply(df.where(isActiveRecord(begin, end)))
}