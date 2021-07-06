package com.vanguard.rdna.abx
package derived

import java.sql.Date

import com.vanguard.rdas.rc.spark.TableColumn
import org.apache.spark.sql.DataFrame
import org.scalatest.WordSpec

import scala.reflect.runtime.universe.TypeTag

abstract class DateActiveGroupByIndicatorColumnsTest[T: TypeTag, V: TypeTag] extends WordSpec with ABXTestSupport {
  import ABXTestSupport.SQLDateInterpolator

  val indicators: DateActiveGroupByIndicatorColumns
  val openRecords: Seq[(T, V)]
  val closedRecords: Seq[(T, V)]

  val startDate = dt"2021-01-04"
  val openDate = dt"9999-12-31"
  val closedDate = dt"2021-01-08"

  private def addDates(ed: Date)(tup: (T, V)): (T, V, Date, Date) = (tup._1, tup._2, startDate, ed)
  private lazy val results: DataFrame = indicators(spark.createDataFrame(openRecords.map(addDates(openDate)) ++ closedRecords.map(addDates(closedDate)))
    .toDF(indicators.groupByCol.name, indicators.inputCol.name, indicators.begin.name, indicators.end.name)).cache()

  def getKeysWhereIndicator(ic: TableColumn, v: Boolean): Seq[T] = results.where(ic.column === (if (v) "Y" else "N"))
    .select(indicators.groupByCol).collect().map(_.getAs[T](0))
}
