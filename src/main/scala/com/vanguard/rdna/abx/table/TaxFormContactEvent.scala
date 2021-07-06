package com.vanguard.rdna.abx
package table

import com.vanguard.rdas.rc.spark.DerivedColumn
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * Gathers 1099/1042 tax form contact events from enterprise tables.
 *
 * @param spark [[SparkSession]] instance
 * @param tables the object that contains all needed enterprise tables
 * @param daysOfStartDelay number of days of delay accounted for mail delivery to start RPO
 */
class TaxFormContactEvent(spark: SparkSession, tables: ABXTables, daysOfStartDelay: Int) {
  final val vbsTaxCodes = List("B19B", "B9IN", "B9MS", "B9OI", "BCRT", "BRMC", "B142", "B19Q", "B19R", "B9DV", "BCON", "BWFT")
  final val mfTaxCodes = List("M19B", "M19Q", "M142", "M19D", "M19R")

  /**
   * Retrieves all 1099/1042 tax form contact events. The returned DataFrame will have the acct_id,
   * last_contact_ts, contact_type and contact_desc columns, in that order. The returned DataFrame
   * contains all contact events in the history. No dedeplication is done.
   *
   * @return a [[DataFrame]] containing all 1099/1042 tax form contact events.
   */
  def getEvents: DataFrame = {

    // contact events fall within RPO active windows will be ignored
    val mfRpoWindows = tables.vmutlFndPosn.where(trim(statusCode.column) === 4)
      .select(acctId, new DerivedColumn(beginDate, date_add(statusChangeDate.column, -daysOfStartDelay)), endDate)
    val brkgRpoWindows = tables.vacctRstrn.where(trim(rstrnTypCode.column) === "RPO")
      .select(acctId, new DerivedColumn(beginDate, date_add(beginTimestamp.column, -daysOfStartDelay)), endTimestamp ==> endDate)
    val rpoWindows = mfRpoWindows.union(brkgRpoWindows)

    val contactVBS: DataFrame = tables.vvbsTaxForm.select(acctId, versnTs ==> lastContactTs, "tax_form_nm" ==> typeCode)
      .where(trim(typeCode.column).isInCollection(vbsTaxCodes))
    val contactMF: DataFrame = tables.vmfTaxForm.select(acctId, versnTs ==> lastContactTs, "tax_form_typ" ==> typeCode)
      .where(trim(typeCode.column).isInCollection(mfTaxCodes))
    val contactDF = contactVBS.union(contactMF)
      .join(tables.vacct.select(acctId), acctId, "inner")

    def formDescription(taxForm: String): Column = {
      format_string("communication_channel=%s, tax_form=%s, type_code=%s",
        lit(mailContactType), lit(taxForm), trim(typeCode.column)
      )
    }

    contactDF
      .join(rpoWindows, contactDF(acctId) === rpoWindows(acctId) && lastContactTs.column >= beginDate.column && lastContactTs.column <= endDate.column, "left_anti")
      .withColumn(contactType, when(trim(typeCode.column).isin("M142", "B142"), lit(taxForm1042sContactType)).otherwise(lit(taxForm1099ContactType)))
      .withColumn(contactDescription,
        when(contactType.column === lit(taxForm1099ContactType), formDescription("1099"))
          .when(contactType.column === lit(taxForm1042sContactType), formDescription("1042s"))
       )
      .select(acctId, lastContactTs, contactType, contactDescription)
  }
}
