package com.vanguard.rdna.abx
package derived

import com.vanguard.rdas.rc.spark.GenericColumn
import com.vanguard.rdna.abx.table.ABXTables
import org.apache.spark.sql.functions.{bround, lit, max, negate, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

private[abx] sealed trait HoldingsData {
  def appendHoldings(accountData: DataFrame): DataFrame = accountData.join(positions, accountJoinColumn).withColumn(marketValue, bround(marketValueColumn, 2))

  def marketValueColumn: Column
  def accountJoinColumn: GenericColumn
  def positions: DataFrame
}

private[abx] case class TaHoldings(spark: SparkSession, tables: ABXTables) extends HoldingsData {
  import tables._

  private lazy val latestPriceDate: java.sql.Timestamp = vissPrcVgi
    .where(currencyCode.column === "USD" && priceTypeCode.column === "NAV")
    .agg(max(priceEffectiveDate)).head.getTimestamp(0)

  def activeFunds: DataFrame = vcVgiIntIns
    .where(mutualFundStatusCode.column === "ACTV")
    .select(insId, portId ==> fundId, cusip, "tckr_sym" ==> tickerSymbol, "lng_nm" ==> fundLongName)

  def assetMetadata: DataFrame = vissPrcVgi
    .where(currencyCode.column === "USD" && priceTypeCode.column === "NAV" && priceEffectiveDate.column === latestPriceDate)
    .deduplicate(insId.column, lastUpdatedTimestamp.column.desc)
    .select(insId, "unit_am" ==> nav, priceEffectiveDate ==> marketValueDate)
    .join(activeFunds, insId, "outer")

  override def positions: DataFrame = vmutlFndPosn.where(isActiveRecord())
    .select(acctId, vastAcctNumber, portId ==> fundId, issueShareQuantity, bookShareQuantity)
    .join(assetMetadata, fundId)

  val accountJoinColumn: GenericColumn = acctId
  val marketValueColumn: Column = (bookShareQuantity.column + issueShareQuantity.column) * nav.column
}

private[abx] case class BrokerageHoldings(spark: SparkSession, tables: ABXTables, isProd: Boolean) extends HoldingsData {
  import tables._

  override def positions: DataFrame = {
    val recordDate = vacctPosBrkCurPrt.select(max(posnDt)).head().getTimestamp(0)
    val partitionId = vacctPosBrkCurPrt.where(posnDt.column === recordDate).select(prtnId).head.getLong(0)
    logStdout(s"In VB positions: got prtn_id $partitionId")

    // the deduplication isn't necessary. Just to 100% guarantee the uniqueness per ins_id
    val insIdToCusipNo = vissCharsVbs.select(insId, cusipNo).deduplicate(insId.column, cusipNo.column.desc)

    // instrument with CUSIP "999999999" is (debt) cash position, therefore negate it
    val mv = when(cusipNo.column === "999999999", negate(marketValueAmount.column)).otherwise(marketValueAmount.column)
    vacctPosnBrkCur.where(prtnId.column === partitionId).drop(cusipNo)
      .join(insIdToCusipNo, insId)
      .select(acctId, marketValue <== mv, marketValueDate <== lit(recordDate))
  }

  val marketValueColumn: Column = marketValue.column
  val accountJoinColumn: GenericColumn = acctId
}


