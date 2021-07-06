package com.vanguard.rdna.abx
package table

import com.vanguard.rdas.rc.spark.TableSchema
import com.vanguard.rdna.abx.derived.TaHoldings
import org.apache.spark.sql.{DataFrame, SparkSession}

class TaHoldingTable(spark: SparkSession, tables: ABXTables, bucket: String, account: => DataFrame)
extends ABXOutputTable(spark, bucket, abxDB, taHoldingTable, TableSchema(
  Array(acctId, vastAcctNumber, cusip, fundId, fundLongName, tickerSymbol, bookShareQuantity, issueShareQuantity, nav, marketValue, marketValueDate)
)) {
  lazy val output: DataFrame = TaHoldings(spark, tables).appendHoldings(account.where(accountType.column === "TA").select(acctId))
}
