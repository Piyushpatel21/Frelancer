package com.vanguard.rdna.abx
package table

import org.apache.spark.sql.{DataFrame, SparkSession}

class AccountContactEventTable(spark: SparkSession, tables: ABXTables, bucket: String)
extends ABXContactTable(spark, bucket, accountContactEventTableName, acctId) {
  val taxFormEvents = new TaxFormContactEvent(spark, tables, 30).getEvents
  val transactionEvents = new TransactionContactEvent(spark, tables).getTransactions

  override val contactData: DataFrame = taxFormEvents.union(transactionEvents)
}
