package com.vanguard.rdna.abx.table

import com.vanguard.rdna.abx.{JobConfig, TableCleaner}
import org.apache.spark.sql.SparkSession

case class TableGenerator(spark: SparkSession, jobConfig: JobConfig, inputTables: ABXTables, recreate: Boolean = true) {

  val destination = s"vgi-retail-${jobConfig.env}-us-east-1-dna-abandoned-property"

  // Zero dependency tables
  val accountTable = new AccountBaseTable(spark, inputTables, destination)
  val clientContactEventTable = new ClientContactEventTable(spark, inputTables, destination)
  val accountContactEventTable = new AccountContactEventTable(spark, inputTables, destination)

  // Account table dependent tables
  val accountOwnerTable = new AccountOwnerTable(spark, inputTables, destination, accountTable.self)
  val taHoldingTable = new TaHoldingTable(spark, inputTables, destination, accountTable.self)

  // Multi-table dependent tables
  val accountContactSummaryTable = new AccountContactSummaryTable(spark, destination, clientContactEventTable.self, accountContactEventTable.self, accountOwnerTable.self)
  val accountDetailTable = new AccountDetailTable(spark, inputTables, destination, accountTable.self, accountOwnerTable.self, accountContactSummaryTable.self, taHoldingTable.self, jobConfig.isProd)
  val accountBeneficiaryTable = new AccountBeneficiaryTable(spark, inputTables, destination, accountDetailTable.self)

  val outputTables: Seq[ABXOutputTable] = List(
    accountTable,
    clientContactEventTable,
    accountContactEventTable,
    accountOwnerTable,
    taHoldingTable,
    accountContactSummaryTable,
    accountDetailTable,
    accountBeneficiaryTable
  )

  def execute(): Unit = {
    TableCleaner.removeOldTables(spark, destination)
    outputTables.foreach { t =>
      // Clear spark cache before generating each table. The assumption is that we're not passing
      // cached DataFrames from one table to another table.
      spark.catalog.clearCache()
      t.capture(recreate)
    }
  }
}

