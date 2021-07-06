package com.vanguard.rdna.abx

import com.vanguard.rdna.abx.table.{ABXTables, ABXTablesImpl, EmailGenerator, TableGenerator}
import org.apache.spark.sql.SparkSession

class MainExecutor(spark: SparkSession, jobConfig: JobConfig) {
  def execute(): Unit = {
    val inputTables: ABXTables = new ABXTablesImpl(spark)
    TableGenerator(spark, jobConfig, inputTables).execute()
    EmailGenerator(jobConfig, inputTables).execute()
  }
}