package com.vanguard.rdna.abx
package table

import com.vanguard.rdas.rc.spark.{SchemaBasedS3Table, TableSchema}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class ABXOutputTable(spark: SparkSession, bucket: String, database: String, tableName: String, schema: TableSchema, appendData: Boolean = false)
extends SchemaBasedS3Table(spark, bucket, database, tableName, schema + fromRepo(lastUpdatedTs), appendData) {
  def output: DataFrame
  def outputWithTimestamp: DataFrame = output + newLastUpdatedTs
  def capture(recreate: Boolean = false): Unit = {
    createTable(recreate)
    logStdout(s"Capturing data for $fullTableName...")
    outputWithTimestamp.writeToTable()
  }
}
