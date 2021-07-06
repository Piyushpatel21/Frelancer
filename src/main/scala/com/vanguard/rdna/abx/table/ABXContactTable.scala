package com.vanguard.rdna.abx
package table

import com.vanguard.rdas.rc.spark.{TableColumn, TableSchema}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class ABXContactTable(spark: SparkSession, bucket: String, tableName: String, val levelColumn: TableColumn)
extends ABXOutputTable(spark, bucket, abxDB, tableName, TableSchema(Array(levelColumn, lastContactTs, contactType, contactDescription))) {
  def contactData: DataFrame
  lazy val output: DataFrame = contactData.deduplicate(Seq(levelColumn.column, contactType.column), Seq(lastContactTs.column.desc))
}
