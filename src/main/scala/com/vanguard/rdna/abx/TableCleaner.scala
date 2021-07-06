package com.vanguard.rdna.abx

import com.vanguard.acl.lumen.operations.FileOperations
import com.vanguard.rdas.rc.core.LoggingSupport
import com.vanguard.rdas.rc.s3.{S3Client, S3Utility}
import org.apache.spark.sql.SparkSession

object TableCleaner extends LoggingSupport {
  def removeTables(spark: SparkSession, bucket: String, s3Client: S3Utility = S3Client)(tables: Set[String]): Unit = {
    tables foreach { table =>
      logStdout(s"Removing table $table if it exists...")
      spark.sql(s"drop table if exists $table")
      val tp = table.replace('.', '/')
      s3Client.deleteFromS3(bucket,  tp + "/")
      s3Client.deleteFromS3(bucket, tp + "_$folder$")
    }
  }

  def removeOldTables(spark: SparkSession, bucket: String): Unit =
    FileOperations.onResource("oldTables.txt", _.getLines.toSet).foreach(removeTables(spark, bucket))
}
