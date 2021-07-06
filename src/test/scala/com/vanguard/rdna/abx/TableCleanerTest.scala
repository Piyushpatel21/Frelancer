package com.vanguard.rdna.abx

import com.vanguard.rdas.rc.s3.S3Utility
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.verify
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

class TableCleanerTest extends FunSuite with MockitoSugar {

  val table = "db.table"
  val expectedBucket = "bucket"

  test("removeTables") {
    val mockS3Client = mock[S3Utility]
    val mockSession = mock[SparkSession]

    TableCleaner.removeTables(mockSession, expectedBucket, mockS3Client)(Set(table))

    verify(mockSession).sql(s"drop table if exists $table")
    verify(mockS3Client).deleteFromS3(expectedBucket, "db/table/")
    verify(mockS3Client).deleteFromS3(expectedBucket, "db/table_$folder$")
  }
}
