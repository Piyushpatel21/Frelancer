package com.vanguard.rdna.abx
package table

import com.vanguard.rdas.rc.spark.TableSchema
import com.vanguard.rdas.rc.test.DataFrameEquality
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.WordSpec

class FakeABXOutputTable(fakeData: DataFrame, _schema: TableSchema)
extends ABXOutputTable(fakeData.sparkSession, "bucket", "test_db", "test_table", _schema) {
  val output: DataFrame = fakeData
  override lazy val tableDdl: String = s"create table if not exists $fullTableName ${schema.describe}"
  override lazy val databaseDdl: String = s"create database if not exists $databaseName"
}

class ABXOutputTableTest extends WordSpec with ABXTestSupport {
  import spark.implicits._

  val expected = Seq((1, "one"), (2, "two"))
  val data = expected.toDF("a", "b")
  val schema = TableSchema(Array("a" -> IntegerType, "b" -> StringType))
  val underTest = new FakeABXOutputTable(data, schema)
  implicit val dfEquality = DataFrameEquality()

  "capture" should {
    "create the table and write the data to the table" in {
      underTest.capture()
      underTest.self expectingColumns("a", "b", lastUpdatedTs) shouldBeEqualTo (expected onColumns("a", "b"))
    }
  }
}
