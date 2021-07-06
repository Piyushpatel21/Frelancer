package com.vanguard.rdna.abx
package table

import java.sql.Timestamp

import com.vanguard.rdas.rc.spark.{DerivedColumn, TableColumn}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import org.mockito.Mockito.when
import org.scalatest.WordSpec

/* Contains the columns we want to check and ignore the rest */
case class AccountDetailTestRecord(
    acct_id: java.math.BigDecimal, brokerage_acct_no: String, client_owned_flag: String, serv_id: Int, sag_id: java.math.BigDecimal, account_type: String,
    acct_estab_dt: Timestamp, po_id_1: Option[Long], po_role_cd_1: String, po_id_2: Option[Long], po_role_cd_2: String,
    edelivery_confirmation: String, derived_ste_cd: String, total_acct_balance: Double
)

class AccountDetailTableTest extends WordSpec with ABXTestSupport {
  import ABXTestSupport.SQLDateInterpolator
  import spark.implicits._

  private implicit def toBigDecimal(l: Long): java.math.BigDecimal = new java.math.BigDecimal(l)
  private implicit def toLongOption(l: Long): Option[Long] = Option(l)

  def colAs(cn: String, dt: DataType): DerivedColumn = TableColumn(cn) := (_.cast(dt))

  val issCharsVbsDF: DataFrame = Seq[(Long, String)](
    (1231234, "999999999"),
    (10460142L, null),
    (10460143L, "00184AAB1"),
    (10460144L, "049560105"),
    (10460145L, "25459A100"),
    (10460146L, ""),
    (10460147L, "25459A100")
  ).toDF(insId, cusipNo)

  val acctPosBrkCurPrtDF: DataFrame = Seq[(Long, Timestamp)](
    (1, ts"2021-05-03 00:00:00.0"),
    (2, ts"2021-04-30 00:00:00.0")
  ).toDF(prtnId, posnDt)

  val acctPosnBrkCurDF: DataFrame = Seq[(Long, Long, Long, String, Double)](
    (1, 48412142737L, 1231234L, "", 100.00),
    (1, 48412142737L, 10460142L, "", 312.00),
    (2, 48412142737L, 10460143L, "", 442.00),     // old partition, market value ignored
    (1, 754528112853L, 10460144L, "", 8909.55),
    (1, 772631112759L, 10460145L, "", 4.58),
    (1, 2392707014738L, 10460146L, "", -123456),
    (1, 51301L, 10460147L, "", 908)               // TA account, market value ignored
  ).toDF(prtnId, acctId, insId, cusipNo, marketValueAmount)

  "output" should {
    "equal all of the accounts pulled from the necessary tables" in {
      val accountDF: DataFrame = readCSV("account_detail_table_test/test1/account", AccountBaseTable.schema.sparkSchema)
      val ownerDF: DataFrame = readCSV("account_detail_table_test/test1/account_owner", AccountOwnerTable.schema.sparkSchema)
      val accountContactSummaryDF: DataFrame = readCSV("account_detail_table_test/test1/account_contact_summary")
      val taHoldingDF = readCSV("account_detail_table_test/test1/ta_holding")

      val expected = Set(
        AccountDetailTestRecord(43101L, null, "Y", 8, 43101L, "TA", ts"1994-05-27 00:00:00.0", 217, "PRRT", None, null, "N", "PA", 418775.11),
        AccountDetailTestRecord(51301L, null, "Y", 8, 51301L, "TA", ts"1996-09-18 00:00:00.0", 281, "PRRT", 282, "SHSH", "N", "ME", 515954.33),
        AccountDetailTestRecord(772631112759L, "55549765", "Y", 90, 128356631112759L, "VBA", ts"2016-03-31 00:00:00.0", 1619451971, "PRRT", None, null, "Y", "AK", 4.58),
        AccountDetailTestRecord(2692203020757L, "17292349", "Y", 9, 913787203020757L, "VBS", ts"2012-04-05 00:00:00.0", None, null, None, null, null, "PA", 0.00),
        AccountDetailTestRecord(2392707014738L, "10547085", "Y", 9, 669416707014738L, "VBS", ts"2017-10-12 00:00:00.0", 982633416, "PRRT", null, null, "N", "KS", -123456.00),
        AccountDetailTestRecord(48412142737L, "68198285", "Y", 90, 888776412142736L, "VBA", ts"2014-12-12 00:00:00.0", 107617352, "PRRT", None, null, "N", "AK", 212.00),
        AccountDetailTestRecord(51401L, null, "Y", 38, 51401L, "TA", ts"1996-10-11 00:00:00.0", 281, "PRRT", 282, "SHSH", "N", "ME", 101766.41),
        AccountDetailTestRecord(754528112853L, "43028771", "Y", 90, 40679528112853L, "VBA", ts"2015-09-28 00:00:00.0", 608714688, "PRRT", None, null, "N", "KS", 8909.55)
      )

      when(tables.vissCharsVbs).thenReturn(issCharsVbsDF)
      when(tables.vacctPosBrkCurPrt).thenReturn(acctPosBrkCurPrtDF)
      when(tables.vacctPosnBrkCur).thenReturn(acctPosnBrkCurDF)

      val underTest = new AccountDetailTable(spark, tables, "bucket", accountDF, ownerDF, accountContactSummaryDF, taHoldingDF)
      underTest.output shouldBeEqualTo (expected.onTheSameColumnsInAnyOrder.withDoubleTolerance(.001))
    }
  }
}