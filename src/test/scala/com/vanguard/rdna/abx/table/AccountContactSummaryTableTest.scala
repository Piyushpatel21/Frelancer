package com.vanguard.rdna.abx
package table

import java.sql.Timestamp

import org.scalatest.WordSpec

case class ExpectedSumm(acct_id: Int, last_contact_ts: Timestamp, contact_type: String, contact_description: String)

class AccountContactSummaryTableTest extends WordSpec with ABXTestSupport {
  import ABXTestSupport.SQLDateInterpolator
  import spark.implicits._

  val dummyAccountOwner = Seq(
    (1, 1, "PRRT"), (1, 4, "SHJS"), (1, 5, "GFAA"),
    (2, 1, "PRRT"), (2, 2, "CUCM"), (2, 4, "SHPS"), (2, 5, "CUSN"),
    (3, 1, "SHJS"), (3, 2, "GFAA"), (3, 3, "CUCM"), (3, 4, "PRRT"), (3, 5, "CUCM"),
    (4, 1, "CUCM"), (4, 2, "SHPS"), (4, 3, "CUSN"), (4, 4, "PRRT"), (4, 5, "CUCM")
  ).toDF(acctId.name, poId.name, poRoleCode.name)

  val dummyClientContact = Seq(
    (1, ts"2020-01-01 01:01:01.001", "WEB_LOGON", "WEB_LOGON 1"),
    (2, ts"2020-06-02 02:02:02.002", "WEB_LOGON", "WEB_LOGON 2"),
    (3, ts"2020-11-30 03:03:03.003", "IN_PERSON", "PH 3"),
    (4, ts"2020-01-01 01:01:04.004", "PHONE", "PH 4"),
    (5, ts"2020-06-02 02:02:05.005", "WEB_LOGON", "WEB_LOGON 5"),
    (6, ts"2020-11-30 03:03:06.006", "WEB_LOGON", "WEB_LOGON 6")
  ).toDF(clientPoid.name, lastContactTs.name, contactType.name, contactDescription.name)

  val dummyAccountContact = Seq(
    (1, ts"2020-01-01 01:01:01.111", "TAX_FORM_1099", "MAIL 1"),
    (2, ts"2020-01-01 01:01:02.222", "TAX_FORM_1099", "MAIL 2"),
    (2, ts"2020-06-02 02:02:02.222", "TAX_FORM_1042S", "MAIL 2"),
    (3, ts"2020-11-30 03:03:03.333", "TAX_FORM_1099", "MAIL 3"),
    (4, ts"2020-01-01 01:01:04.444", "TAX_FORM_1099", "MAIL 4"),
    (5, ts"2020-06-02 02:02:05.555", "TAX_FORM_1042S", "MAIL 5"),
    (5, ts"2020-11-30 03:03:05.555", "TAX_FORM_1099", "MAIL 5"),
    (6, ts"2020-11-30 03:03:06.666", "TAX_FORM_1042S", "MAIL 6")
  ).toDF(acctId.name, lastContactTs.name, contactType.name, contactDescription.name)

  val expected = Set(
    ExpectedSumm(1, ts"2020-01-01 01:01:01.111", "TAX_FORM", "MAIL 1"),
    ExpectedSumm(1, ts"2020-06-02 02:02:05.005", "WEB_LOGON", "WEB_LOGON 5"),
    ExpectedSumm(1, ts"2020-01-01 01:01:04.004", "BASIC", "PH 4"),
    ExpectedSumm(1, ts"2020-06-02 02:02:02.222", "TAX_FORM_RELATED", "MAIL 2"),
    ExpectedSumm(2, ts"2020-01-01 01:01:01.111", "TAX_FORM_RELATED", "MAIL 1"),
    ExpectedSumm(2, ts"2020-01-01 01:01:04.004", "BASIC", "PH 4"),
    ExpectedSumm(2, ts"2020-06-02 02:02:02.222", "TAX_FORM", "MAIL 2"),
    ExpectedSumm(2, ts"2020-06-02 02:02:05.005", "WEB_LOGON", "WEB_LOGON 5"),
    ExpectedSumm(3, ts"2020-11-30 03:03:03.333", "TAX_FORM", "MAIL 3"),
    ExpectedSumm(3, ts"2020-01-01 01:01:04.444", "TAX_FORM_RELATED", "MAIL 4"),
    ExpectedSumm(3, ts"2020-11-30 03:03:03.003", "BASIC", "PH 3"),
    ExpectedSumm(3, ts"2020-06-02 02:02:05.005", "WEB_LOGON", "WEB_LOGON 5"),
    ExpectedSumm(4, ts"2020-11-30 03:03:03.333", "TAX_FORM_RELATED", "MAIL 3"),
    ExpectedSumm(4, ts"2020-11-30 03:03:03.003", "BASIC", "PH 3"),
    ExpectedSumm(4, ts"2020-06-02 02:02:05.005", "WEB_LOGON", "WEB_LOGON 5"),
    ExpectedSumm(4, ts"2020-01-01 01:01:04.444", "TAX_FORM", "MAIL 4"),
    ExpectedSumm(5, ts"2020-11-30 03:03:05.555", "TAX_FORM", "MAIL 5"),
    ExpectedSumm(6, ts"2020-11-30 03:03:06.666", "TAX_FORM", "MAIL 6")
  )

  lazy val underTest = new AccountContactSummaryTable(spark, "bucket", dummyClientContact, dummyAccountContact, dummyAccountOwner)

  "output" should {
    "equal all of the contact records pulled from the necessary tables" in {
      underTest.output expectingColumns(acctId, lastContactTs, contactType, contactDescription) shouldBeEqualTo
        (expected onColumns(acctId, lastContactTs, contactType, contactDescription))
    }
  }
}