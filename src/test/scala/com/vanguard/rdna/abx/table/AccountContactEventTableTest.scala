package com.vanguard.rdna.abx
package table

import org.mockito.Mockito.when
import org.scalatest.FunSuite

class AccountContactEventTableTest extends FunSuite with ABXTestSupport {
  import ABXTestSupport.SQLDateInterpolator
  import spark.implicits._

  test("account_contact_event table is populated with tax form events and transactions both recurring and non-recurring") {
    val vbsTaxData = Seq(
      (2, ts"2020-11-02", "B19B"),
      (2, ts"2020-11-22", "BCON"),
      (4, ts"2020-11-04", "B9MS"),
      (4, ts"2020-11-24", "B19Q"),
      (6, ts"2020-11-29", "B142"),
      (12, ts"2020-11-10", "B19Q"),
      (14, ts"2020-11-20", "B19R"),
      (16, ts"2020-11-30", "B9DV")
    )
    val mfTaxData = Seq(
      (1, ts"2020-11-01", "M19B"),
      (1, ts"2020-11-21", "M19R"),
      (3, ts"2020-11-03", "M19D"),
      (3, ts"2020-11-23", "M19Q"),
      (5, ts"2020-11-30", "M142"),
      (11, ts"2020-11-10", "M19D"),
      (13, ts"2020-11-20", "M19R"),
      (15, ts"2020-11-30", "M19R")
    )
    val vbTransactionData = Seq(
      (2, ts"2021-01-22", "DDP"),
      (2, ts"2021-01-23", "ACH"),
      (4, ts"2021-01-24", "DDPR"),
      (4, ts"2021-01-25", "BAD"),
      (6, ts"2021-01-26", "WTRF"),
      (12, ts"2021-02-01", "MMDD")
    )
    val (mf1, mf2) = ("0100", "0301")
    val early = ts"2021-01-04"
    val end = ts"9999-12-31"
    val taTransactionData = Seq(
      (100, ts"2021-01-22", "5003", mf1),
      (101, ts"2021-01-06", "5004", mf2),
      (102, ts"2021-01-23", "5001", mf1),
      (300, ts"2021-01-24", "5011", mf1),
      (301, ts"2021-01-25", "9999", mf2),
      (500, ts"2021-01-26", "4124", mf2),
      (110, ts"2021-01-25", "6256", mf2)
    )
    val mfPosnData = Seq(
      (1, 100, mf1, early, end, 5, null),
      (1, 101, mf2, early, end, 5, null),
      (1, 102, mf1, early, end, 5, null),
      (3, 300, mf1, early, end, 5, null),
      (3, 301, mf2, early, end, 5, null),
      (5, 500, mf2, early, end, 5, null),
      (11, 110, mf2, early, end, 5, null)
    )
    val acctData = 1 to 10
    val acctRstrnData = Seq.empty[(String, String, String, String)]
    val expected = Set(
      ExpectedContactRecord(1, ts"2020-11-21", taxForm1099ContactType),
      ExpectedContactRecord(1, ts"2021-01-22", recurringTransContactType),
      ExpectedContactRecord(1, ts"2021-01-23", nonRecurringTransContactType),
      ExpectedContactRecord(2, ts"2020-11-22", taxForm1099ContactType),
      ExpectedContactRecord(2, ts"2021-01-22", recurringTransContactType),
      ExpectedContactRecord(2, ts"2021-01-23", nonRecurringTransContactType),
      ExpectedContactRecord(3, ts"2020-11-23", taxForm1099ContactType),
      ExpectedContactRecord(3, ts"2021-01-24", recurringTransContactType),
      ExpectedContactRecord(4, ts"2020-11-24", taxForm1099ContactType),
      ExpectedContactRecord(4, ts"2021-01-24", recurringTransContactType),
      ExpectedContactRecord(5, ts"2020-11-30", taxForm1042sContactType),
      ExpectedContactRecord(5, ts"2021-01-26", nonRecurringTransContactType),
      ExpectedContactRecord(6, ts"2020-11-29", taxForm1042sContactType),
      ExpectedContactRecord(6, ts"2021-01-26", nonRecurringTransContactType),
      ExpectedContactRecord(11, ts"2021-01-25", recurringTransContactType),
      ExpectedContactRecord(12, ts"2021-02-01", recurringTransContactType)
    )

    when(tables.vvbsTaxForm).thenReturn(vbsTaxData.toDF(acctId, versnTs, "tax_form_nm"))
    when(tables.vmfTaxForm).thenReturn(mfTaxData.toDF(acctId, versnTs, "tax_form_typ"))
    when(tables.vacct).thenReturn(acctData.toDF(acctId))
    when(tables.vacctRstrn).thenReturn(acctRstrnData.toDF(acctId, rstrnTypCode, beginTimestamp, endTimestamp))

    when(tables.vtxnsRtlFinHist).thenReturn(taTransactionData.toDF(accountNumber, taTransactionDate, taTransactionCode, portId))
    when(tables.vmutlFndPosn).thenReturn(mfPosnData.toDF(acctId, vastAcctNumber, portId, beginDate, endDate, statusCode, statusChangeDate))
    when(tables.vbrkgTran).thenReturn(vbTransactionData.toDF(acctId, vbTransactionDate, vbTransactionCode))

    val underTest = new AccountContactEventTable(spark, tables, "bucket")

    underTest.output expectingColumns(acctId, lastContactTs, contactType, contactDescription) shouldBeEqualTo
      (expected onColumns(acctId, lastContactTs, contactType))
  }
}