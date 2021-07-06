package com.vanguard.rdna.abx
package table

import java.sql.Timestamp

import org.mockito.Mockito.when
import org.scalatest.FunSuite

class TaxFormContactEventTest extends FunSuite with ABXTestSupport {
  import ABXTestSupport.SQLDateInterpolator
  import spark.implicits._

  test("MF contact events outside of RPO active windows are all included") {
    val vbsTaxData = Seq.empty[(Int, Timestamp, String)]
    val mfTaxData = Seq(
      (1, ts"2017-06-07", "M19R"),
      (1, ts"2018-04-09", "M142"),
      (1, ts"2019-08-17", "M19B"),
      (1, ts"2020-11-21", "M19D")
    )
    val acctData = 1 to 10
    val mutlFndPosnData = Seq(
      (1, "04", ts"2020-11-22", ts"9999-12-31", ts"2018-12-01")
    )
    val acctRstrnData = Seq.empty[(Int, String, String, String)]
    val expected = Set(
      ExpectedContactRecord(1, ts"2017-06-07", taxForm1099ContactType),
      ExpectedContactRecord(1, ts"2018-04-09", taxForm1042sContactType)
    )

    when(tables.vvbsTaxForm).thenReturn(vbsTaxData.toDF(acctId, versnTs, "tax_form_nm"))
    when(tables.vmfTaxForm).thenReturn(mfTaxData.toDF(acctId, versnTs, "tax_form_typ"))
    when(tables.vacct).thenReturn(acctData.toDF(acctId))
    when(tables.vmutlFndPosn).thenReturn(mutlFndPosnData.toDF(acctId, statusCode, beginDate, endDate, statusChangeDate))
    when(tables.vacctRstrn).thenReturn(acctRstrnData.toDF(acctId, rstrnTypCode, beginTimestamp, endTimestamp))

    val underTest = new TaxFormContactEvent(spark, tables, 5)

    underTest.getEvents expectingColumns(acctId, lastContactTs, contactType, contactDescription) shouldBeEqualTo
      (expected onColumns(acctId, lastContactTs, contactType))
  }
}