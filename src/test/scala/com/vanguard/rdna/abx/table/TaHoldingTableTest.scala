package com.vanguard.rdna.abx
package table

import com.vanguard.rdna.abx.ABXTestSupport.SQLDateInterpolator
import org.mockito.Mockito.when
import org.scalatest.FunSuite

import scala.math.BigDecimal.RoundingMode
import scala.util.Random

class TaHoldingTableTest extends FunSuite with ABXTestSupport {
  import spark.implicits._

  def randomDouble(scale: Int, max: Int = 10000): Double = BigDecimal(Random.nextInt(max) * Random.nextDouble()).setScale(scale, RoundingMode.HALF_EVEN).toDouble

  val accountDF: Seq[(Int, String)] = Seq((1, "TA"), (2, "TA"))

  val currentPrice = randomDouble(8)
  val prevPrice = randomDouble(8)
  val currentPriceDate = ts"2021-03-22"
  val lastUpdatedAt = ts"2021-03-24"

  val tissPrcVgi = Seq(
    (1000, "USD", "NAV", currentPriceDate, currentPrice, lastUpdatedAt),
    (1000, "USD", "NAV", ts"2021-03-19", prevPrice, lastUpdatedAt),
    (1000, "USD", "NAV", currentPriceDate, currentPrice, currentPriceDate),
    (1000, "EURO", "NAV", currentPriceDate, currentPrice * .84, lastUpdatedAt)
  )

  val longName = "TEST FUND LONG NAME"
  val ticker = "TFLN"

  val bs1 = randomDouble(5, 100)
  val is1 = randomDouble(5, 100)
  val bs2 = randomDouble(5, 100)
  val is2 = randomDouble(5, 100)

  val startDate = dt"2021-03-15"
  val openDate = dt"9999-12-31"
  val closedDate = dt"2021-03-17"

  val tcVgiIntIns: Seq[(Int, Int, Int, String, String, String)] = Seq((1000, 100, 10000, ticker, longName, "ACTV"))
  val tmutlFndPosn = Seq(
    (1, 10, 100, bs1, is1, startDate, openDate),
    (2, 20, 100, bs2, is2, startDate, openEndDate),
    (1, 10, 100, bs2, is2, startDate, closedDate)
  )

  test("TA Holdings Output") {
    when(tables.vissPrcVgi).thenReturn(tissPrcVgi.toDF(insId, currencyCode, priceTypeCode, priceEffectiveDate, "unit_am", lastUpdatedTimestamp))
    when(tables.vcVgiIntIns).thenReturn(tcVgiIntIns.toDF(insId, portId, cusip, "tckr_sym", "lng_nm", mutualFundStatusCode))
    when(tables.vmutlFndPosn).thenReturn(tmutlFndPosn.toDF(acctId, vastAcctNumber, portId, "book_shr_qy", "iss_shr_qy", beginDate, endDate))

    val expected = Seq(
      (1, 100, 10, 10000, ticker, longName, is1, bs1, currentPrice, (is1 + bs1) * currentPrice, currentPriceDate),
      (2, 100, 20, 10000, ticker, longName, is2, bs2, currentPrice, (is2 + bs2) * currentPrice, currentPriceDate)
    )

    new TaHoldingTable(spark, tables, "bucket", accountDF.toDF(acctId, accountType)).output shouldBeEqualTo
      expected.withDoubleTolerance(.005).onColumns(acctId, fundId, vastAcctNumber, cusip, tickerSymbol,
        fundLongName, issueShareQuantity, bookShareQuantity, nav, marketValue, marketValueDate)
  }
}
