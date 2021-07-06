package com.vanguard.rdna.abx
package table

import org.mockito.Mockito.when
import org.scalatest.WordSpec

class ClientContactEventTableTest extends WordSpec with ABXTestSupport {
  import ABXTestSupport.SQLDateInterpolator
  import spark.implicits._

  val latestDate = ts"2020-11-30"
  val earlierDate = ts"2020-11-29"
  val earliestDate = ts"2020-11-01"
  val endDate = dt"9999-12-31"

  val webLogonData1 = Seq((1, latestDate, 2), (2, earlierDate, 2), (4, earliestDate, 2), (7, latestDate, 1))
  val phauData = Seq((1, earlierDate, clamContactType, "TOAI", "PH"), (2, latestDate, clamContactType, "TOAI", "WEB"),
    (3, earliestDate, "PHAU", "MDFY", "PH"), (5, earlierDate, clamContactType, "VCCS", "PH"), (7, earlierDate, "AGAP", "ASCP", "PH"))

  val expected = Set(ExpectedContactRecord(1, latestDate, webLogonContactType), ExpectedContactRecord(2, earlierDate, webLogonContactType),
    ExpectedContactRecord(3, earliestDate, phoneContactType), ExpectedContactRecord(4, earliestDate, webLogonContactType), ExpectedContactRecord(5, earlierDate, clamContactType),
    ExpectedContactRecord(7, earlierDate, inPersonContactType), ExpectedContactRecord(1, earlierDate, clamContactType))

  lazy val underTest = new ClientContactEventTable(spark, tables, "bucket")

  "output" should {
    "equal all of the client level contacts pulled from the necessary tables" in {
      when(tables.vsectyCred).thenReturn(webLogonData1.toDF(poId.name, lastLogonTs.name, "secty_domn_id"))
      when(tables.tcntct).thenReturn(phauData.toDF(clientPoid.name, "init_ts", contactCode.name, contactTypeCode.name, channelCode.name))
      underTest.output.show
      underTest.output expectingColumns(clientPoid, lastContactTs, contactType, contactDescription) shouldBeEqualTo
        (expected onColumns(clientPoid, lastContactTs, contactType))
    }
  }
}
