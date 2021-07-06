package com.vanguard.rdna.abx
package table

import java.sql.Timestamp

import org.mockito.Mockito.when
import org.scalatest.WordSpec

class AccountBaseTableTest extends WordSpec with ABXTestSupport {
  import ABXTestSupport.SQLDateInterpolator
  import spark.implicits._

  "accountBaseData" should {
    "return the base data with IIG-related accounts removed" in {
      val abdTsagAacctRlshp = Seq((1L, 10L, "ACCT"), (2L, 20L, "ACCT"), (3L, 30L, "ACCT"))
      val abdTacct = Seq((1L, "0000"), (2L, null), (3L, "0001"))
      val abdTsag = Seq(
        (10L, 9,  dt"2021-01-01", dt"2021-01-08", Timestamp.from(java.time.Instant.now)),
        (20L, 90, dt"2021-01-01", dt"9999-12-31", Timestamp.from(java.time.Instant.now)),
        (30L, 8,  dt"2021-01-01", dt"2021-01-08", Timestamp.from(java.time.Instant.now)),
        (30L, 65, dt"2021-01-01", dt"9999-12-31", Timestamp.from(java.time.Instant.now))
      )

      when(tables.vsagAcctRlshp).thenReturn(abdTsagAacctRlshp.toDF("rlshp_id", sagId, rlshpTypCode))
      when(tables.vacct).thenReturn(abdTacct.toDF(acctId, rtrmtPlnTypCode))
      when(tables.vsag).thenReturn(abdTsag.toDF(sagId, servId, beginDate, endDate, lastUpdatedTimestamp))

      val underTest = new AccountBaseTable(spark, tables, "bucket")
      val acctIds: List[Long] = underTest.getAccountBaseData.select(acctId).collect().map(_.getLong(0)).toList
      acctIds should not contain 3L
    }
  }

  val startDate = dt"2021-01-01"
  val closedDate = dt"2021-01-08"
  val yesterday = Timestamp.from(java.time.Instant.now)

  val baseColumns = List(acctId, rtrmtPlnTypCode, sagId, servId, sagBeginDate, sagEndDate)

  val tsagAcctRlshp = Seq((1, 10, "ACCT"), (2, 20, "ACCT"), (3, 30, "ACCT"), (4, 40, "ACCT"), (6, 60, "ACCT"), (6, 61, "ACCT"), (7, 11, "ACCT"))
  val tacct = Seq((1, "0000"), (2, "0000"), (3, "0001"), (4, "0010"), (6, null), (7, null))
  val tsag = Seq(
    (10, 9, startDate, closedDate, yesterday),
    (20, 90, startDate, openEndDate, yesterday),
    (30, 8, startDate, openEndDate, yesterday),
    (31, 65, startDate, closedDate, yesterday),
    (40, 38, startDate, openEndDate, yesterday),
    (50, 3, startDate, openEndDate, yesterday),
    (60, 8, startDate, openEndDate, yesterday),
    (61, 65, startDate, openEndDate, yesterday),
    (11, 8, startDate, closedDate, yesterday),
    (12, 2, startDate, openEndDate, yesterday),
    (22, 2, startDate, openEndDate, yesterday)
  )

  val brokeragePrefix = "1000000"

  val tacctAltnIdAsgn = Seq((1, 9, s"${brokeragePrefix}10"), (2, 9, s"${brokeragePrefix}20"), (5, 0, s"${brokeragePrefix}50"))

  val tsagVbsRgstrn = Seq((10, "01", 2, "50 MOREHALL RD", "MALVERN, PA, 19355", null, null, null, null, "MALVERN", "PA", "19355", "1724", "US", "N"),
    (20, "01", 4, "PALACE OF VERSAILLES", "PLACE D'ARMES", "78000 VERSAILLES", "FRANCE", null, null, "VERSAILLES", null, "78000", null, "FR", "Y"))

  val tsagAddrRlshp = Seq((11, 1, "RGST", "VALD", dt"2020-12-10"), (20, 2, "TAXR", "VALD", dt"2020-01-02"), (30, 3, "RGST  ", "  VALD", dt"2020-09-08"))
  val tcin15 = Seq((1, "50 MOREHALL RD", "MALVERN, PA, 19355-1724", null, null, "MALVERN", "PA", "19355", "1724", "US", "N", ts"2020-01-02 01:02:03"),
    (3, "2300 CHESTNUT ST", "PHILADELPHIA, PA, 19103-0300", null, null, "PHILADELPHIA", "PA", "19103", "0300", "US", "N", ts"2009-10-04 11:12:15"))

  val tagrmtRgstrnTx = Seq((30, 2, "UNIT", "TESTER A", null, null, null, null, "N"))

  val tsagBusRlshp = Seq(
    (11, Some(100), "PRRT", startDate, openEndDate),
    (12, Some(100), "OW ", startDate, openEndDate),
    (20, Some(200), "PRRT", startDate, openEndDate),
    (22, Some(200), "OW", startDate, openEndDate),
    (30, Some(300), "PRRT", startDate, openEndDate),
    (40, None, null, null, null)
  )

  val tdisinstSagRlshp = Seq((10, null))

  val tmutlFndPosn = Seq((3, "04", startDate, openEndDate), (3, " 05", startDate, openEndDate))
  val tacctPosnBrk = Seq((1, startDate), (2, startDate), (3, startDate))
  val tacctRstrn = Seq((1, "RPO", startDate, openEndDate), (2, "ESCH", startDate, openEndDate))

  val tsagRlshp = Seq((10, 11, 9))
  val tagrmtRgstrn = Seq((40, "04"), (30, "02"))

  val additionalOutputColumns = List(
    altnSagId, openClosedCode, accountType, taAcctId, brokerageAccountNumber, rgstrnNumberOfNameLines,
    rgstrnLine1, rgstrnLine2, rgstrnLine3, rgstrnLine4, rgstrnLine5, rgstrnLine6, rgstrnFognAddressFlag,
    rgstrnCityName, rgstrnStateCode, rgstrnZipCode, rgstrnZipPlus4, rgstrnCountryCode,
    streetAddressLine1, streetAddressLine2, streetAddressLine3, streetAddressLine4,
    cityName, stateCode, zipCode, zipPlus4, countryCode, fognAddressFlag,
    rgstrnTypCode, accountEstabDate, rpoIndicator, escheatedIndicator
  )

  "output" should {
    "contain all of the data for the account table" in {

      val expectedOutput = Seq(
        Seq(3, "0001", 30, 8, startDate, openEndDate, 30, "OPEN", "TA", null, null, 2, "UNIT", "TESTER A", null, null, null, null, "N", null, null, null, null, null,
          "2300 CHESTNUT ST", "PHILADELPHIA, PA, 19103-0300", null, null, "PHILADELPHIA", "PA", "19103", "0300", "US", "N", "02", startDate, "Y", "N"),
        Seq(4, "0010", 40, 38, startDate, openEndDate, 40, "OPEN", "TA", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, "04", null, "N", "N"),
        Seq(1, "0000", 10, 9, startDate, closedDate, 11, "CLOSED", "VBS", 7, "10000001", null, null, null, null, null, null, null,
          null, null, null, null, null, null, "50 MOREHALL RD", "MALVERN, PA, 19355-1724", null, null, "MALVERN", "PA", "19355", "1724", "US", "N", null, startDate, "Y", "N"),
        Seq(2, "0000", 20, 90, startDate, openEndDate, 20, "OPEN", "VBA", null, "10000002", 4, "PALACE OF VERSAILLES", "PLACE D'ARMES", "78000 VERSAILLES", "FRANCE",
          null, null, "Y", "VERSAILLES", null, "78000", null, "FR", null, null, null, null, null, null, null, null, null, null, "01", startDate, "N", "Y"),
        Seq(7, null, 11, 8, startDate, closedDate, 11, "CLOSED", "TA", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          "50 MOREHALL RD", "MALVERN, PA, 19355-1724", null, null, "MALVERN", "PA", "19355", "1724", "US", "N", null, null, "N", "N")
      )

      when(tables.vsagAcctRlshp).thenReturn(tsagAcctRlshp.toDF("rlshp_id", sagId, rlshpTypCode))
      when(tables.vacct).thenReturn(tacct.toDF(acctId, rtrmtPlnTypCode))
      when(tables.vsag).thenReturn(tsag.toDF(sagId, servId, beginDate, endDate, lastUpdatedTimestamp))

      when(tables.vsagBusRlshp).thenReturn(tsagBusRlshp.toDF(sagId, poId, poRoleCode, beginDate, endDate))
      when(tables.vdisinstSagRlshp).thenReturn(tdisinstSagRlshp.toDF(disSagId, sagRlshpTypCode))

      when(tables.vsagRlshp).thenReturn(tsagRlshp.toDF(memberSagId, ownerSagId, sagRlshpTypCode))
      when(tables.vagrmtRgstrn).thenReturn(tagrmtRgstrn.toDF(sagId, rgstrnTypCode))

      when(tables.vacctAltnIdAsgn).thenReturn(tacctAltnIdAsgn.toDF(acctId, "acct_altn_id_cd", "asgnmt_val"))
      when(tables.vmutlFndPosn).thenReturn(tmutlFndPosn.toDF(acctId, statusCode, beginDate, endDate))
      when(tables.vacctPosnBrk).thenReturn(tacctPosnBrk.toDF(acctId, beginDate))
      when(tables.vacctRstrn).thenReturn(tacctRstrn.toDF(acctId, rstrnTypCode, beginTimestamp, endTimestamp))
      when(tables.vagrmtRgstrn).thenReturn(tagrmtRgstrn.toDF(sagId, rgstrnTypCode))

      when(tables.vsagVbsRgstrn).thenReturn(tsagVbsRgstrn.toDF(sagId, rgstrnTypCode, "no_of_nm_lns",
        rgstrnLine1, rgstrnLine2, rgstrnLine3, rgstrnLine4, rgstrnLine5, rgstrnLine6,
        cityName, stateCode, zipCode, zipPlus4, countryCode, fognAddressFlag))

      when(tables.vagrmtRgstrnTx).thenReturn(tagrmtRgstrnTx.toDF(sagId, numberOfNameLines,
        rgstrnLine1, rgstrnLine2, rgstrnLine3, rgstrnLine4, rgstrnLine5, rgstrnLine6, fognAddressFlag))

      when(tables.vsagAddrRlshp).thenReturn(tsagAddrRlshp.toDF(sagId, addressId, "rlshp_typ_cd", "addr_status_cd", lastUpdatedTimestamp))
      when(tables.vcin15).thenReturn(tcin15.toDF(addressId, streetAddressLine1, streetAddressLine2, streetAddressLine3, streetAddressLine4,
        cityName, stateCode, zipCode, zipPlus4, countryCode, fognAddressFlag, lastUpdatedTimestamp))

      val underTest = new AccountBaseTable(spark, tables, "bucket")
      underTest.output shouldBeEqualTo (expectedOutput onColumns(baseColumns ::: additionalOutputColumns:_*))
    }
  }
}
