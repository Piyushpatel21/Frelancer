package com.vanguard.rdna.abx
package table

import java.sql.Timestamp

import org.apache.spark.sql.DataFrame
import org.mockito.Mockito.when
import org.scalatest.WordSpec

import scala.collection.mutable.ListBuffer

case class ExpectedAccountOwner(acct_id: Int, po_id: Int, po_Role_Code: String, firstName: String, middleName: String, lastName: String,
                                orgName: String, email_Address: String, sems_Status_Cd: String, emailBounceIndicator: String, tinId: String, tinType: String, birthDate: Timestamp,
                                isDeceased: String, deceasedDate: Timestamp, vndrVrfDeathDate: Timestamp, deathCertFileDate: Timestamp)


class AccountOwnerTableTest extends WordSpec with ABXTestSupport {
  import ABXTestSupport.SQLDateInterpolator
  import spark.implicits._

  val earlyTs = ts"2020-06-13"
  val randomTimestamp = ts"2020-12-10 16:18:45.133"
  val lateTimestamp = ts"9999-12-31"

  val dummyAccountData = Seq(
    (100, 100, earlyTs, randomTimestamp, "VBA"),
    (101, 101, earlyTs, lateTimestamp, "VBA"),
    (102, 102, earlyTs, lateTimestamp, "VBS"),
    (103, 103, earlyTs, lateTimestamp, "VBA")
  ).toDF(acctId, altnSagId, sagBeginDate, sagEndDate, accountType)

  val dummyBusRlshpData = Seq(
    (100, 100, "PRRT", randomTimestamp, lateTimestamp),
    (101, 101, "PRRT", randomTimestamp, lateTimestamp),
    (102, 102, "PRRT", randomTimestamp, lateTimestamp)
  ).toDF(sagId, poId, poRoleCode, beginDate, endDate)

  val dummyRlshpData = Seq((100, 100, 100), (101, 101, 101), (102, 102, 102)).toDF(memberSagId, ownerSagId, sagId)

  val dummyEmailTelctronicAddr = Seq(
    (lateTimestamp, "a@a.com", "R", 100, 100, 3),
    (lateTimestamp, "a@a.com", "R", 101, 101, 3),
    (lateTimestamp, "a@a.com", "R", 102, 102, 3)
  ).toDF(endTimestamp, frmttdAddr, semsStatusCd, addressId, statusCode, invldEmailCnt)

  val dummyEmailPref = Seq(
    (lateTimestamp, "EMAL", "WEBP", 199, 100, ts"2018-01-02 01:02:03.230"),
    (lateTimestamp, "EMAL", "WEBP", 100, 100, ts"2018-01-02 01:02:03.234"),
    (lateTimestamp, "EMAL", "WEBP", 101, 101, ts"2018-03-04 00:00:00.000"),
    (lateTimestamp, "EMAL", "WEBP", 102, 102, ts"2018-05-06 00:00:00.000")
  ).toDF(endTimestamp, usageCd, purpCd, addressId, poId, lastUpdatedTimestamp)

  val dummyEmailTcin170 = Seq(
    (100, "Y", randomTimestamp, randomTimestamp),
    (101, "Y", randomTimestamp, randomTimestamp),
    (102, "Y", randomTimestamp, randomTimestamp)
  ).toDF(vgiClientId, webFl, "efftv_dt", lastUpdatedTimestamp)

  val dummyClientInfoData = Seq(
    (100, "test", "a", "user", "org", "tinId", "tinType"),
    (101, "test", "a", "user", "org", "tinId", "tinType"),
    (102, "test", "a", "user", "org", "tinId", "tinType")
  ).toDF(vgiClientId, firstName, middleName, lastName, orgName, tinId, tinType)

  val dummyClientInfoData24: DataFrame = Seq(
    (100, "N", randomTimestamp, randomTimestamp, randomTimestamp, randomTimestamp),
    (101, "N", randomTimestamp, randomTimestamp, randomTimestamp, randomTimestamp),
    (102, "N", randomTimestamp, randomTimestamp, randomTimestamp, randomTimestamp),
    (105, "N", randomTimestamp, randomTimestamp,randomTimestamp, randomTimestamp)
  ).toDF(vgiClientId, isDeceased, deceasedDate, birthDate,vndrVrfDeathDate,deathCertFileDate)

  val expected = Set(
    ExpectedAccountOwner(100, 100, "PRRT", "test", "a", "user", "org", "a@a.com", "R", "N", "tinId", "tinType", randomTimestamp, "N", randomTimestamp, randomTimestamp, randomTimestamp),
    ExpectedAccountOwner(101, 101, "PRRT", "test", "a", "user", "org", "a@a.com", "R", "N", "tinId", "tinType", randomTimestamp, "N", randomTimestamp, randomTimestamp, randomTimestamp),
    ExpectedAccountOwner(102, 102, "PRRT", "test", "a", "user", "org", "a@a.com", "R", "N", "tinId", "tinType", randomTimestamp, "N", randomTimestamp, randomTimestamp, randomTimestamp)
  )

  lazy val underTest = new AccountOwnerTable(spark, tables, "bucket", dummyAccountData)

  val openTs = ts"9999-12-31"

  def createTservRecords(sag: Int, start: Timestamp, end: Timestamp, isTa: Boolean, statement: Boolean = false, conf: Boolean = false,
                         report: Boolean = false, notices: Boolean = false, taxForm: Boolean = false): Seq[(Int, Int, Timestamp, Timestamp, Timestamp, String)] = {
    val buffer = new ListBuffer[(Int, Int, Timestamp, Timestamp, Timestamp, String)]
    val isOpen = openTs == end
    val (updateTs, delCode) = if (isOpen) (start, "YE") else (end, "NO")

    def createRecord(id: Int): (Int, Int, Timestamp, Timestamp, Timestamp, String) = (sag, id, start, end, updateTs, delCode)

    if (statement) buffer += createRecord(208)
    if (conf && isTa) buffer += createRecord(214)
    if (report) buffer += createRecord(if (isTa) 205 else 248)
    if (notices) buffer += createRecord(239)
    if (taxForm) buffer += createRecord(237)

    buffer
  }

  "appendEdeliveryPreferences" should {
    import spark.implicits._
    val startDate = dt"2021-03-01"
    val tsag = Seq((11, startDate, openEndDate, 2), (21, startDate, openEndDate, 2), (31, startDate, openEndDate, 2),
      (51, startDate, openEndDate, 2), (61, startDate, openEndDate, 2)).toDF(sagId, beginDate, endDate, servId)
    val tsagBusRlshp = Seq(
      (11, 1, startDate, openEndDate, "OW"),
      (21, 2, startDate, openEndDate, " OW"),
      (31, 3, startDate, openEndDate, "OW "),
      (51, 5, startDate, openEndDate, "OW"),
      (61, 6, startDate, openEndDate, "OW ")
    ).toDF(sagId, poId, beginDate, endDate, poRoleCode)

    val earlyTs = ts"2021-03-02"
    val middleTs = ts"2021-03-03"
    val lateTs = ts"2021-03-04"

    val twebRgstrnOpt = Seq(
      (11, "", "", "", earlyTs, earlyTs),
      (31, "EMAL", "", "EMAL", lateTs, lateTs),
      (51, "MAIL", "", "MAIL", lateTs, lateTs),
      (51, "EMAL", "", "", earlyTs, lateTs),
      (61, "MAIL", "", "", lateTs, lateTs)
    ).toDF(sagWebRgstrnId, brokerageConfirmationCode, vboConfirmationCode, proxyStatementCode, webRgstrnOptTimestamp, lastUpdatedTimestamp)

    val tservAgrmtWebOp = Seq(
      createTservRecords(11, earlyTs, openTs, false, true, true, true, true, true),
      createTservRecords(21, earlyTs, openTs, true, true, true, true, true, true),
      createTservRecords(31, earlyTs, middleTs, false, true, true, true, true, true),
      createTservRecords(31, middleTs, openTs, false),
      createTservRecords(51, earlyTs, openTs, false, true, true, false, true, true),
      createTservRecords(61, earlyTs, openTs, false, true, true, true, true, true)
    ).flatten.toDF(sagWebRgstrnId, prodtFeatureId, beginTimestamp, endTimestamp, lastUpdatedTimestamp, deliveryModeCode)

    when(tables.vsag).thenReturn(tsag)
    when(tables.vsagBusRlshp).thenReturn(tsagBusRlshp)
    when(tables.vwebRgstrnOpt).thenReturn(twebRgstrnOpt)
    when(tables.vservAgrmtWebOp).thenReturn(tservAgrmtWebOp)

    "add the edelivery preferences based on the tserv_agrmt_web_op and tweb_rgstrn_opt tables (brokerage only)" when {
      "brokerage account" in {
        val base = Seq(
          (10, 1, "PRRT", "VBA", startDate, openEndDate),
          (30, 3, "PRRT", "VBA", startDate, openEndDate),
          (50, 5, "PRRT", "VBA", startDate, openEndDate),
          (60, 6, "PRRT", "VBA", startDate, openEndDate)
        ).toDF(sagId, poId, poRoleCode, accountType, sagBeginDate, sagEndDate)
        val expected = Seq(
          (10, 1, "N", "Y", "Y", "Y", "Y", "Y", earlyTs, earlyTs, earlyTs, earlyTs, null, earlyTs),
          (30, 3, "Y", "N", "N", "N", "N", "N", middleTs, middleTs, middleTs, middleTs, lateTs, middleTs),
          (50, 5, "N", "Y", "Y", "Y", "N", "N", earlyTs, earlyTs, null, earlyTs, lateTs, null),
          (60, 6, "N", "Y", "Y", "Y", "Y", "Y", earlyTs, earlyTs, earlyTs, earlyTs, lateTs, earlyTs)
        )

        underTest.addEdeliveryPreferences(base) shouldBeEqualTo
          (expected onColumns(sagId, poId, eDeliveryConfirmation, eDeliveryStatements, eDeliveryTaxForm, eDeliveryNotices, eDeliveryReports, eDeliveryProxy,
            lastUpdatedEDeliveryStatements, lastUpdatedEDeliveryNotices , lastUpdatedEDeliveryProxy, lastUpdatedEDeliveryTaxForm, lastUpdatedEDeliveryConfirmation, lastUpdatedEDeliveryReports))
      }
     "TA account" in {
       val base = Seq(
         (20, 2, "PRRT", "TA", startDate, openEndDate),
         (40, 3, "PRRT", "TA", startDate, openEndDate),
         (70, 5, "PRRT", "TA", startDate, openEndDate)
       ).toDF(sagId, poId, poRoleCode, accountType, sagBeginDate, sagEndDate)
       val expected = Seq(
         (20, 2, "Y", "Y", "Y", "Y", "Y", "N", earlyTs, earlyTs, null, earlyTs, earlyTs, earlyTs),
         (40, 3, "N", "N", "N", "N", "N", "Y", middleTs, middleTs, lateTs, middleTs, null, null),
         (70, 5, "N", "Y", "Y", "Y", "N", "N", earlyTs, earlyTs, lateTs, earlyTs, null, null)
       )

       underTest.addEdeliveryPreferences(base) shouldBeEqualTo
         (expected onColumns(sagId, poId, eDeliveryConfirmation, eDeliveryStatements, eDeliveryTaxForm, eDeliveryNotices, eDeliveryReports, eDeliveryProxy,
           lastUpdatedEDeliveryStatements, lastUpdatedEDeliveryNotices, lastUpdatedEDeliveryProxy, lastUpdatedEDeliveryTaxForm, lastUpdatedEDeliveryConfirmation, lastUpdatedEDeliveryReports))
     }
    }
  }

  "output" should {
    "equal all of the contact records pulled from the necessary tables" in {
      when(tables.vsagBusRlshp).thenReturn(dummyBusRlshpData)
      when(tables.vsagRlshp).thenReturn(dummyRlshpData)
      when(tables.vcntctPref).thenReturn(dummyEmailPref)
      when(tables.velctronicAddr).thenReturn(dummyEmailTelctronicAddr)
      when(tables.vcin170).thenReturn(dummyEmailTcin170)
      when(tables.vcin24).thenReturn(dummyClientInfoData24)
      when(tables.vcin22).thenReturn(dummyClientInfoData)

      underTest.output shouldBeEqualTo (expected onColumns(acctId, poId, poRoleCode, firstName, middleName, lastName,
        orgName, emailAddress, semsStatusCd, emailBounceIndicator, tinId, tinType, birthDate, isDeceased, deceasedDate, vndrVrfDeathDate, deathCertFileDate))
    }
  }
}
