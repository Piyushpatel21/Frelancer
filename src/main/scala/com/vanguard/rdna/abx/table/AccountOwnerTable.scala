package com.vanguard.rdna.abx
package table

import com.vanguard.rdas.rc.spark.{GenericColumn, TableColumn, TableSchema}
import com.vanguard.rdna.abx.derived.EdeliveryPreferences
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object AccountOwnerTable {
  val BasicOwnerColumns: List[GenericColumn] = List(
    poId, poRoleCode, firstName, middleName, lastName, orgName, emailAddress,
    semsStatusCd, emailBounceIndicator, tinId, tinType, birthDate, isDeceased, deceasedDate, vndrVrfDeathDate, deathCertFileDate
  )

  val EDeliveryColumns: List[GenericColumn] = List(
    eDeliveryStatements, lastUpdatedEDeliveryStatements, eDeliveryConfirmation, lastUpdatedEDeliveryConfirmation,
    eDeliveryReports, lastUpdatedEDeliveryReports, eDeliveryProxy, lastUpdatedEDeliveryProxy,
    eDeliveryNotices, lastUpdatedEDeliveryNotices, eDeliveryTaxForm, lastUpdatedEDeliveryTaxForm
  )

  val schema: TableSchema = TableSchema(List(acctId) ::: BasicOwnerColumns ::: EDeliveryColumns)

  val busSagId: GenericColumn = TableColumn("bus_sag_id")
  val busBeginDate: GenericColumn = TableColumn("bus_bgn_dt")
  val busEndDate: GenericColumn = TableColumn("bus_end_dt")
  val webSagBeginDate: GenericColumn = TableColumn("web_sag_bgn_dt")
  val webSagEndDate: GenericColumn = TableColumn("web_sag_end_dt")
  val tempPoId: GenericColumn = TableColumn("temp_po_id")
}

class AccountOwnerTable(spark: SparkSession, tables: ABXTables, bucket: String, accountData: => DataFrame)
extends ABXOutputTable(spark, bucket, abxDB, "account_owner", AccountOwnerTable.schema) {
  import AccountOwnerTable._
  import tables._

  /**
   * Appends e-delivery preferences.
   */
  def addEdeliveryPreferences(df: DataFrame): DataFrame = {
    val owPoids = vsagBusRlshp.where(trim(poRoleCode.column) === "OW")
      .select(sagId ==> busSagId, poId, beginDate ==> busBeginDate, endDate ==> busEndDate)
    val webSags = vsag.where(servId.column === 2)
      .select(sagId ==> sagWebRgstrnId, beginDate ==> webSagBeginDate, endDate ==> webSagEndDate)
    val webRgstrnIds = df.join(owPoids, poId)
      .where(busBeginDate.column <= sagEndDate.column && busEndDate.column >= sagEndDate.column)
      .select(poId, busSagId, sagBeginDate, sagEndDate)
      .join(webSags, busSagId.column === sagWebRgstrnId.column)
      // not including webSagBeginDate.column <= sagEndDate.column check below because a lot of
      // webSagBeginDate's are after sagEndDate for valid ones
      .where(webSagEndDate.column >= sagEndDate.column)
      .deduplicate(Seq(poId.column), Seq(webSagEndDate.column.desc, webSagBeginDate.column.asc))
      // Rename po_id to temp_po_id below to work around the Spark issue
      // http://issues.apache.org/jira/browse/SPARK-14948
      .select(poId ==> tempPoId, sagWebRgstrnId)

    val cachedDF = df.join(webRgstrnIds, poId.column === tempPoId.column, "left").cache()
    new EdeliveryPreferences(spark, tables)
      .appendPreferences(cachedDF)
      .cache()
      .debugLogging("appendEdeliveryPreferences")
  }
  
  lazy val emailDF : DataFrame = {
    val activeEmails = velctronicAddr
      .where(endTimestamp.column > current_timestamp())
      .select(frmttdAddr ==> emailAddress, semsStatusCd, statusCode, addressId, invldEmailCnt)

    vcntctPref
      .where(endTimestamp.column > current_timestamp() && trim(usageCd.column) === "EMAL" && trim(purpCd.column) === "WEBP")
      .deduplicate(poId.column, lastUpdatedTimestamp.column.desc)
      .select(addressId, poId)
      .join(activeEmails, addressId)
      .join(vcin170.where(trim(webFl.column) === "Y").select(vgiClientId ==> poId).distinct(), poId)
      .withColumn(emailBounceIndicator, flag(trim(semsStatusCd.column).isin("R","U","X") && invldEmailCnt.column >= 4 ))
  }

  lazy val output: DataFrame = {
    val withPoids = accountData
      .select(acctId, accountType, altnSagId, sagBeginDate, sagEndDate)
      .join(vsagBusRlshp.select(sagId, poId, poRoleCode, beginDate, endDate), altnSagId.column === sagId.column)
      .where(beginDate.column <= sagEndDate.column && endDate.column >= sagEndDate.column)
      .select(acctId, accountType, poId, poRoleCode, sagBeginDate, sagEndDate)

    addEdeliveryPreferences(withPoids.cache())
      .join(vcin24.select(deceasedDate, isDeceased,vndrVrfDeathDate, deathCertFileDate, birthDate, vgiClientId ==> poId), poId,"left")
      .join(vcin22, poId.column === vgiClientId.column,"left")
      .join(emailDF.select(poId, emailAddress, semsStatusCd, emailBounceIndicator), poId,"left")
  }

  override def capture(recreate: Boolean): Unit = super.capture(true)
}

