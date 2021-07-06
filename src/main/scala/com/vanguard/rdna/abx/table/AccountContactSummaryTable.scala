package com.vanguard.rdna.abx
package table

import com.vanguard.rdas.rc.spark.TableColumn
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class AccountContactSummaryTable(spark: SparkSession, bucket: String, clientContactEvent: => DataFrame, accountContactEvent: => DataFrame, accountOwner: => DataFrame)
extends ABXContactTable(spark, bucket, accountContactSummaryTableName, acctId) {
  import spark.implicits._

  private val contactRoleCodes = List(
    "SHJS", "SHSH", "SHGP", "SHLP", "SHNO", "SHPS", "SHMN", "SHAL", "SHRM", "SHBN", "PRRT",
    "CUCU", "CUGU", "CUPL", "CUUS", "CUPA", "CUCM", "CUCO", "CUAL", "CUSD", "CUSN", "TRTR",
    "BNBN", "GDLA", "GFAA", "GLAA", "EXEX", "EXAD", "FPOA"
  )

  def getClientLevelContacts: DataFrame = {
    // combine PHONE, CLAM, IN_PERSON as a single type: BASIC
    val (basicDF, otherDF) = accountOwner.where(trim(poRoleCode.column).isInCollection(contactRoleCodes))
      .join(clientContactEvent, clientPoid.column === poId.column)
      .select(acctId, lastContactTs, contactType, contactDescription)
      .cache()
      .split(contactType.column.isin(phoneContactType, clamContactType, inPersonContactType))
    val dedupBasicDF = basicDF.deduplicate(acctId.column, lastContactTs.column.desc)
      .withColumn(contactType, lit(basicContactType))
      .select(acctId, lastContactTs, contactType, contactDescription)
    dedupBasicDF.union(otherDF)
  }

  def getAccountLevelContacts: DataFrame = {
    // combine TAX_FORM_1099 and TAX_FORM_1042S as a single type: TAX_FORM
    val (taxFormDF, otherDF) = accountContactEvent.select(acctId, lastContactTs, contactType, contactDescription)
      .split(contactType.column.isin(taxForm1099ContactType, taxForm1042sContactType))
    val dedupTaxFormDF = taxFormDF.deduplicate(acctId.column, lastContactTs.column.desc)
      .withColumn(contactType, lit(taxFormContactType))
      .select(acctId, lastContactTs, contactType, contactDescription)
    dedupTaxFormDF.union(otherDF)
  }

  def getRelatedContacts: DataFrame = {
    val tempPoId = TableColumn("temp_po_id")
    val tempAcctId = TableColumn("temp_acct_id")

    val acctContactPoid: DataFrame = accountOwner.where(trim(poRoleCode.column) === "PRRT")
      .join(getAccountLevelContacts, acctId)
      .select(acctId ==> tempAcctId, poId ==> tempPoId, lastContactTs, contactType, contactDescription)

    accountOwner.where(trim(poRoleCode.column) === "PRRT")
      .join(acctContactPoid, poId.column === tempPoId.column && acctId.column =!= tempAcctId.column)
      .withColumn(contactType, concat(contactType.column, lit("_RELATED")))
      .select(acctId, lastContactTs, contactType, contactDescription)
  }

  lazy val contactData: DataFrame = getClientLevelContacts.union(getAccountLevelContacts).union(getRelatedContacts)
}
