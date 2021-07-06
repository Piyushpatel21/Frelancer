package com.vanguard.rdna.abx
package table

import com.vanguard.rdna.abx.derived.ContactTypes
import org.apache.spark.sql.functions.{format_string, trim}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

class ClientContactEventTable(spark: SparkSession, tables: ABXTables, bucket: String)
extends ABXContactTable(spark, bucket, clientContactEventTableName, clientPoid) {
  import spark.implicits._

  private val basicContactTypes = contactType <== ContactTypes.column

  private val basicDescription = contactDescription <== format_string("communication_channel=%s, contact_type=%s, contact_sub_type=%s",
    trim(channelCode.column), trim(contactCode.column), trim(contactTypeCode.column))

  lazy val contactData: DataFrame = {
    val phauContactTable = tables.tcntct.select(clientPoid, "init_ts" ==> lastContactTs, basicContactTypes, basicDescription).where(contactType.column.isNotNull)

    // Find out if we also need vgi_org_id = 1601
    val webLogonData = tables.vsectyCred
      .where($"secty_domn_id" === 2)
      .select(poId ==> clientPoid, lastLogonTs ==> lastContactTs, contactType <== webLogonContactType) +
      (contactDescription <== "WEB_LOGON")

    phauContactTable.union(webLogonData.where(lastContactTs.column.isNotNull))
  }
}
