package com.vanguard.rdna.abx
package table

import com.vanguard.rdas.rc.spark.{TableColumn, TableSchema}
import com.vanguard.rdna.abx.derived._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class AccountBaseTable(override val spark: SparkSession, tables: ABXTables, bucket: String)
extends ABXOutputTable(spark, bucket, abxDB, accountBaseTableName, AccountBaseTable.schema) {
  import spark.implicits._
  import tables._

  private val iigAcctId = TableColumn("iig_acct_id")
  private val iigSagBeginDate = TableColumn("iig_sag_bgn_dt")
  private val iigSagEndDate = TableColumn("iig_sag_end_dt")
  private val taSagId = TableColumn("ta_sag_id")
  private val taSagBeginDate = TableColumn("ta_sag_bgn_dt")
  private val taSagEndDate = TableColumn("ta_sag_end_dt")
  private val addrSagId = TableColumn("addr_sag_id")
  private val mbrAcctId = TableColumn("mbr_acct_id")
  private val tempAcctId = TableColumn("temp_acct_id")

  /**
   *  Get accounts by joining ''tacct'', ''tsag_acct_rlshp'' and ''tsag'' tables in the discovery layer.
   *  Note that no filtering or deduplication is applied to the resulting dataframe and the accounts
   *  (identified by account id) may not be unique. Account duplication typically happens when a VBS
   *  account is converted to VBA where the VBS sag is end dated and a new VBA sag is created.
   *  The resulting dataframe has the following columns:
   *  [[acctId]], [[rtrmtPlnTypCode]], [[sagId]], [[servId]], [[sagBeginDate]], [[sagEndDate]].
   *
   *  @param servIds a list of [[servId]]s for the desired account types
   */
  def getAccounts(servIds: Int*): DataFrame = {
    vacct.select(acctId, rtrmtPlnTypCode)
      .join(vsagAcctRlshp.where(trim(rlshpTypCode.column) === "ACCT").select(rlshpId ==> acctId, sagId).distinct, acctId)
      .join(vsag.where(servId.column.isin(servIds.map(lit): _*)), sagId)
      .select(acctId, rtrmtPlnTypCode, sagId, servId, beginDate ==> sagBeginDate, endDate ==> sagEndDate)
  }

  /**
   * Get unique accounts (identified by account id) by picking the record with the latest [[sagEndDate]].
   * If two records with the same account id have the same [[sagEndDate]], then the record with
   * the later [[sagBeginDate]] is picked for deduplication. The resulting dataframe has the following columns:
   *  [[acctId]], [[rtrmtPlnTypCode]], [[sagId]], [[servId]], [[sagBeginDate]], [[sagEndDate]].
   *
   *  @param servIds a list of [[servId]]s for the desired account types
   */
  def getUniqueAccounts(servIds: Int*): DataFrame = {
    getAccounts(servIds: _*)
      .deduplicate(Seq(acctId.column), Seq(sagEndDate.column.desc, sagBeginDate.column.desc))
      .select(acctId, rtrmtPlnTypCode, sagId, servId, sagBeginDate, sagEndDate)
  }

  /**
   * Get IIG accounts. The resulting dataframe contains the following
   * columns: [[acctId]], [[iigSagBeginDate]], [[iigSagEndDate]]
   */
  def getIigAccountData: DataFrame = {
    getAccounts(65)
      .select(acctId ==> iigAcctId, sagBeginDate ==> iigSagBeginDate, sagEndDate ==> iigSagEndDate)
  }

  /**
   * Get base account data after IIG exclusion. VBS-IIG account exclusion is done separately for VBS accounts.
   * Account ids are unique in the resulting dataframe which contains the following columns:
   * [[acctId]], [[rtrmtPlnTypCode]], [[sagId]], [[servId]], [[sagBeginDate]], [[sagEndDate]].
   */
  def getAccountBaseData: DataFrame = {
    val iigMatching = iigAcctId.column === acctId.column &&
      iigSagBeginDate.column <= sagEndDate.column && iigSagEndDate.column >= sagEndDate.column

    getUniqueAccounts(8, 38, 9, 90)
      .join(getIigAccountData, iigMatching, "left_anti")
      .cache()
      .debugLogging("after IIG exclusion")
  }

  implicit class AccountDataFrameAugmented(df: DataFrame) {
    /**
     * Derive the account open/closed status based on the [[sagEndDate]] column. It appends the [[openClosedCode]]
     * column to the dataframe.
     */
    def deriveAccountStatus: DataFrame = {
      df.withColumn(openClosedCode, flag(sagEndDate.column > current_timestamp(), "OPEN", "CLOSED"))
        .cache()
        .debugLogging("deriveAccountStatus")
    }

    /**
     * Derive the account type based on the [[servId]] value. When [[servId]] is 9, we rely on the
     * relationship data in the ''tdisinst_sag_rlshp'' table for VBS vs VBO accounts. It appends
     * the [[accountType]] column to the dataframe.
     */
    def deriveAccountType: DataFrame = {
      val columns: Array[String] = df.columns :+ accountType.name
      // the following distinct() call is necessary to avoid duplicates
      val disSagIds = vdisinstSagRlshp.where(sagRlshpTypCode.column === 10)
        .select(disSagId).distinct()
      df.join(disSagIds, sagId.column === disSagId.column, "left")
        .withColumn(AccountTypes.derived)
        .where(accountType.column.isNotNull)
        .select(columns.head, columns.tail: _*)
        .cache()
        .debugLogging("deriveAccountType")
    }

    /**
     * Set up VBS-TA linkage for VBS accounts with linked TA. It appends [[altnSagId]] and [[taAcctId]] to
     * the dataframe. VBS-IIG accounts are removed during this step.
     *
     * - For VBS accounts with linked TA: [[altnSagId]] is the linked TA account's sag id; [[taAcctId]] is
     * the linked TA account's account id.
     *
     * - For all other accounts, including VBS accounts without linked TA: [[altnSagId]] is the same as
     * [[sagId]]; [[taAcctId]] is ''null''.
     */
    def setupVbsTaLinkage: DataFrame = {
      val vbsTaRelations = vsagRlshp.where(sagRlshpTypCode.column === 9).select(memberSagId, ownerSagId)
      val vbsWithTaDF = df.where(accountType.column === "VBS")
        .join(vbsTaRelations, sagId.column === memberSagId.column)
        .join(getUniqueAccounts(8, 38).select(sagId ==> ownerSagId, acctId ==> taAcctId,
          sagBeginDate ==> taSagBeginDate, sagEndDate ==> taSagEndDate), ownerSagId)
        .select(acctId, taAcctId, ownerSagId ==> taSagId, taSagBeginDate, taSagEndDate)
        .cache()

      val IigMatching = iigAcctId.column === taAcctId.column &&
        iigSagBeginDate.column <= taSagEndDate.column && iigSagEndDate.column >= taSagEndDate.column
      val iigAcctIds = vbsWithTaDF.join(getIigAccountData, IigMatching).select(acctId).distinct().cache()
      logStdout(s"Found ${iigAcctIds.count()} VBS-IIG accounts")

      // Remove VBS-IIG accounts
      val nonVbsIigDF = vbsWithTaDF.join(iigAcctIds, acctId, "left_anti")

      // Find accounts with multiple TA links
      val dupCountDF = nonVbsIigDF.groupBy(acctId.column).count()
      // Rename acct_id to temp_acct_id below to work around the Spark issue
      // http://issues.apache.org/jira/browse/SPARK-14948
      val dupVbsTaAcctIds = dupCountDF.where($"count" >= 2).select(acctId ==> tempAcctId)
      val distinctVbsTaDF = vbsWithTaDF.join(dupVbsTaAcctIds, acctId.column === tempAcctId.column, "left_anti")
        .select(acctId, taAcctId, taSagId, taSagBeginDate, taSagEndDate)
      val dupAcctIds = dupVbsTaAcctIds.collect()
      if (dupAcctIds.length == 0)
        logStdout("All VBS accounts have single TA links")
      else
        logStdout(s"""Multiple TA accounts found for ${dupAcctIds.length} VBS-TA account ids: [${dupAcctIds.mkString(",")}]""")

      // deduplicate based on role code. Pick the record with the PRRT role.
      //TODO: We need to revisit whether this approach is legitimate.
      val dedupVbsTaDF = vbsWithTaDF.join(dupVbsTaAcctIds, acctId.column === tempAcctId.column)
        .join(vsagBusRlshp, sagId.column === taSagId.column)
        .where(beginDate.column <= taSagEndDate.column && endDate.column >= taSagEndDate.column)
        .deduplicate(acctId.column, when(trim(poRoleCode.column) === "PRRT", 0).otherwise(1).asc)
        .select(acctId, taAcctId, taSagId, taSagBeginDate, taSagEndDate)
        .cache()
      logStdout(s"Deduplicated ${dedupVbsTaDF.count()} VBS-TA accounts")

      // Rename acct_id to temp_acct_id below to work around the Spark issue
      // http://issues.apache.org/jira/browse/SPARK-14948
      val vbsTaDF = distinctVbsTaDF.union(dedupVbsTaDF).withColumnRenamed(acctId, tempAcctId)

      val columns = df.columns ++ List(altnSagId.name, taAcctId.name)
      df.join(iigAcctIds, acctId, "left_anti")
        .join(vbsTaDF, acctId.column === tempAcctId.column, "left")
        .withColumn(altnSagId <== coalesce(taSagId.column, sagId.column))
        .select(columns.head, columns.tail: _*)
        .cache()
        .debugLogging("setupVbsTaLinkage")
    }

    /**
     * Appends the [[brokerageAccountNumber]] column to the dataframe.
     */
    def appendBrokerageAccountNumber: DataFrame = {
      val getBrokerageAcctNumber = brokerageAccountNumber <== substring($"asgnmt_val", 1, 8)
      df.join(vacctAltnIdAsgn.where(accountAltnIdCode.column === 9).select(acctId, getBrokerageAcctNumber), acctId, "left")
        .cache()
        .debugLogging("appendBrokerageAccountNumber")
    }

    /**
     * Appends registration address to the dataframe.
     *
     * - For TA accounts, use the ''tagrmt_rgstrn_tx'' table
     *
     * - For other accounts, use the ''tsag_vbs_rgstrn'' table
     */
    def appendRegistrationAddress: DataFrame = {
      val sharedCols = Seq(sagId ==> altnSagId, numberOfNameLines ==> rgstrnNumberOfNameLines, rgstrnLine1, rgstrnLine2,
        rgstrnLine3, rgstrnLine4, rgstrnLine5, rgstrnLine6, fognAddressFlag ==> rgstrnFognAddressFlag)
      val vbsOnly = Seq(cityName ==> rgstrnCityName, stateCode ==> rgstrnStateCode, zipCode ==> rgstrnZipCode,
        zipPlus4 ==> rgstrnZipPlus4, countryCode ==> rgstrnCountryCode)

      val (ta, nonTa) = df.split(accountType.column === "TA" || (accountType.column === "VBS" && taAcctId.column.isNotNull))
      val nonTaDF = nonTa.join(vsagVbsRgstrn.select(sharedCols ++ vbsOnly: _*), altnSagId, "left")
      val taDF = ta.join(vagrmtRgstrnTx.select(sharedCols: _*), altnSagId, "left")
      nonTaDF.unionByNameWith(taDF, "outer")
        .cache()
        .debugLogging("appendRegistrationAddress")
    }

    /**
     * Appends street address to the dataframe.
     */
    def appendStreetAddress: DataFrame = {
      val streetAddresses = vsagAddrRlshp.where(trim(rlshpTypCode.column) === "RGST" && trim(addrStatusCode.column) === "VALD")
        // There can be multiple address ids per sag_id in non-prod.
        // Pick only the last updated address id per sag_id.
        .deduplicate(Seq(sagId.column), Seq(lastUpdatedTimestamp.column.desc))
        .select(sagId, addressId)
        .join(tables.vcin15.deduplicate(addressId.column, lastUpdatedTimestamp.column.desc), addressId)
        .select(sagId ==> addrSagId, streetAddressLine1, streetAddressLine2, streetAddressLine3, streetAddressLine4, cityName,
          stateCode, zipCode, zipPlus4, countryCode, fognAddressFlag)
      df.join(streetAddresses, altnSagId.column === addrSagId.column, "left")
        .drop(addrSagId)
        .cache()
        .debugLogging("appendStreetAddress")
    }

    /**
     * Appends [[rgstrnTypCode]] to the dataframe.
     *
     * - For VBA accounts, use the ''tsag_vbs_rgstrn'' table for the registration type.
     *
     * - For TA and TA linked VBS accounts, use the ''tagmrt_rgstrn'' table for the registration type.
     */
    def appendRegistrationTypeCode: DataFrame = {
      val vba = df.where(accountType.column === "VBA").select(sagId).join(vsagVbsRgstrn.select(sagId, rgstrnTypCode), sagId)
      val ta = df.where(accountType.column === "TA").select(sagId).join(vagrmtRgstrn.select(sagId, rgstrnTypCode), sagId)
      val sagRgstrnCodes = vba.union(ta).withColumnRenamed(sagId, altnSagId).distinct()
      df.join(sagRgstrnCodes, altnSagId, "left")
        .cache()
        .debugLogging("appendRegistrationTypeCode")
    }

    def appendAccountEstablishedDate: DataFrame = {
      val mfBeginDateDF = vmutlFndPosn.select(acctId, beginDate).deduplicate(acctId.column, beginDate.column.asc)

      val taToVbaEstabDateDF = {
        val rlshp = vsagRlshp.where(sagRlshpTypCode.column === 300).select(memberSagId, ownerSagId)
        rlshp.join(vsagAcctRlshp.where(trim(rlshpTypCode.column) === "ACCT").select(sagId, rlshpId ==> acctId), sagId.column === ownerSagId.column).drop(sagId)
          .join(vsagAcctRlshp.where(trim(rlshpTypCode.column) === "ACCT").select(sagId, rlshpId ==> mbrAcctId), sagId.column === memberSagId.column).drop(sagId)
          .join(mfBeginDateDF.withColumnRenamed(acctId, mbrAcctId), mbrAcctId)
          .select(acctId, beginDate)
      }

      val mfEstabDateDF = df.select(acctId).join(mfBeginDateDF, acctId)
      val brkgEstabDateDF = df.select(acctId)
        .join(vacctPosnBrk.select(acctId, beginDate).deduplicate(acctId.column, beginDate.column.asc), acctId)

      val estabDateDF = taToVbaEstabDateDF.union(mfEstabDateDF).union(brkgEstabDateDF)
        .deduplicate(acctId.column, beginDate.column)
        .select(acctId, beginDate ==> accountEstabDate)

      df.join(estabDateDF, acctId, "left")
        .cache()
        .debugLogging("appendAccountEstablishedDate")
    }

    def appendRpoEscheatedIndicators: DataFrame = {
      val (ta, nonTa) = df.split(accountType.column === "TA")
      val taDF = ta.withIndicators(vmutlFndPosn.deduplicate(acctId.column, endDate.column.desc), TAStatusIndicators)
      val nonTaDF = nonTa.withIndicators(vacctRstrn.deduplicate(acctId.column, endTimestamp.column.desc), VBRstrnCodeIndicators)
      taDF.union(nonTaDF)
        .cache()
        .debugLogging("appendRpoEscheatedIndicators")
    }
  }

  override lazy val output: DataFrame = getAccountBaseData
    .deriveAccountStatus
    .deriveAccountType
    .setupVbsTaLinkage
    .appendBrokerageAccountNumber
    .appendRegistrationAddress
    .appendStreetAddress
    .appendRegistrationTypeCode
    .appendAccountEstablishedDate
    .appendRpoEscheatedIndicators
}

object AccountBaseTable {
  final val schema: TableSchema = TableSchema(Array(
    acctId, brokerageAccountNumber, servId, sagId, accountType, rgstrnTypCode, rtrmtPlnTypCode, accountEstabDate, sagBeginDate, sagEndDate, openClosedCode, altnSagId, taAcctId,
    rpoIndicator, escheatedIndicator, rgstrnNumberOfNameLines, rgstrnLine1, rgstrnLine2, rgstrnLine3, rgstrnLine4, rgstrnLine5, rgstrnLine6,
    rgstrnCityName, rgstrnStateCode, rgstrnZipCode, rgstrnZipPlus4, rgstrnCountryCode, rgstrnFognAddressFlag,
    streetAddressLine1, streetAddressLine2, streetAddressLine3, streetAddressLine4, cityName, stateCode, zipCode, zipPlus4, countryCode, fognAddressFlag
  ))
}