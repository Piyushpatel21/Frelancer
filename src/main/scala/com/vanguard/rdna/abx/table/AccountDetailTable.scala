package com.vanguard.rdna.abx
package table

import com.vanguard.rdas.rc.spark.{ColumnRepository, GenericColumn, TableColumn, TableSchema}
import com.vanguard.rdna.abx.derived.{BrokerageHoldings, DerivedStateCode}
import org.apache.spark.sql.functions.{array, col, lit, sum, when}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AccountDetailTable(override val spark: SparkSession, tables: ABXTables, bucket: String, account: => DataFrame,
  accountOwner: => DataFrame, accountContactSummary: => DataFrame, taHoldings: => DataFrame, isProd: Boolean = true
) extends ABXOutputTable(spark, bucket, abxDB, accountDetailTableName, AccountDetailTable.schema) {
  import spark.implicits._

  lazy val (delinkedVbsAccts, otherAccts) = account.split(accountType.column === "VBS" && taAcctId.column.isNull)

  lazy val acctsWithAddrArrays = otherAccts.where(accountType.column.isin("TA", "VBA", "VBS"))
    .withColumn(numberOfNameLines, when(rgstrnNumberOfNameLines.column.isNull, 0).otherwise(rgstrnNumberOfNameLines.column))
    .withColumn("rgstrn_array", array(rgstrnLine1, rgstrnLine2, rgstrnLine3, rgstrnLine4, rgstrnLine5, rgstrnLine6))
    .withColumn("address_array", array(streetAddressLine1, streetAddressLine2, streetAddressLine3, streetAddressLine4))

  lazy val acctsWithStAddr = (1 to 7).foldLeft(acctsWithAddrArrays) { case (df: DataFrame, i: Int) =>
    df.withColumn(s"addr_ln_$i", when(numberOfNameLines.column >= i, $"rgstrn_array"(i - 1))
      .otherwise($"address_array"((lit(i - 1) - numberOfNameLines.column).cast(IntegerType))))
  }

  lazy val acctsWithRgstrnAddr: DataFrame = delinkedVbsAccts.select(
    acctId, brokerageAccountNumber, servId, sagId, accountType, rgstrnTypCode, rtrmtPlnTypCode,
    accountEstabDate, sagBeginDate, sagEndDate, openClosedCode, altnSagId, taAcctId, rpoIndicator, escheatedIndicator,
    rgstrnNumberOfNameLines ==> numberOfNameLines, rgstrnLine1 ==> addressLine1, rgstrnLine2 ==> addressLine2, rgstrnLine3 ==> addressLine3,
    rgstrnLine4 ==> addressLine4, rgstrnLine5 ==> addressLine5, rgstrnLine6 ==> addressLine6, addressLine7 <== lit(null),
    rgstrnCityName ==> cityName, rgstrnStateCode ==> stateCode, rgstrnZipCode ==> zipCode,
    rgstrnZipPlus4 ==> zipPlus4, rgstrnCountryCode ==> countryCode, rgstrnFognAddressFlag ==> fognAddressFlag
  )

  lazy val accountWithAddress: DataFrame = acctsWithRgstrnAddr.unionByNameWith(acctsWithStAddr, "inner")

  lazy val accountWithOwner: DataFrame = {
    val priorityNumber = TableColumn("_priority")
    val PR1 = List("PRRT")
    val PR2 = List(
      "SHJS", "SHSH", "SHGP", "SHLP", "SHNO", "SHPS", "SHMN", "SHAL", "SHRM", "SHBN",
      "CUCU", "CUGU", "CUPL", "CUUS", "CUPA", "CUCM", "CUCO", "CUAL", "CUSD", "CUSN",
      "TRTR", "BNBN"
    )

    val accountOwnerWithPriority = accountOwner.withColumn(priorityNumber,
      when(poRoleCode.column.isin(PR1: _*), 1).when(poRoleCode.column.isin(PR2: _*), 2))
    val owners = accountOwnerWithPriority.where(priorityNumber.column.isNotNull)
      .deduplicate(Seq(acctId.column, poId.column), Seq(priorityNumber.column.asc))
      .limit(Seq(acctId.column), Seq(priorityNumber.column.asc), AccountDetailTable.ownerCount, false)
      .cache()

    val withOwners = (1 to AccountDetailTable.ownerCount).foldLeft(accountWithAddress) { case (accDF, index) =>
      val ownerDF = owners.where(col("_row_number_") === index)
        .select(acctId :: AccountOwnerTable.BasicOwnerColumns.map { c => c ==> s"${c.name}_$index" }: _*)
      accDF.join(ownerDF, acctId, "left").cache()
    }

    val cols: List[TableColumn] = acctId :: AccountOwnerTable.EDeliveryColumns
    val edelDF = owners.where(col("_row_number_") === 1 && poRoleCode.column === "PRRT").select(cols: _*)

    withOwners.join(edelDF, acctId, "left")
  }

  private val contactTypeMap: Map[String, (GenericColumn, GenericColumn)] = Map(
    basicContactType -> (dateContactBasic, contactBasicDescription),
    webLogonContactType -> (dateContactWeb, contactWebDescription),
    mailContactType -> (dateContactMail, contactMailDescription),
    nonRecurringTransContactType -> (dateContactTransaction, contactTransactionDescription),
    recurringTransContactType -> (dateContactRecurring, contactRecurringDescription),
    taxFormContactType -> (dateContactTaxform, contactTaxformDescription),
    nonRecurringTransRelatedContactType -> (dateContactTransRelated, contactTransRelatedDescription),
    recurringTransRelatedContactType -> (dateContactRecurringRelated, contactRecurringRelatedDescription),
    taxFormRelatedContactType -> (dateContactTaxformRelated, contactTaxformRelatedDescription)
  )

  lazy val baseOutput: DataFrame = {
    val contactTypeDfs = accountContactSummary.splitBy(contactType, contactTypeMap.keys.toSeq:_*)._1 map {
      case (ct, ctdf) =>
        val (dateColumn, descColumn) = contactTypeMap(ct)
        ctdf.select(acctId, lastContactTs ==> dateColumn, contactDescription ==> descColumn)
    }
    contactTypeDfs.foldLeft[DataFrame](accountWithOwner)(joinOn(acctId, "left"))
  }

  lazy val withDeriveStateCode: DataFrame = baseOutput.as[DerivedStateCode].map(_.derivedStateCodeRecord).toDF(acctId, derivedStateCode)

  private val deriveTotalAccountBalance = totalAccountBalance <== sum(marketValue)
  lazy val withDerivedAccountBalance: DataFrame = {
    val (ta, vb) = baseOutput.select(acctId, accountType, brokerageAccountNumber).split(accountType.column === "TA")

    val taData = ta.join(taHoldings, acctId, "left").select(acctId, marketValue, marketValueDate)
    val vbHoldings = BrokerageHoldings(spark, tables, isProd).appendHoldings(vb)
    val vbData = vb.join(vbHoldings, acctId, "left")

    taData.unionByNameWith(vbData, "inner")
      .replaceNulls(marketValue, 0d)
      .groupBy(acctId, marketValueDate)
      .agg(deriveTotalAccountBalance.column)
      .deduplicate(acctId.column, marketValueDate.column.desc_nulls_last)
  }

  private val deriveClientOwnedFlag =
    clientOwnedFlag <== flag(accountType.column === "TA" || brokerageAccountNumber.column.cast(IntegerType).between(10000000, 89999999))

  lazy val output: DataFrame = baseOutput.join(withDeriveStateCode, acctId).join(withDerivedAccountBalance, acctId) + deriveClientOwnedFlag
}

object AccountDetailTable {

  val ownerCount: Int = 5

  private def getColumn(tc: TableColumn): (TableColumn, DataType, String) = implicitly[ColumnRepository].get(tc)

  private val edeliveryColumnsWithSchema: List[(TableColumn, DataType, String)] =
    AccountOwnerTable.EDeliveryColumns.map(getColumn)

  private val ownerColumnsWithSchema: List[(GenericColumn, DataType, String)] =
    (1 to ownerCount).flatMap { i =>
      AccountOwnerTable.BasicOwnerColumns.map { tc =>
        val (_, dt, comment) = getColumn(tc)
        (TableColumn(s"${tc.name}_$i"), dt, s"$comment $i")
      }
    }.toList

  private val suffixColumns: List[(TableColumn, DataType, String)] = List(totalAccountBalance, marketValueDate, clientOwnedFlag).map(getColumn)

  private val baseColumns: List[(TableColumn, DataType, String)] = List(
    acctId, brokerageAccountNumber, servId, sagId, accountType, rgstrnTypCode, rtrmtPlnTypCode,
    accountEstabDate, sagBeginDate, sagEndDate, openClosedCode, altnSagId, taAcctId, rpoIndicator, escheatedIndicator,
    numberOfNameLines, addressLine1, addressLine2, addressLine3, addressLine4, addressLine5, addressLine6, addressLine7, cityName, stateCode, zipCode, zipPlus4, countryCode,
    fognAddressFlag, derivedStateCode, dateContactBasic, contactBasicDescription, dateContactWeb, contactWebDescription, dateContactMail,
    contactMailDescription, dateContactTransaction, contactTransactionDescription, dateContactRecurring, contactRecurringDescription,
    dateContactTaxform, contactTaxformDescription, dateContactTransRelated, contactTransRelatedDescription, dateContactRecurringRelated,
    contactRecurringRelatedDescription, dateContactTaxformRelated, contactTaxformRelatedDescription
  ).map(getColumn)

  val schema: TableSchema = TableSchema(baseColumns ::: edeliveryColumnsWithSchema ::: ownerColumnsWithSchema ::: suffixColumns)
}