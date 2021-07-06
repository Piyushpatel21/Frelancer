package com.vanguard.rdna.abx
package table

import com.vanguard.rdas.rc.spark.TableSchema
import org.apache.spark.sql.{DataFrame, SparkSession}

class AccountBeneficiaryTable(spark: SparkSession, tables: ABXTables, bucket: String, accountDetail: => DataFrame
) extends ABXOutputTable(spark, bucket, abxDB, accountBeneficiaryTable, AccountBeneficiaryTable.schema) {
  lazy val output: DataFrame = {
    val allBeneficiaries = tables.vbnfcryDtl.where(beneficiaryChangeOfOwnershipFlag.column === "N")
      .select(beneficiarySetId, beneficiaryId, taxpayerIdNo, relationshipToOwnerCode, beneficiaryTypeCode,
        beneficiaryAllocationPercentage, birthDate, trustDate, beneficiaryName, beneficaryDesignationCode)

    accountDetail.select(acctId, sagId)
      .join(tables.vsagBnfcrySet.select(sagId, beneficiarySetId), sagId)
      .distinct
      .join(allBeneficiaries, beneficiarySetId)
  }
}

object AccountBeneficiaryTable {
  final val schema: TableSchema = TableSchema(Array(acctId, sagId, beneficiarySetId, beneficiaryId, taxpayerIdNo,
    relationshipToOwnerCode, beneficiaryTypeCode, beneficiaryAllocationPercentage, birthDate, trustDate,
    beneficiaryName, beneficaryDesignationCode))
}