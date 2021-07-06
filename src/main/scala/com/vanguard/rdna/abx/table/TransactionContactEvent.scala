package com.vanguard.rdna.abx
package table

import com.vanguard.rdas.rc.spark.GenericColumn
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{trim, format_string}

class TransactionContactEvent(spark: SparkSession, tables: ABXTables) {
  import tables._

  private def getTransactionData(base: DataFrame, contactCodes: TransactionCodeLists, tcc: GenericColumn, tdc: GenericColumn): DataFrame = base.withColumn(tcc := trim)
    .where(tcc.column.isInCollection(contactCodes.validCodes))
    .select(acctId, tdc ==> lastContactTs, contactType <== flag(tcc.column.isInCollection(contactCodes.recurringCodes), recurringTransContactType, nonRecurringTransContactType),
      contactDescription <== format_string("transaction_code=%s", tcc.column))

  def getTransactions: DataFrame = {
    val mfp = vmutlFndPosn.select(acctId, vastAcctNumber, beginDate, endDate, portId)
    val taBase = vtxnsRtlFinHist.select(accountNumber, taTransactionCode, taTransactionDate, portId)
      .join(mfp, accountNumber.column === vastAcctNumber.column && vtxnsRtlFinHist(portId) === mfp(portId) && taTransactionDate.column.between(beginDate.column, endDate.column))

    getTransactionData(vbrkgTran, TransactionCodeLists.bdCodeLists, vbTransactionCode, vbTransactionDate)
      .union(getTransactionData(taBase, TransactionCodeLists.taCodeLists, taTransactionCode, taTransactionDate))
  }
}
