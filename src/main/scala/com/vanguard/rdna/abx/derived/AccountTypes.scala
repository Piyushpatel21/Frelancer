package com.vanguard.rdna.abx.derived

import com.vanguard.rdas.rc.spark.CaseColumn
import com.vanguard.rdna.abx.{accountType, disSagId, servId}

object AccountTypes extends CaseColumn(accountType) {
  val VBA: CaseWhen = new CaseWhen(servId.column === 90, "VBA")
  val TA: CaseWhen = new CaseWhen(servId.column.isin(8, 38), "TA")
  val VBS: CaseWhen = new CaseWhen(servId.column === 9 && disSagId.column.isNull, "VBS")
  val VBO: CaseWhen = new CaseWhen(servId.column === 9 && disSagId.column.isNotNull, "VBO")
}