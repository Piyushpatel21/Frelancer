package com.vanguard.rdna.abx
package table

import org.apache.spark.sql.functions.{col, trim}
import org.apache.spark.sql.types.{HiveStringType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait ABXTables {
  // Account tables
  def vacct: DataFrame
  def vacctAltnIdAsgn: DataFrame
  def vacctBrkg: DataFrame
  def vacctRstrn: DataFrame
  def vacctPosnBrk: DataFrame
  def vmutlFndPosn: DataFrame
  def vvbsTaxForm: DataFrame
  def vmfTaxForm: DataFrame

  // Sag tables
  def vsag: DataFrame
  def vsagRlshp: DataFrame
  def vsagBusRlshp: DataFrame
  def vsagVbsRgstrn: DataFrame
  def vsagAcctRlshp: DataFrame
  def vsagAddrRlshp: DataFrame
  def vdisinstSagRlshp: DataFrame

  // Client Info tables
  def vcin15: DataFrame
  def vcin22: DataFrame
  def vcin24: DataFrame
  def vcin170: DataFrame

  // Misc tables
  def tcntct: DataFrame // using full history for this table
  def vsectyCred: DataFrame
  def vservAgrmtWebOp: DataFrame
  def vagrmtRgstrn: DataFrame
  def vagrmtRgstrnTx: DataFrame
  def vwebRgstrnOpt: DataFrame
  def vacctPosnBrkCur: DataFrame
  def vacctPosBrkCurPrt: DataFrame

  // Email tables
  def vcntctPref: DataFrame
  def velctronicAddr: DataFrame

  // Brokerage tables
  def vbrkgTran: DataFrame
  def vtxnsRtlFinHist: DataFrame

  // Holdings tables
  def vcVgiIntIns: DataFrame
  def vissCharsVbs: DataFrame
  def vissPrcVgi: DataFrame

  // Beneficiary tables
  def vbnfcryDtl: DataFrame
  def vsagBnfcrySet: DataFrame
}

class ABXTablesImpl(val spark: SparkSession) extends ABXTables {

  private def getDF(s: String, trimAll: Boolean): DataFrame = {
    val retval = spark.table(s)
    if (trimAll) {
      retval.schema.foldLeft(retval) { (df, field) =>
        if (field.dataType == StringType || field.dataType.isInstanceOf[HiveStringType]) df.withColumn(field.name, trim(col(field.name)))
        else df
      }
    } else retval
  }
  private implicit def getDF(s: String): DataFrame = getDF(s, true)

  // Account tables
  val vacct: DataFrame = s"$retailDB.vacct"
  val vacctAltnIdAsgn: DataFrame = s"$retailDB.vacct_altn_id_asgn"
  val vacctBrkg: DataFrame = s"$retailDB.vacct_brkg"
  val vacctRstrn: DataFrame = s"$retailDB.vacct_rstrn"
  val vacctPosnBrk: DataFrame = s"$retailDB.vacct_posn_brk"
  val vmutlFndPosn: DataFrame = s"$retailDB.vmutl_fnd_posn"
  val vvbsTaxForm: DataFrame = s"$retailDB.vvbs_tax_form"
  val vmfTaxForm: DataFrame = s"$retailDB.vmf_tax_form"

  // Sag tables
  val vsag: DataFrame = s"$platformDB.vsag"
  val vsagRlshp: DataFrame = s"$retailDB.vsag_rlshp"
  val vsagBusRlshp: DataFrame = s"$platformDB.vsag_bus_rlshp"
  val vsagVbsRgstrn: DataFrame = s"$retailDB.vsag_vbs_rgstrn"
  val vsagAcctRlshp: DataFrame = s"$retailDB.vsag_acct_rlshp"
  val vsagAddrRlshp: DataFrame = s"$retailDB.vsag_addr_rlshp"
  val vdisinstSagRlshp: DataFrame = s"$retailDB.VDISINST_SAG_RLSHP"

  // Client Info tables
  val vcin15: DataFrame = s"$platformDB.vcin0150"
  val vcin22: DataFrame = s"$platformDB.vcin0220"
  val vcin24: DataFrame = s"$platformDB.vcin0240"
  val vcin170: DataFrame = s"$platformDB.vcin1700"

  // Misc tables
  val tcntct: DataFrame = s"$retailDB.tcntct"
  val vsectyCred: DataFrame = s"$retailDB.vsecty_cred"
  val vservAgrmtWebOp: DataFrame = s"$retailDB.vserv_agrmt_web_op"
  val vagrmtRgstrn: DataFrame = s"$retailDB.vagrmt_rgstrn"
  val vagrmtRgstrnTx: DataFrame = s"$retailDB.vagrmt_rgstrn_tx"
  val vwebRgstrnOpt: DataFrame = s"$retailDB.vweb_rgstrn_opt"
  val vacctPosnBrkCur: DataFrame = s"$retailDB.vacct_posn_brk_cur"
  val vacctPosBrkCurPrt: DataFrame = s"$retailDB.vacctposbrkcur_prt"

  //email tables
  val vcntctPref: DataFrame = s"$platformDB.vcntct_pref"
  val velctronicAddr: DataFrame = s"$platformDB.velctronic_addr"

  // Brokerage tables
  val vbrkgTran: DataFrame = s"$retailDB.vbrkg_tran"
  val vtxnsRtlFinHist: DataFrame = s"$retailDB.vtxns_rtl_fin_hist"

  // Holdings tables
  val vcVgiIntIns: DataFrame = s"$retailDB.vc_vgi_int_ins"
  val vissCharsVbs: DataFrame = s"$retailDB.viss_chars_vbs"
  val vissPrcVgi: DataFrame = s"$retailDB.viss_prc_vgi"

  // Beneficiary tables
  val vbnfcryDtl: DataFrame = s"$retailDB.vbnfcry_dtl"
  val vsagBnfcrySet: DataFrame = s"$retailDB.vsag_bnfcry_set"
}