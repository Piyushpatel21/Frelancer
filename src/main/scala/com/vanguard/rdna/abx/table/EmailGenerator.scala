package com.vanguard.rdna.abx.table

import com.vanguard.rdas.rc.core.SmtpUtils
import com.vanguard.rdna.abx.JobConfig
import com.vanguard.rdna.abx._

case class EmailGenerator(jobConfig:JobConfig, inputTables: ABXTables){
  def execute(): Unit = {
    val bdFallOutCodesList = inputTables.vbrkgTran.select(vbTransactionCode).distinct().collect().toVector
      .map(_.getString(0)).filterNot(TransactionCodeLists.bdCodeLists.allCodes.contains)
    val bdFallOutCodesText =
      if (bdFallOutCodesList.isEmpty) "No Brokerage Fall Out Codes \n"
      else s"Brokerage Fallout Codes are: ${bdFallOutCodesList.mkString(", ")}\n"

    val taFallOutCodesList = inputTables.vtxnsRtlFinHist.select(taTransactionCode).distinct().collect().toVector
      .map(_.getString(0)).filterNot(TransactionCodeLists.taCodeLists.allCodes.contains)
    val taFallOutCodesText =
      if (taFallOutCodesList.isEmpty) "No TA Fall Out Codes \n"
      else s"TA Fallout Codes are: ${taFallOutCodesList.mkString(", ")}\n"

    val fromAddress = "RDnA_Reg_and_Compliance_Team2@vanguard.com"

    val bodyText =
      s"This is an automated email that Abandoned Property job ran successfully in ${jobConfig.env}.\n" + 
        s"$taFallOutCodesText \n" +
        s"$bdFallOutCodesText \n" +
      s"Contact $fromAddress with any questions or if you have received this in error"

    val (isProd, toAddress) =
      if (jobConfig.env == "prod") (true, "rdas_abandoned_property_notification@vanguard.com")
      else (false, "rdas_abandoned_property_non_prod@vanguard.com")

    SmtpUtils.sendPlainTextEmail(s"Abandoned Property Job Successful in ${jobConfig.env}", bodyText, fromAddress, toAddress, isProd)
  }
}
