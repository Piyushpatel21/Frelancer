package com.vanguard.rdna

import com.vanguard.rdas.rc.core.LoggingSupport
import com.vanguard.rdas.rc.spark.{ColumnRepository, DFOps, ImplicitColumnRepository, TableColumn}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{current_timestamp, lit}

package object abx extends DFOps with ImplicitColumnRepository with LoggingSupport {

  implicit class MoreDFOps(df: DataFrame) {
    def debugLogging(label: String): DataFrame = {
      logStdout(s"DataFrame count $label = ${df.count()}")
      df
    }

    def debugLogging(label: String, column: Column, ids: Any*): DataFrame = {
      logStdout(s"DataFrame count $label = ${df.count()}")
      if (ids.nonEmpty) {
        df.where(column.isin(ids.map(lit): _*)).show(false)
      }
      df
    }
  }

  // Databases and tables
  val retailDB = "retail_entmaster1"
  val platformDB = "platform_entmaster1"
  val abxDB = "retail_abxmaster"
  val clientContactEventTableName = "client_contact_event"
  val accountContactEventTableName = "account_contact_event"
  val accountContactSummaryTableName = "account_contact_summary"
  val accountBaseTableName = "account_base"
  val accountDetailTableName = "account_detail"
  val accountOwnerTable = "account_owner"
  val taHoldingTable = "ta_holding"
  val brokerageHoldingTable = "brokerage_holding"
  val accountBeneficiaryTable = "account_beneficiary"

  // Contact types
  val webLogonContactType = "WEB_LOGON"
  val phoneContactType = "PHONE"
  val mailContactType = "MAIL"
  val inPersonContactType = "IN_PERSON"
  val clamContactType = "CLAM"
  val taxForm1099ContactType = "TAX_FORM_1099"
  val taxForm1042sContactType = "TAX_FORM_1042S"
  val recurringTransContactType = "RECURRING_TRANS"
  val nonRecurringTransContactType = "NON_RECURRING_TRANS"
  val basicContactType = "BASIC"
  val taxFormContactType = "TAX_FORM"
  val nonRecurringTransRelatedContactType = "NON_RECURRING_TRANS_RELATED"
  val recurringTransRelatedContactType = "RECURRING_TRANS_RELATED"
  val taxFormRelatedContactType = "TAX_FORM_RELATED"

  // Common columns
  val acctId = TableColumn("acct_id")
  val rlshpId = TableColumn("rlshp_id")
  val sagId = TableColumn("sag_id")
  val poId = TableColumn("po_id")
  val poRoleCode = TableColumn("po_role_cd")
  val typeCode = TableColumn("type_cd")
  val beginDate = TableColumn("efftv_bgn_dt")
  val beginTimestamp = TableColumn("efftv_bgn_ts")
  val endDate = TableColumn("efftv_end_dt")
  val endTimestamp = TableColumn("efftv_end_ts")
  val versnTs = TableColumn("versn_ts")
  val lastUpdatedTs = TableColumn("last_update_ts")
  val statusChangeDate = TableColumn("status_chg_dt")
  val portId = TableColumn("port_id")

  // Account columns
  val accountType = TableColumn("account_type")
  val servId = TableColumn("serv_id")
  val sagBeginDate = TableColumn("sag_efftv_bgn_dt")
  val sagEndDate = TableColumn("sag_efftv_end_dt")
  val brokerageAccountNumber = TableColumn("brokerage_acct_no")
  val accountAltnIdCode = TableColumn("acct_altn_id_cd")
  val accountEstabDate = TableColumn("acct_estab_dt")
  val accountNumber = TableColumn("acct_no")
  val vastAcctNumber = TableColumn("vast_acct_no")
  val rgstrnTypCode = TableColumn("rgstrn_typ_cd")
  val rtrmtPlnTypCode = TableColumn("rtrmt_pln_typ_cd")
  val accountStatusCode = TableColumn("acct_status_cd")
  val servAccountStatusCode = TableColumn("serv_acct_sts_cd")
  val statusCode = TableColumn("status_cd")
  val marginIndicator = TableColumn("margin_indicator")
  val rstrnTypCode = TableColumn("rstrn_typ_cd")
  val rpoIndicator = TableColumn("rpo_indicator")
  val escheatedIndicator = TableColumn("escheated_indicator")
  val openClosedCode = TableColumn("open_clsd_cd")
  val marginPermissionCode = TableColumn("mgn_prmsn_cd")
  val lastUpdatedTimestamp = TableColumn("lst_updtd_ts")
  val taAcctId = TableColumn("ta_acct_id")
  val altnSagId = TableColumn("altn_sag_id")

  // Sag columns
  val disSagId = TableColumn("dis_sag_id")
  val rlshpTypCode = TableColumn("rlshp_typ_cd")
  val addrStatusCode = TableColumn("addr_status_cd")
  val sagRlshpTypCode = TableColumn("sag_rlshp_typ_cd")
  val sagWebRgstrnId = TableColumn("sag_web_rgstrn_id")
  val memberSagId = TableColumn("mbr_sag_id")
  val ownerSagId = TableColumn("ownr_sag_id")

  // Edelivery preference columns
  val eDeliveryStatements = TableColumn("edelivery_statements")
  val eDeliveryConfirmation = TableColumn("edelivery_confirmation")
  val eDeliveryReports = TableColumn("edelivery_reports")
  val eDeliveryNotices = TableColumn("edelivery_notices")
  val eDeliveryTaxForm = TableColumn("edelivery_tax_form")
  val eDeliveryProxy = TableColumn("edelivery_proxy_materials")

  val lastUpdatedEDeliveryStatements = TableColumn("last_updated_edelivery_statements")
  val lastUpdatedEDeliveryConfirmation = TableColumn("last_updated_edelivery_confirmation")
  val lastUpdatedEDeliveryReports = TableColumn("last_updated_edelivery_reports")
  val lastUpdatedEDeliveryNotices = TableColumn("last_updated_edelivery_notices")
  val lastUpdatedEDeliveryTaxForm = TableColumn("last_updated_edelivery_tax_form")
  val lastUpdatedEDeliveryProxy = TableColumn("last_updated_edelivery_proxy_materials")
  val prodtFeatureId  = TableColumn("prodt_featr_id")
  val deliveryModeCode = TableColumn("delvry_mode_cd")
  val brokerageConfirmationCode = TableColumn("brkrage_cnfrm_cd")
  val vboConfirmationCode = TableColumn("vbo_cnfrm_cd")
  val proxyStatementCode = TableColumn("proxy_stmt_cd")
  val webRgstrnOptTimestamp = TableColumn("web_rgstrn_opt_ts")

  // Address columns
  val numberOfNameLines = TableColumn("no_of_nm_lns")
  val rgstrnNumberOfNameLines = TableColumn("rgstrn_no_of_nm_lns")
  val rgstrnLine1 = TableColumn("rgstrn_ln_1_tx")
  val rgstrnLine2 = TableColumn("rgstrn_ln_2_tx")
  val rgstrnLine3 = TableColumn("rgstrn_ln_3_tx")
  val rgstrnLine4 = TableColumn("rgstrn_ln_4_tx")
  val rgstrnLine5 = TableColumn("rgstrn_ln_5_tx")
  val rgstrnLine6 = TableColumn("rgstrn_ln_6_tx")

  val addressId = TableColumn("addr_id")
  val streetAddressLine1 = TableColumn("st_addr_1_tx")
  val streetAddressLine2 = TableColumn("st_addr_2_tx")
  val streetAddressLine3 = TableColumn("st_addr_3_tx")
  val streetAddressLine4 = TableColumn("st_addr_4_tx")

  val cityName = TableColumn("city_nm")
  val stateCode = TableColumn("ste_cd")
  val zipCode = TableColumn("zip_cd")
  val zipPlus4 = TableColumn("zip_plus_4_cd")
  val countryCode = TableColumn("cntry_cd")
  val fognAddressFlag = TableColumn("fogn_addr_fl")
  val rgstrnCityName = TableColumn("rgstrn_city_nm")
  val rgstrnStateCode = TableColumn("rgstrn_ste_cd")
  val rgstrnZipCode = TableColumn("rgstrn_zip_cd")
  val rgstrnZipPlus4 = TableColumn("rgstrn_zip_plus_4_cd")
  val rgstrnCountryCode = TableColumn("rgstrn_cntry_cd")
  val rgstrnFognAddressFlag = TableColumn("rgstrn_fogn_addr_fl")
  val derivedStateCode = TableColumn("derived_ste_cd")

  val addressLine1 = TableColumn("addr_ln_1")
  val addressLine2 = TableColumn("addr_ln_2")
  val addressLine3 = TableColumn("addr_ln_3")
  val addressLine4 = TableColumn("addr_ln_4")
  val addressLine5 = TableColumn("addr_ln_5")
  val addressLine6 = TableColumn("addr_ln_6")
  val addressLine7 = TableColumn("addr_ln_7")

  //columns needed for Account filtering/manupulation
  val acctAltnIdCode = TableColumn("ACCT_ALTN_ID_CD")
  val asgnmtVal = TableColumn("asgnmt_val")

  // Tcntct columns
  val clientPoid = TableColumn("clnt_po_id")
  val channelCode = TableColumn("commnctn_chnl_cd")
  val contactCode = TableColumn("cntct_cd")
  val contactTypeCode = TableColumn("cntct_typ_cd")
  val lastContactTs = TableColumn("last_contact_ts")
  val contactDescription = TableColumn("contact_desc")
  val contactType = TableColumn("contact_type")
  val lastLogonTs = TableColumn("lst_lgn_ts")

  // Client Info columns
  val vgiClientId = TableColumn("vgi_clnt_id")
  val firstName = TableColumn("frst_nm")
  val middleName = TableColumn("midl_nm")
  val lastName = TableColumn("lst_nm")
  val orgName = TableColumn("org_nm")
  val emailAddress = TableColumn("email_address")
  val tinId = TableColumn("tin_id")
  val tinType = TableColumn("tin_typ")
  val birthDate = TableColumn("brth_dt")
  val isDeceased = TableColumn("decsd_fl")
  val deceasedDate = TableColumn("decsd_dt")
  val vndrVrfDeathDate = TableColumn("vndr_vrf_death_dt")
  val deathCertFileDate = TableColumn("death_cert_file_dt")

  // Email columns
  val usageCd = TableColumn("usage_cd")
  val purpCd = TableColumn("purp_cd")
  val frmttdAddr = TableColumn("frmttd_addr")
  val invldEmailCnt = TableColumn("invld_email_cnt")
  val webFl = TableColumn("web_fl")
  val semsStatusCd = TableColumn("sems_status_cd")
  val emailBounceIndicator = TableColumn("email_bounce_indicator")

  // Transaction columns
  val vbTransactionCode = TableColumn("txn_src_cd")
  val vbTransactionDate = TableColumn("txn_procs_dt")
  val taTransactionCode = TableColumn("txn_alowd_cd")
  val taTransactionDate = TableColumn("procs_dt")
  val marketValueAmount = TableColumn("mkt_val_am")
  val prtnId = TableColumn("prtn_id")
  val posnDt = TableColumn("posn_dt")
  val bookShares = TableColumn("book_shr_qy")
  val issShares = TableColumn("iss_shr_qy")
  val totalBrkgBalance = TableColumn("total_brkg_bal")
  val totalTaBalance = TableColumn("total_ta_bal")

  // Holdings columns
  val fundId = TableColumn("fund_id")
  val fundLongName = TableColumn("fund_long_name")
  val tickerSymbol = TableColumn("ticker_symbol")
  val cusip = TableColumn("cusip")
  val cusipNo = TableColumn("cusip_no")
  val nav = TableColumn("nav")
  val issueShareQuantity = TableColumn("iss_shr_qy")
  val bookShareQuantity = TableColumn("book_shr_qy")
  val shareQuantity = TableColumn("share_qty")
  val marketValue = TableColumn("market_value")
  val insId = TableColumn("ins_id")
  val currencyCode = TableColumn("currcy_cd")
  val priceTypeCode = TableColumn("prc_typ_cd")
  val priceEffectiveDate = TableColumn("prc_efftv_dt")
  val marketValueDate = TableColumn("market_value_dt")
  val mutualFundStatusCode = TableColumn("mutl_fnd_status_cd")

  // Beta holdings columns
  val stockRecordDate = TableColumn("stock_record_date")
  val ingestDate = TableColumn("ingest_date")
  val securityNumber = TableColumn("sec_no")
  val controlNumber = TableColumn("ctrl_no")

  // Detail column tables
  val dateContactBasic = TableColumn("dt_contact_basic")
  val contactBasicDescription = TableColumn("contact_basic_desc")
  val dateContactWeb = TableColumn("dt_contact_web")
  val contactWebDescription = TableColumn("contact_web_desc")
  val dateContactMail = TableColumn("dt_contact_mail")
  val contactMailDescription = TableColumn("contact_mail_desc")
  val dateContactTransaction = TableColumn("dt_contact_trans")
  val contactTransactionDescription = TableColumn("contact_trans_desc")
  val dateContactRecurring = TableColumn("dt_contact_recurring")
  val contactRecurringDescription = TableColumn("contact_recurring_desc")
  val dateContactTaxform = TableColumn("dt_contact_taxform")
  val contactTaxformDescription = TableColumn("contact_taxform_desc")
  val dateContactTransRelated = TableColumn("dt_contact_trans_related")
  val contactTransRelatedDescription = TableColumn("contact_trans_related_desc")
  val dateContactRecurringRelated = TableColumn("dt_contact_recurring_related")
  val contactRecurringRelatedDescription = TableColumn("contact_recurring_related_desc")
  val dateContactTaxformRelated = TableColumn("dt_contact_taxform_related")
  val contactTaxformRelatedDescription = TableColumn("contact_taxform_related_desc")
  val totalAccountBalance = TableColumn("total_acct_balance")

  //Beneficiary Columns
  val beneficiarySetId = TableColumn("bnfcry_set_id")
  val beneficiaryId = TableColumn("bnfcry_id")
  val taxpayerIdNo = TableColumn("taxpayr_id_no")
  val relationshipToOwnerCode = TableColumn("rlshp_to_ownr_cd")
  val beneficiaryTypeCode = TableColumn("bnfcry_typ_cd")
  val beneficiaryAllocationPercentage = TableColumn("bnfcry_alocn_pc")
  val trustDate = TableColumn("trst_dt")
  val beneficiaryName = TableColumn("bnfcry_nm")
  val beneficaryDesignationCode = TableColumn("bnfcry_dsgn_cd")
  val beneficiaryChangeOfOwnershipFlag = TableColumn("bnfcry_coo_fl")

  // Derived columns
  val newLastUpdatedTs = TableColumn("last_update_ts") <== current_timestamp()
  val clientOwnedFlag = TableColumn("client_owned_flag")

  final val ValidStateCodes: Set[String] = Set(
    "AA", "AE", "AK", "AL", "AP", "AR", "AS", "AZ", "CA", "CO",
    "CT", "DC", "DE", "FL", "FM", "GA", "GU", "HI", "IA", "ID",
    "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MH", "MI",
    "MN", "MO", "MP", "MS", "MT", "NC", "ND", "NE", "NH", "NJ",
    "NM", "NV", "NY", "OH", "OK", "OR", "PA", "PR", "PW", "RI",
    "SC", "SD", "TN", "TX", "UT", "VA", "VI", "VT", "WA", "WI",
    "WV", "WY")

  def isActiveRecord(beginDateColumn: TableColumn = beginDate, endDateColumn: TableColumn = endDate): Column =
    beginDateColumn.column <= current_timestamp() && endDateColumn.column > current_timestamp()

  override implicit val repo: ColumnRepository = ABXColumnRepository
}
