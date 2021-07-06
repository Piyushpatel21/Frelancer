package com.vanguard.rdna.abx.derived

import java.sql.Timestamp

case class WebRgstrnRecord(
    sag_web_rgstrn_id: java.math.BigDecimal,
    account_type: String,
    brkrage_cnfrm_cd: Option[String],
    vbo_cnfrm_cd: Option[String],
    proxy_stmt_cd: Option[String],
    web_rgstrn_opt_ts: Timestamp,
    lst_updtd_ts: Timestamp
) {
  def confirmationCode: Option[String] = if (account_type == "VBO") vbo_cnfrm_cd else brkrage_cnfrm_cd
}
