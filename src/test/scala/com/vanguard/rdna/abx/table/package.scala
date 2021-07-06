package com.vanguard.rdna.abx

import java.sql.Timestamp

package object table {
  case class ExpectedContactRecord(acct_id: Int, last_contact_ts: Timestamp, contact_type: String)
}
