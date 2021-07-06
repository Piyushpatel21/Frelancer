package com.vanguard.rdna.abx
package derived

import com.vanguard.rdas.rc.spark.CaseColumn
import org.scalatest.WordSpec

class AccountTypesTest extends WordSpec with ABXTestSupport with CaseColumnTest {

  override val cc: CaseColumn = AccountTypes
  override val columns: Seq[String] = Seq(servId, disSagId)

  "accountType" when {
    "serv_id is 90" should {
      "be VBA" in {
        runAssertion("VBA", (90, null))
      }
    }
    "serv_id is 8 or 38" should {
      "be TA" in {
        runAssertion("TA", (8, null), (38, null))
      }
    }
    "serv_id is 9" should {
      "be VBO" when {
        "sag_rlshp_typ_cd is 10" in {
          runAssertion("VBO", (9, 10))
        }
      }
      "be VBS otherwise" in {
        runAssertion("VBS", (9, null))
      }
    }
  }
}
