package com.vanguard.rdna.abx
package derived

class VBRstrnCodeIndicatorsTest extends DateActiveGroupByIndicatorColumnsTest[Int, String] {

  override val indicators: DateActiveGroupByIndicatorColumns = VBRstrnCodeIndicators
  override val openRecords: Seq[(Int, String)] = Seq((1, "RPO"), (1, "ESCH"), (2, "RPO"), (3, "ESCH"))
  override val closedRecords: Seq[(Int, String)] = Seq((2, "ESCH"), (3, "RPO"))

  "rpo_indicator for non-TA accounts" should {
    "be 'Y'" when {
      "an active record with rstrn code 'RPO' is associated with the account" in {
        getKeysWhereIndicator(rpoIndicator, true) should contain only (1, 2)
      }
    }
    "be 'N' otherwise" in {
      getKeysWhereIndicator(rpoIndicator, false) should contain only 3
    }
  }

  "escheated_indicator for non-TA accounts" should {
    "be 'Y'" when {
      "an active record with rstrn code 'ESCH' is associated with the account" in {
        getKeysWhereIndicator(escheatedIndicator, true) should contain only (1, 3)
      }
    }
    "be 'N' otherwise" in {
      getKeysWhereIndicator(escheatedIndicator, false) should contain only 2
    }
  }
}
