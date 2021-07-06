package com.vanguard.rdna.abx
package derived

class TAStatusIndicatorsTest extends DateActiveGroupByIndicatorColumnsTest[Int, String] {

  override val indicators: DateActiveGroupByIndicatorColumns = TAStatusIndicators
  override val openRecords: Seq[(Int, String)] = Seq((1, "04"), (1, " 05 "), (2, "04"), (3, "05"))
  override val closedRecords: Seq[(Int, String)] = Seq((2, "05 "), (3, " 04"))

  "rpo_indicator for TA accounts" should {
    "be 'Y'" when {
      "an active record with status code 4 is associated with the account" in {
        getKeysWhereIndicator(rpoIndicator, true) should contain only (1, 2)
      }
    }
    "be 'N' otherwise" in {
      getKeysWhereIndicator(rpoIndicator, false) should contain only 3
    }
  }

  "escheated_indicator for TA accounts" should {
    "be 'Y'" when {
      "an active record with status code 5 is associated with the account" in {
        getKeysWhereIndicator(escheatedIndicator, true) should contain only (1, 3)
      }
    }
    "be 'N' otherwise" in {
      getKeysWhereIndicator(escheatedIndicator, false) should contain only 2
    }
  }
}
