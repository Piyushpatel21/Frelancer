package com.vanguard.rdna.abx
package derived

import org.scalatest.FunSuite

class DerivedStateCodeTest extends FunSuite with ABXTestSupport {

  private implicit def stringOption(s: String): Option[String] = Option(s)

  test("When ste_cd field is populated, populate derived_ste_cd with ste_cd") {
    DerivedStateCode(1,"TA","","","","","","","","PHILADELPHIA","PA","19111","US","N").derivedStateCodeRecord shouldBe (1,"PA")
    DerivedStateCode(2,"VBA","","","","","","","","ATLANTA","GA","10267","US","N").derivedStateCodeRecord shouldBe (2,"GA")
    DerivedStateCode(3,"VBS","","","","","","","","CAMDEN","NJ","19367","US","N").derivedStateCodeRecord shouldBe (3,"NJ")
  }

  test("When ste_cd field is not populated and fogn_addr_fl = 'Y', populate derived_ste_cd with 'DE' for TA accounts and 'PA' for all others") {
    DerivedStateCode(1,"TA","","","","","","","","PARIS","","19111","US","Y").derivedStateCodeRecord shouldBe (1,"DE")
    DerivedStateCode(2,"VBA","","","","","","","","QUEBEC","","10267","US","Y").derivedStateCodeRecord shouldBe (2,"PA")
    DerivedStateCode(3,"VBS","","","","","","","","KYOTO","","19367","US","Y").derivedStateCodeRecord shouldBe (3,"PA")
  }

  test("When ste_cd field is not populated, fogn_addr_fl = 'N', and cntry_cd is not 'US', populate derived_ste_cd with 'DE' for TA accounts and 'PA' for all others") {
    DerivedStateCode(1,"TA","","","","","","","","PARIS","","19111","FR","N").derivedStateCodeRecord shouldBe (1,"DE")
    DerivedStateCode(2,"VBA","","","","","","","","QUEBEC","","10267","CA","N").derivedStateCodeRecord shouldBe (2,"PA")
    DerivedStateCode(3,"VBS","","","","","","","","KYOTO","","19367","JA","N").derivedStateCodeRecord shouldBe (3,"PA")
  }

  test("When ste_cd field is not populated, fogn_addr_fl = 'N', and cntry_cd = 'US, populate derived_ste_cd with state code taken from address lines") {
    DerivedStateCode(1,"TA","MARYANNE COLLINS","777 W GERMANTOWN PIKE APT 233","PLYMOUTH MEETING PA 19462-1026","","","","","","","","US","N").derivedStateCodeRecord shouldBe (1,"PA")
    DerivedStateCode(2,"VBA","XAVIER HULL","TRUST","3579 W PINE STREET","PALMER AK 99645","","","","","","","US","N").derivedStateCodeRecord shouldBe (2,"AK")
    DerivedStateCode(3,"VBS","APRIL LEE &","CATHERINE SAKAI","PO BOX 13","WAYNE PA 19087","","","","CAMDEN","","19367","US","N").derivedStateCodeRecord shouldBe (3,"PA")
  }

  test("When no state code can be obtained by previously outlined methods, populate derived_ste_cd with 'UNKNOWN'") {
    DerivedStateCode(1,"TA","","","","","","","","PHILADELPHIA","","19111","US","N").derivedStateCodeRecord shouldBe (1,"UNKNOWN")
    DerivedStateCode(2,"VBA","","","","","","","","ATLANTA","","10267","US","N").derivedStateCodeRecord shouldBe (2,"UNKNOWN")
    DerivedStateCode(3,"VBS","","","","","","","","CAMDEN","","19367","US","N").derivedStateCodeRecord shouldBe (3,"UNKNOWN")
  }
}
