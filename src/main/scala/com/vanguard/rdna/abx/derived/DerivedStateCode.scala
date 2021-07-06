package com.vanguard.rdna.abx
package derived

import scala.util.matching.Regex

case class DerivedStateCode(
    acct_id: BigDecimal, account_type: String, addr_ln_1: Option[String], addr_ln_2: Option[String], addr_ln_3: Option[String],
    addr_ln_4: Option[String], addr_ln_5: Option[String], addr_ln_6: Option[String], addr_ln_7: Option[String], city_nm: Option[String],
    ste_cd: Option[String], zip_cd: Option[String], cntry_cd: Option[String], fogn_addr_fl: Option[String]
) {

  def derivedStateCode: String = {

    val zipCodeRegex: Regex = """\d{5}""".r
    val addressRegex: Regex = """([a-zA-Z'.\s]+)[,\s]\s*([A-Z]{2})\s*(\d{5})-?(\d*)[\s\d-]*""".r

    /*
      Regex explanations:
        zipCodeRegex: exactly 5 digits
        addressRegex:
            ([a-zA-Z'.\s]+) any letter (either case), space, apostrophe, or period 1 or more times (capture group 1)
            [,\s]\s* comma or space followed by 0 or more spaces
            ([A-Z]{2})\s* exactly 2 upper case letters (capture group 2) followed by 0 or more spaces
            (\d{5})-? exactly 5 digits (capture group 3) optionally followed by a hyphen
            (\d*)[\s\d-]* 0 or more digits (capture group 4) followed by a 0 or more spaces, digits, or hyphens;
                         this is intended to be the zip 4 digit extension but you'll want to verify that you've only captured 4 digits because the data can be weird here;
                         because the (\d*) is greedy it will match until it hits a space or a hyphen
     */

    def stateCodeIsValid: Boolean = ste_cd.exists(ValidStateCodes.contains)
    def zipCodeIsValid: Boolean  = zip_cd.exists(zipCodeRegex.pattern.matcher(_).matches)

    if (stateCodeIsValid && zipCodeIsValid)
      ste_cd.get
    else if (fogn_addr_fl.contains("Y") || !cntry_cd.contains("US"))
      if (account_type == "TA") "DE" else "PA"
    else
      // Get the first non-empty string iterating backwards and check if matches the addressRegex - if it does not match OR there are no non-empty strings return "UNKNOWN"
      Seq(addr_ln_7, addr_ln_6, addr_ln_5, addr_ln_4, addr_ln_3, addr_ln_2, addr_ln_1) collectFirst {
        case Some(str) if !str.trim.isEmpty => str
      } collect {
        // extractor pattern: only extract if it matches the address regex
        case addressRegex(_, stecd, _, _) if ValidStateCodes.contains(stecd) => stecd
      } getOrElse "UNKNOWN"
  }

  def derivedStateCodeRecord: (BigDecimal, String) = (acct_id, derivedStateCode)
}