package com.vanguard.rdna.abx.derived

import com.vanguard.rdas.rc.spark.CaseColumn
import com.vanguard.rdna.abx.{channelCode, clamContactType, contactCode, contactType, contactTypeCode, inPersonContactType, orCondition, phoneContactType}
import org.apache.spark.sql.functions.trim

object ContactTypes extends CaseColumn(contactType) {
  // probably will need to refactor this later for account contact types
  private[abx] case class ContactType(contactType: String, validCommunicationChannels: Set[String], validCodes: Map[String, Set[String]])
    extends CaseWhen(trim(channelCode.column).isInCollection(validCommunicationChannels) && validCodes.map({
      case (ct, subTypes) => trim(contactCode.column) === ct && trim(contactTypeCode.column).isInCollection(subTypes)
    }).reduce(orCondition), contactType)

  private val phoneOnly = Set("PH")

  val CLAM = ContactType(clamContactType, phoneOnly, Map(
    "CLAM" -> Set("TOAI", "CA05", "VCCS", "VPNU", "RAMQ", "ORLL", "ARMG", "AROP", "SNTC", "SPTC", "VCW1", "AESS", "AIPS", "AWPS", "AWP1",
      "BFAM", "BKSP", "BKSU", "BMAG", "BMOA", "BOAG", "CACS", "CAES", "CALM", "CALT", "CWRA", "ENSC", "ICWR", "RLAG", "TAAP",
      "TAAU", "TACC", "TACP", "TAPR", "TAWP", "TWPR", "BKST", "2WRE", "CAEP", "CARR", "CARS", "DRTT", "EPPW", "REGR", "RESR")))

  val PHONE = ContactType(phoneContactType, phoneOnly, Map(
    "PHAU" -> Set("KBAF","KBAP", "MDFY","PASD", "REST", "STUP", "PASQ"),
    "CLER" -> Set("CAWR","VMTP", "UNRE", "WSTD", "WSTE","WSTG", "WSTR"),
    "NCSA" -> Set("ACHN", "NULL","AGNC", "APSA", "BENC","CWNC","DDNC","DVNC", "EDNC","NANC", "NCAE","NCAW", "ONCE","RMNC", "VBNC","WRNC", "NCED", "NCVV"),
    "DOCS" -> Set("DSFR"))
  )

  val INPERSON = ContactType(inPersonContactType, phoneOnly, Map(
    "FLAP" -> Set("FLCP", "FLRC", "FLSD"),
    "PAAP" -> Set("PACM", "PARS", "PASC"),
    "BDG5" -> Set("BDS4", "BDS3", "CAMP"),
    "AGAP" -> Set("ASCP", "ASRC", "ASSD"),
    "AMAP" -> Set("AMCP", "AMRC", "AMSD"),
    "BDAP" -> Set("BDCP", "BDRC", "BDSD"),
    "FPAP" -> Set("FPCP", "FPRC", "FPSD"),
    "SAAP" -> Set("SACP", "SARC", "SASD"),
    "RSAP" -> Set("RSCP", "RSRC", "RSSD"),
    "KMML" -> Set("AACF", "VVFP"),
    "EGGP" -> Set("EGDC", "ENGC"),
    "VBIO" -> Set("DRCO"),
    "PAIC" -> Set("PASA"),
    "VSAP" -> Set("VSCM", "VSRS", "VSSC"))
  )
}

