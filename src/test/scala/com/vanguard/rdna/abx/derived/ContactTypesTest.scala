package com.vanguard.rdna.abx.derived

import com.vanguard.rdna.abx.ABXTestSupport
import com.vanguard.rdna.abx.derived.ContactTypes.ContactType
import org.scalatest.WordSpec

case class ContactTypeTestRecord(expected: String, commnctn_chnl_cd: String, cntct_cd: String, cntct_typ_cd: String)
case class ContactTypeTestResultRecord(expected: String, actual: String)

class ContactTypesTest extends WordSpec with ABXTestSupport {
  import spark.implicits._

  def assertCorrect(result: ContactTypeTestResultRecord): Unit = result.actual shouldBe result.expected

  "column" should {
    "derive the contact type based on the input records" in {
      val contactTypes = ContactTypes.values.asInstanceOf[Set[ContactType]]
      val records = for (contactType <- contactTypes; ch <- contactType.validCommunicationChannels; cc <- contactType.validCodes.keys;
         ct <- contactType.validCodes(cc)) yield ContactTypeTestRecord(contactType.contactType, ch, cc, ct)

      records.toSeq.toDF().select($"expected", ContactTypes.column.as("actual")).as[ContactTypeTestResultRecord].collect().foreach(assertCorrect)
    }
  }
}
