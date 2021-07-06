package com.vanguard.rdna.abx
package table

import org.mockito.Mockito.when
import org.scalatest.WordSpec

class AccountBeneficiaryTableTest extends WordSpec with ABXTestSupport {
  import ABXTestSupport.SQLDateInterpolator
  import spark.implicits._

  val birthDateTimestamp = ts"1990-11-31 16:18:45.133"
  val trustTimestamp = ts"1995-11-31 16:18:45.133"

  val dummyAccount = Seq((1,1), (2,2)).toDF(acctId, sagId)
  val dummyBeneficiarySet = Seq((100,1), (200,2)).toDF(beneficiarySetId, sagId)
  val dummyBeneficiaryDtl = Seq(
    (100,101,123,"relationshipToOwnerCode1", "beneficiaryTypeCode1",100, birthDateTimestamp, trustTimestamp, "beneficiaryName1", "designationCode1", "N"),
    (200, 201, 456, "relationshipToOwnerCode2", "beneficiaryTypeCode2", 100, birthDateTimestamp, trustTimestamp, "beneficiaryName2","designationCode2", "Y"),
    (200, 201, 456, "relationshipToOwnerCode3", "beneficiaryTypeCode3", 100, birthDateTimestamp, trustTimestamp, "beneficiaryName3","designationCode3", "Y"),
    (200, 201, 456, "relationshipToOwnerCode4", "beneficiaryTypeCode4", 100, birthDateTimestamp, trustTimestamp, "beneficiaryName4","designationCode4", "N")
  ).toDF(
    beneficiarySetId, beneficiaryId, taxpayerIdNo, relationshipToOwnerCode,
    beneficiaryTypeCode, beneficiaryAllocationPercentage, birthDate, trustDate,
    beneficiaryName, beneficaryDesignationCode, beneficiaryChangeOfOwnershipFlag
  )

  val expected = Seq(
    (1, 1, 101, 100, 123, "relationshipToOwnerCode1", "beneficiaryTypeCode1", 100, birthDateTimestamp, trustTimestamp, "beneficiaryName1", "designationCode1"),
    (2, 2, 201, 200, 456, "relationshipToOwnerCode4", "beneficiaryTypeCode4", 100, birthDateTimestamp, trustTimestamp, "beneficiaryName4", "designationCode4")
  )
  val underTest = new AccountBeneficiaryTable(spark, tables, "bucket", dummyAccount)

  "output" should {
    "equal all of the beneficiary records pulled from the necessary tables" in {
      when(tables.vsagBnfcrySet).thenReturn(dummyBeneficiarySet)
      when(tables.vbnfcryDtl).thenReturn(dummyBeneficiaryDtl)

      underTest.output shouldBeEqualTo (expected onColumns(acctId, sagId, beneficiaryId, beneficiarySetId, taxpayerIdNo, relationshipToOwnerCode, beneficiaryTypeCode,
        beneficiaryAllocationPercentage, birthDate, trustDate, beneficiaryName, beneficaryDesignationCode))
    }
  }

}
