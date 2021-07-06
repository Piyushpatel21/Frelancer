package com.vanguard.rdna.abx
package derived

import com.vanguard.rdas.rc.spark.CaseColumn
import org.apache.spark.sql.DataFrame
import scala.reflect.runtime.universe.TypeTag

trait CaseColumnTest { // maybe move to common code at some point
  this: ABXTestSupport =>

  val cc: CaseColumn
  val columns: Seq[String]

  def convertDF[T <: Product: TypeTag](data: Seq[T]): DataFrame = spark.createDataFrame(data).toDF(columns:_*)
  def runAssertion[T <: Product: TypeTag](expected: Any, data: T*): Unit = {
    val result = convertDF(data).withColumn(cc.derived).select(cc.derived.name).collect()
    assert(result.nonEmpty && result.forall(_.get(0) == expected))
  }
}
