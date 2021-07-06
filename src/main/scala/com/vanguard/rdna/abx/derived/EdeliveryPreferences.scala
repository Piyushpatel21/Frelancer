package com.vanguard.rdna.abx
package derived

import java.math.{BigDecimal => SparkBigDecimal}
import java.sql.Timestamp

import com.vanguard.rdas.rc.spark.{GenericColumn, TableColumn}
import com.vanguard.rdna.abx.table.ABXTables
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object EdeliveryPreferences {

  type ResultRecord = (SparkBigDecimal, Int, Timestamp, Timestamp, Boolean)

  val StatementCode: Int = 208
  val NoticeCode: Int = 239
  val TaxFormCode: Int = 237
  val TaConfirmationCode: Int = 214
  val VbConfirmationCode: Int = -214
  val VbReportProxyCode: Int = 248
  val TaReportCode: Int = 205
  val TaProxyCode: Int = -205
  val EdelWebCodes: Set[String] = Set("EMAL", "BOTH")
  val EndTime: Column = lit(Timestamp.valueOf("9999-12-31 00:00:00"))

  // account categories
  val VB = "VB"
  val TA = "TA"

  /**
   * acctType: when the value is Some(VB), Some(TA) or None, it means the feature id
   * is applicable to VB, TA or both, respectively.
   */
  case class FeatureMapping(featureId: Int, valueColumn: String, timestampColumn: String, accountCategory: Option[String] = None) {
    def tempValueColumn: String = accountCategory.map { ac => s"${valueColumn}_$ac" }.getOrElse(valueColumn)
    def tempTimestampColumn: String = accountCategory.map { ac => s"${timestampColumn}_$ac" }.getOrElse(timestampColumn)
  }

  val FeatureMappings: List[FeatureMapping] = List(
    FeatureMapping(StatementCode, eDeliveryStatements, lastUpdatedEDeliveryStatements),
    FeatureMapping(NoticeCode, eDeliveryNotices, lastUpdatedEDeliveryNotices),
    FeatureMapping(TaxFormCode, eDeliveryTaxForm, lastUpdatedEDeliveryTaxForm),
    FeatureMapping(VbConfirmationCode, eDeliveryConfirmation, lastUpdatedEDeliveryConfirmation, Some(VB)),
    FeatureMapping(TaConfirmationCode, eDeliveryConfirmation, lastUpdatedEDeliveryConfirmation, Some(TA)),
    FeatureMapping(VbReportProxyCode, eDeliveryReports, lastUpdatedEDeliveryReports, Some(VB)),
    FeatureMapping(VbReportProxyCode, eDeliveryProxy, lastUpdatedEDeliveryProxy, Some(VB)),
    FeatureMapping(TaReportCode, eDeliveryReports, lastUpdatedEDeliveryReports, Some(TA)),
    FeatureMapping(TaProxyCode, eDeliveryProxy, lastUpdatedEDeliveryProxy, Some(TA))
  )

  val AllFeatureIds: List[Int] = FeatureMappings.map(_.featureId).toSet.toList
  val TaColumnMappings: Map[String, String] = FeatureMappings.filter(_.accountCategory == Option(TA)).flatMap { fm =>
    Map(fm.tempValueColumn -> fm.valueColumn, fm.tempTimestampColumn -> fm.timestampColumn)
  }.toMap
  val VbColumnMappings: Map[String, String] = FeatureMappings.filter(_.accountCategory == Option(VB)).flatMap { fm =>
    Map(fm.tempValueColumn -> fm.valueColumn, fm.tempTimestampColumn -> fm.timestampColumn)
  }.toMap

  def mapToFeatureIds(record: WebRgstrnRecord): Seq[ResultRecord] = {
    def createRecord(webCode: Option[String], featureId: Int): Option[ResultRecord] = webCode collect {
      case trimmed if trimmed.nonEmpty =>
        (record.sag_web_rgstrn_id, featureId, record.web_rgstrn_opt_ts, record.lst_updtd_ts, EdelWebCodes.contains(trimmed.toUpperCase))
    }
    Seq(
      createRecord(record.proxy_stmt_cd, TaProxyCode),
      createRecord(record.confirmationCode, VbConfirmationCode)
    ) flatMap (_.toSeq)
  }
}

class EdeliveryPreferences(spark: SparkSession, tables: ABXTables) {
  import EdeliveryPreferences._
  import spark.implicits._

  private val isEdel: GenericColumn = TableColumn("is_edel")
  private val deriveEdel = isEdel <== (deliveryModeCode.column === "YE" || (prodtFeatureId.column === StatementCode && deliveryModeCode.column === "AN"))

  def appendPreferences(df: DataFrame): DataFrame = {
    val webTableLatest = tables.vwebRgstrnOpt
      .where(webRgstrnOptTimestamp.column.isNotNull)
      .deduplicate(Seq(sagWebRgstrnId.column), Seq(webRgstrnOptTimestamp.column.desc, lastUpdatedTimestamp.column.desc))
      .join(df.select(sagWebRgstrnId, accountType), sagWebRgstrnId)
      .as[WebRgstrnRecord]
      .flatMap(mapToFeatureIds)
      .toDF(sagWebRgstrnId, prodtFeatureId, webRgstrnOptTimestamp, lastUpdatedTimestamp, isEdel)
      .select(sagWebRgstrnId, prodtFeatureId, webRgstrnOptTimestamp ==> beginTimestamp, endTimestamp <== EndTime, lastUpdatedTimestamp, isEdel)

    val reconciled = tables.vservAgrmtWebOp
      .select(sagWebRgstrnId, prodtFeatureId, beginTimestamp, endTimestamp, lastUpdatedTimestamp, deriveEdel)
      .union(webTableLatest)

    val consolidatedDF = reconciled
      .deduplicate(Seq(sagWebRgstrnId.column, prodtFeatureId.column),
        Seq(endTimestamp.column.desc, lastUpdatedTimestamp.column.desc, beginTimestamp.column.desc))
      .select(sagWebRgstrnId, prodtFeatureId, lastUpdatedTimestamp, isEdel)
      .cache()
      .splitBy(prodtFeatureId, AllFeatureIds: _*)._1.map {
        case (featureId: Int, df: DataFrame) =>
          val fms = FeatureMappings.filter(_.featureId == featureId)
          val acc1 = fms.foldLeft(df) { (accDF, fm) => accDF.withColumn(fm.tempValueColumn, isEdel.column) }
          fms.foldLeft(acc1) { (accDF, fm) => accDF.withColumn(fm.tempTimestampColumn, lastUpdatedTimestamp.column)}
      }.reduce[DataFrame] {
        case (a: DataFrame, b: DataFrame) =>
          a.join(b, sagWebRgstrnId, "outer").drop(lastUpdatedTimestamp, prodtFeatureId, isEdel)
      }

    // convert boolean e-delivery preference values to 'Y' or 'N'.
    val joinedDF = df.join(consolidatedDF, sagWebRgstrnId, "left")
    val convertedDF = FeatureMappings.map(_.tempValueColumn).foldLeft(joinedDF) { (accDF, c) =>
      accDF.withColumn(TableColumn(c) <== flag(col(c).isNotNull && col(c), "Y", "N"))
    }.cache()

    val (ta, nonTa) = convertedDF.split(accountType.column === "TA")
    val taDf = ta.withColumnsRenamed(TaColumnMappings)
    val vbDf = nonTa.withColumnsRenamed(VbColumnMappings)
    taDf.unionByNameWith(vbDf, "inner")
  }
}