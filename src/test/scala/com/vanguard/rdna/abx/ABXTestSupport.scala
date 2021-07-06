package com.vanguard.rdna.abx

import java.io.File
import java.sql.{Date, Timestamp}

import com.google.common.io.Files
import com.vanguard.rdas.rc.test.{RowEquality, SparkTestOps}
import com.vanguard.rdna.abx.table.ABXTables
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, Suite}

object ABXTestSupport {
  lazy val tmpDir: File = {
    println("Attempting to create temporary directory...")
    val retval: File = Files.createTempDir()
    println(s"tmpDir in SparkHiveSupport = $retval")
    retval
  }
  lazy val masterSparkSession: SparkSession = {
    println("Attempting to create spark session...")
    val retval: SparkSession = sparkSessionBuilder(tmpDir).getOrCreate()
    println("Successfully created SparkSession!")
    retval
  }
  val resourcesPath: String = System.getProperty("user.dir") + "/src/test/resources"

  def tmpSubDir(subDir: String): String = new File(tmpDir, subDir).getCanonicalPath
  def getTmpDirAsTableLocation(subDir: String = null): String = s"file:///${FilenameUtils.separatorsToUnix(Option(subDir).map(tmpSubDir).getOrElse(tmpDir.getCanonicalPath))}"

  private def sparkSessionBuilder(tmpDir: File): SparkSession.Builder = {
    val localMetastorePath = tmpSubDir("metastore")
    val localWarehousePath = tmpSubDir("warehouse")
    val appId = this.getClass.getName + math.floor(math.random * 10E4).toLong.toString

    SparkSession
      .builder()
      .master("local[*]")
      .appName("rdna-test")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.ui.enabled", "false")
      .config("spark.app.id", appId)
      .config("spark.driver.host", "localhost")
      .config("spark.sql.warehouse.dir", localWarehousePath)
      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
      .config("spark.hadoop.hive.exec.dynamic.partition", "true")
      .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
  }

  implicit class SQLDateInterpolator(val sc: StringContext) extends AnyVal {
    private def parseArgs(args: Seq[Any]): String = sc.s(args:_*).stripMargin
    def dt(args: Any*): Date = Date.valueOf(parseArgs(args))
    def ts(args: Any*): Timestamp = {
      val s = parseArgs(args)
      Timestamp.valueOf(if (s.matches("""\d\d\d\d\-[0-1]\d-[0-3]\d""")) s + " 00:00:00" else s)
    }
  }
}

trait ABXTestSupport extends SparkTestOps with Matchers with MockitoSugar {
  this: Suite =>
  import ABXTestSupport.SQLDateInterpolator

  val spark: SparkSession = ABXTestSupport.masterSparkSession
  implicit val rowEquality: RowEquality = RowEquality()

  val openEndDate = dt"9999-12-31"
  val tables: ABXTables = mock[ABXTables]

  def readCSV(fileName: String, schema: StructType = null): DataFrame = Option(schema)
    .map(spark.read.schema)
    .getOrElse(spark.read.option("inferSchema", true))
    .option("dateFormat", "yyyy-MM-dd")
    .option("header", true)
    .option("nullValue", "NULL")
    .csv(s"src/test/resources/$fileName.csv")
}