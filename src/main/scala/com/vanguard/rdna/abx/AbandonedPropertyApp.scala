package com.vanguard.rdna.abx

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object AbandonedPropertyApp {
  def main(sysArgs: Array[String]): Unit = {
    val args = GlueArgParser.getResolvedOptions(sysArgs, Array("JOB_NAME", "catalogid", "env", "nodes"))
    val jobConfig = JobConfig(args("env"), args("catalogid"), args("nodes"))
    val conf = new SparkConf()
      .set("hive.metastore.glue.catalogid", jobConfig.catalogId)
      .set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
      .set("spark.hadoop.hive.exec.dynamic.partition", "true")
      .set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.sql.parquet.writeLegacyFormat", "true")
      .set("spark.sql.shuffle.partitions", jobConfig.nodes)

    val sparkContext = new SparkContext(conf)
    val glueContext: GlueContext = new GlueContext(sparkContext)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    // This will launch the actual application
    try {
      new MainExecutor(glueContext.sparkSession, jobConfig).execute()
    } catch {
      case e: Exception => // This is just done to get some better error reporting
        println(e.getMessage)
        e.printStackTrace()
        throw e
    }

    Job.commit()
  }
}