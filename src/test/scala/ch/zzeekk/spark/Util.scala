package ch.zzeekk.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.Try

object Util {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def sparkSessionBuilder( withHive : Boolean = false ) : SparkSession.Builder = {
    // check env
    val hadoopHome = System.getenv("HADOOP_HOME")
    // require(hadoopHome!=null, "HADOOP_HOME is not set!") // jenkins has no HADOOP_HOME set...
    if (hadoopHome == null) logger.error("HADOOP_HOME is not set!")
    // create builder
    val builder = SparkSession.builder()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.shuffle.partitions", "10")
    // Enable hive support if available
    if (withHive) Try( builder.enableHiveSupport())
    builder.master("local")
  }
}
