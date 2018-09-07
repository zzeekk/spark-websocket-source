package ch.zzeekk.spark

import java.io.{File, FilenameFilter}
import java.nio.file.{Files, Path, StandardCopyOption}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

class CsvSourceTest extends FunSuite {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private implicit lazy val ss: SparkSession = {
    Util.sparkSessionBuilder(true).getOrCreate()
  }

  test("csv streaming test") {

    // write test.csv from resource to temp-file, so it can be read by spark
    val filePrefix = "streaming-test-"
    val filePostfix = ".csv"
    val csvInputStream = getClass.getResourceAsStream("/test.csv")
    val tmpFile = File.createTempFile(filePrefix,filePostfix)
    val streamInputPath = tmpFile.getParent.toString + s"\\${filePrefix}*${filePostfix}"
    // delete existing tmp-files
    val existingTmpFiles = tmpFile.getParentFile.listFiles( new FilenameFilter {
      def accept(dir: File, name: String) = name.startsWith( filePrefix ) && name.endsWith( filePostfix )
    })
    existingTmpFiles.foreach( f => f.delete())
    Files.copy(csvInputStream, tmpFile.toPath, StandardCopyOption.REPLACE_EXISTING)

    logger.info(s"initializing stream, looking for files $streamInputPath")
    val csvSchema = new StructType()
      .add("type",StringType)
      .add("msg",StringType)
    val df_csvStream = ss
      .readStream
      .format("csv")
      .option("sep", ";")
      .option("header", true)
      .schema(csvSchema)
      .load(streamInputPath)

    logger.info("starting stream")
    val streamQuery = df_csvStream
      .writeStream
      .trigger(Trigger.ProcessingTime(5.seconds))
      .format("console")
      .start()

    streamQuery.awaitTermination()
  }
}
