package ch.zzeekk.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

class WebsocketSourceV2EchoTest extends FunSuite {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private implicit lazy val ss: SparkSession = {
    Util.sparkSessionBuilder(true).getOrCreate()
  }

  // Websocket URL to connect to
  val url = "ws://echo.websocket.org"
  // send this as initial message
  val initMsg = """echo test"""

  test("websocket echo test") {
    import ss.implicits._

    logger.info("initializing stream")
    val df_websocketMsg = ss
      .readStream
      .format("ch.zzeekk.spark.WebsocketSourceV2")
      .option(WebsocketSourceV2.URL, url)
      .option(WebsocketSourceV2.INIT_MSG, initMsg)
      .option(WebsocketSourceV2.NUM_PARTITIONS, 1)
      .load

    logger.info("starting stream")
    val streamQuery = df_websocketMsg
      .writeStream
      .trigger(Trigger.ProcessingTime(5.seconds))
      .format("console")
      .start()

    streamQuery.awaitTermination()
  }
}
