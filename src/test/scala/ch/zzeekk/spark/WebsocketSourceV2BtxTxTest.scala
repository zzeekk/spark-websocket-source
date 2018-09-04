package ch.zzeekk.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

class WebsocketSourceV2BtxTxTest extends FunSuite {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private implicit lazy val ss: SparkSession = {
    Util.sparkSessionBuilder(true).getOrCreate()
  }

  // Websocket URL to connect to
  val url = "wss://ws.blockchain.info/inv" // docs see https://www.blockchain.com/de/api/api_websocket
  // send this as initial message
  val initMsg = """{"op":"unconfirmed_sub"}"""

  def getDfBtcTx = {
    import ss.implicits._

    logger.info("initializing stream")
    val df_websocketMsg = ss
      .readStream
      .format("ch.zzeekk.spark.WebsocketSourceV2")
      .option(WebsocketSourceV2.URL, url)
      .option(WebsocketSourceV2.INIT_MSG, initMsg)
      .option(WebsocketSourceV2.NUM_PARTITIONS, 1)
      .load
    //df_websocketMsg.printSchema

    val df_btcTx = df_websocketMsg
      .withColumn("parsed_msg", from_json($"msg", schema))
      .select($"parsed_msg.op", $"parsed_msg.x.*")
    df_btcTx.printSchema

    // return
    df_btcTx
  }

  test("read and parse blockchain.info ") {
    import ss.implicits._

    logger.info("starting stream")
    val streamQuery = getDfBtcTx
      .writeStream
      .trigger(Trigger.ProcessingTime(5.seconds))
      .format("console")
      .start()

    streamQuery.awaitTermination()
  }

  test("running sum over btc tx value") {
    import ss.implicits._
    ss.conf.set("spark.sql.codegen.wholeStage", false) // avoid InternalCompilerException: Compiling "GeneratedClass": Code of method ... grows beyond 64 KB

    val df_btcVolume = getDfBtcTx
      .withColumn("ts", to_timestamp($"time"))
      .withColumn( "out", explode($"out"))
      .withWatermark("ts", "10 minutes")
      .groupBy(window($"ts", "30 seconds", "10 seconds" ).as("window"))
      .agg(sum($"out.value").as("out_volume30s"))

    logger.info("starting stream")
    val streamQuery = df_btcVolume
      .orderBy($"window")
      .writeStream
      //.trigger(Trigger.ProcessingTime(5.seconds))
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()

    streamQuery.awaitTermination()
  }

  /* sample message
  {
    "op": "utx",
    "x": {
      "lock_time": 0,
      "ver": 1,
      "size": 192,
      "inputs": [
        {
          "sequence": 4294967295,
          "prev_out": {
            "spent": true,
            "tx_index": 99005468,
            "type": 0,
            "addr": "1BwGf3z7n2fHk6NoVJNkV32qwyAYsMhkWf",
            "value": 65574000,
            "n": 0,
            "script": "76a91477f4c9ee75e449a74c21a4decfb50519cbc245b388ac"
          },
          "script": "483045022100e4ff962c292705f051c2c2fc519fa775a4d8955bce1a3e29884b2785277999ed02200b537ebd22a9f25fbbbcc9113c69c1389400703ef2017d80959ef0f1d685756c012102618e08e0c8fd4c5fe539184a30fe35a2f5fccf7ad62054cad29360d871f8187d"
        }
      ],
      "time": 1440086763,
      "tx_index": 99006637,
      "vin_sz": 1,
      "hash": "0857b9de1884eec314ecf67c040a2657b8e083e1f95e31d0b5ba3d328841fc7f",
      "vout_sz": 1,
      "relayed_by": "127.0.0.1",
      "out": [
        {
          "spent": false,
          "tx_index": 99006637,
          "type": 0,
          "addr": "1A828tTnkVFJfSvLCqF42ohZ51ksS3jJgX",
          "value": 65564000,
          "n": 0,
          "script": "76a914640cfdf7b79d94d1c980133e3587bd6053f091f388ac"
        }
      ]
    }
  }
  */
  // fixed message schema for parsing json (WebsocketSourceV2 doesnt yet support schema inference)
  val schema = new StructType()
    .add("op", StringType)
    .add( "x", new StructType()
      .add("lock_time", IntegerType)
      .add("ver", IntegerType)
      .add("size", IntegerType)
      .add("inputs", new ArrayType( containsNull=false, elementType=new StructType()
        .add("sequence", LongType)
        .add("script", StringType)
        .add("prev_out", new StructType()
          .add("spent", BooleanType)
          .add("tx_index", IntegerType)
          .add("type", IntegerType)
          .add("addr", StringType)
          .add("value", IntegerType)
          .add("n", StringType)
          .add("script", StringType)
        )
      ))
      .add("time", LongType)
      .add("tx_index", IntegerType)
      .add("vin_sz", IntegerType)
      .add("hash", StringType)
      .add("vout_sz", IntegerType)
      .add("relayed_by", StringType)
      .add("out", new ArrayType( containsNull=false, elementType=new StructType()
        .add("spent", BooleanType)
        .add("tx_index", IntegerType)
        .add("type", IntegerType)
        .add("addr", StringType)
        .add("value", IntegerType)
        .add("n", StringType)
        .add("script", StringType)
      ))
    )

}
