package ch.zzeekk.spark

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{DataSourceRegister}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types._
import org.json4s.DefaultFormats

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean

case class LongOffset( offset:Long ) extends Offset {
  override def json(): String = offset.toString
  def +(offset1:Long) = LongOffset(offset+offset1)
}

/**
  * A websocket streaming source using Apache Spark DataSource V2 API.
  * This requires Spark 2.3 to compile.
  * Commit handling isn't properly implemented, as we cant request specific offsets from a websocket service
  */
class WebsocketSourceV2 extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister with Logging {
  override def createMicroBatchReader(schema: Optional[StructType], checkpointLocation: String, options: DataSourceOptions): MicroBatchReader = {
    new WebsocketMicroBatchReader(options)
  }

  override def shortName(): String = "websocketV2"
}

object WebsocketSourceV2 {
  val URL = "url"
  val INIT_MSG = "initMsg"
  val NUM_PARTITIONS = "numPartitions"

  val SCHEMA = new StructType()
    .add(StructField("msg", StringType))
}

class WebsocketBatchTask(events: Seq[String]) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new WebsocketBatchReader(events)
}

class WebsocketBatchReader(events: Seq[String]) extends DataReader[Row] {
  private var currentIdx = -1

  override def next(): Boolean = {
    // Return true as long as the new index is in the seq.
    currentIdx += 1
    currentIdx < events.size
  }

  override def get(): Row = {
    Row(events(currentIdx))
  }

  override def close(): Unit = {}
}

class WebsocketMicroBatchReader(options: DataSourceOptions) extends MicroBatchReader with Logging {

  private val url = options.get(WebsocketSourceV2.URL).get
  private val initMsg = options.get(WebsocketSourceV2.INIT_MSG)
  private val numPartitions = options.get(WebsocketSourceV2.NUM_PARTITIONS).orElse("1").toInt

  private var startOffset: LongOffset = LongOffset(0)
  private var endOffset: LongOffset = LongOffset(0)
  private var currentOffset: LongOffset = LongOffset(0)
  private var lastReturnedOffset: LongOffset = LongOffset(-1)
  private var lastCommitedOffset: LongOffset = LongOffset(-1)

  // List of incoming messages
  private val incomingEvents = mutable.Queue[String]()

  // Websocket Sink to handle incoming messages
  private val incoming = Sink.foreach[Message] {
    case message:
      TextMessage.Strict => {
      incomingEvents.enqueue(message.text)
      log.debug(s"got message with size ${message.text.length}")
      currentOffset = currentOffset + 1
    }
    case m => throw new RuntimeException("Invalid message type received " + m)
  }

  // start receiving events
  private val initialized = new AtomicBoolean(false)
  private var promiseClosed: Promise[Option[Message]] = null

  private def initialize() = {

    log.info(s"connecting to $url with msg $initMsg")
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    // connecting
    val outgoing = if (initMsg.isPresent) {
      Source.single(TextMessage(initMsg.get))
        .concatMat(Source.maybe[Message])(Keep.right) // Source.maybe is a Promise that keeps the connection open
    } else Source.maybe[Message]
    val flow = Flow.fromSinkAndSourceMat(incoming, outgoing)(Keep.right)
    val (upgradeResponse, promiseClosed) = Http().singleWebSocketRequest(WebSocketRequest(url), flow)

    // log status
    upgradeResponse.flatMap {
      upgrade =>
        if (upgrade.response.status == StatusCodes.SwitchingProtocols) Future.successful(Done)
        else throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
    }
      .onComplete(_ => log.info("connection successful"))
    promiseClosed.future.onComplete(_ => log.info("connection closed"))

    // return
    promiseClosed
  }

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = synchronized {
    // initialize if needed
    if (initialized.compareAndSet(false, true)) {
      this.promiseClosed = initialize()
    }

    // get events to process from queue
    log.debug(s"createDataReaderFactories, start: $startOffset, end: $endOffset, lastReturnedOffset: $lastReturnedOffset, currentOffset: $currentOffset")
    val newEvents = for( i <- startOffset.offset to endOffset.offset-1) yield incomingEvents.dequeue
    lastReturnedOffset = lastReturnedOffset + newEvents.size
    // prepare tasks
    val tasks = if (!newEvents.isEmpty) {
      val eventsPerTask = math.ceil(newEvents.size / numPartitions.toDouble).toInt
      log.debug(s"#events: {newEvents.size}, eventsPerTask: $eventsPerTask")
      newEvents.grouped(eventsPerTask).map {
        block => new WebsocketBatchTask(block).asInstanceOf[DataReaderFactory[Row]]
      }
    } else {
      log.debug("no new events to create tasks")
      Seq()
    }
    tasks.toList.asJava
  }

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    log.debug(s"setOffsetRange: start: $start, end: $end")
    if (start.isPresent) startOffset = start.get.asInstanceOf[LongOffset]
    if (end.isPresent) endOffset = end.get.asInstanceOf[LongOffset]
  }

  override def getStartOffset(): Offset = {
    log.debug(s"getStartOffset")
    startOffset
  }

  override def getEndOffset(): Offset = {
    log.debug(s"getEndOffset")
    currentOffset
  }

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }

  override def readSchema(): StructType = {
    WebsocketSourceV2.SCHEMA
  }

  override def commit(end: Offset): Unit = {
    log.debug(s"commit $end")
    lastCommitedOffset = end.asInstanceOf[LongOffset]
  }

  override def stop(): Unit = {
    // complete promise to disconnect
    promiseClosed.success(None)
  }
}