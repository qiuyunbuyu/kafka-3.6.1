/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent._
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.Meter
import kafka.network
import kafka.server.{KafkaConfig, RequestLocal}
import kafka.utils.{Logging, NotNothing, Pool}
import kafka.utils.Implicits._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.EnvelopeResponseData
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import java.util
import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object RequestChannel extends Logging {
  private val requestLogger = Logger("kafka.request.logger")

  val RequestQueueSizeMetric = "RequestQueueSize"
  val ResponseQueueSizeMetric = "ResponseQueueSize"
  val ProcessorMetricTag = "processor"

  def isRequestLoggingEnabled: Boolean = requestLogger.underlying.isDebugEnabled

  sealed trait BaseRequest
  case object ShutdownRequest extends BaseRequest
  case object WakeupRequest extends BaseRequest

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress) {
    val sanitizedUser: String = Sanitizer.sanitize(principal.getName)
  }

  class Metrics(enabledApis: Iterable[ApiKeys]) {
    def this(scope: ListenerType) = {
      this(ApiKeys.apisForListener(scope).asScala)
    }

    private val metricsMap = mutable.Map[String, RequestMetrics]()

    (enabledApis.map(_.name) ++
      Seq(RequestMetrics.consumerFetchMetricName, RequestMetrics.followFetchMetricName, RequestMetrics.verifyPartitionsInTxnMetricName)).foreach { name =>
      metricsMap.put(name, new RequestMetrics(name))
    }

    def apply(metricName: String): RequestMetrics = metricsMap(metricName)

    def close(): Unit = {
       metricsMap.values.foreach(_.removeMetrics())
    }
  }

  case class CallbackRequest(fun: RequestLocal => Unit,
                             originalRequest: Request) extends BaseRequest

  class Request(val processor: Int,
                val context: RequestContext,
                val startTimeNanos: Long,
                val memoryPool: MemoryPool,
                @volatile var buffer: ByteBuffer,
                metrics: RequestChannel.Metrics,
                val envelope: Option[RequestChannel.Request] = None) extends BaseRequest {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeNanos = -1L
    @volatile var apiLocalCompleteTimeNanos = -1L
    @volatile var responseCompleteTimeNanos = -1L
    @volatile var responseDequeueTimeNanos = -1L
    @volatile var messageConversionsTimeNanos = 0L
    @volatile var apiThrottleTimeMs = 0L
    @volatile var temporaryMemoryBytes = 0L
    @volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None
    @volatile var callbackRequestDequeueTimeNanos: Option[Long] = None
    @volatile var callbackRequestCompleteTimeNanos: Option[Long] = None

    val session = Session(context.principal, context.clientAddress)

    // 解析出RequestAndSize = RequestBody + size
    private val bodyAndSize: RequestAndSize = context.parseRequest(buffer)

    // This is constructed on creation of a Request so that the JSON representation is computed before the request is
    // processed by the api layer. Otherwise, a ProduceRequest can occur without its data (ie. it goes into purgatory).
    val requestLog: Option[JsonNode] =
      if (RequestChannel.isRequestLoggingEnabled) Some(RequestConvertToJson.request(loggableRequest))
      else None

    def header: RequestHeader = context.header

    def sizeOfBodyInBytes: Int = bodyAndSize.size

    def sizeInBytes: Int = header.size + sizeOfBodyInBytes

    //most request types are parsed entirely into objects at this point. for those we can release the underlying buffer.
    //some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain a reference
    //to the buffer. for those requests we cannot release the buffer early, but only when request processing is done.
    if (!header.apiKey.requiresDelayedAllocation) {
      releaseBuffer()
    }

    def isForwarded: Boolean = envelope.isDefined

    private def shouldReturnNotController(response: AbstractResponse): Boolean = {
      response match {
        case _: DescribeQuorumResponse => response.errorCounts.containsKey(Errors.NOT_LEADER_OR_FOLLOWER)
        case _ => response.errorCounts.containsKey(Errors.NOT_CONTROLLER)
      }
    }

    def buildResponseSend(abstractResponse: AbstractResponse): Send = {
      envelope match {
        case Some(request) =>
          val envelopeResponse = if (shouldReturnNotController(abstractResponse)) {
            // Since it's a NOT_CONTROLLER error response, we need to make envelope response with NOT_CONTROLLER error
            // to notify the requester (i.e. BrokerToControllerRequestThread) to update active controller
            new EnvelopeResponse(new EnvelopeResponseData()
              .setErrorCode(Errors.NOT_CONTROLLER.code()))
          } else {
            val responseBytes = context.buildResponseEnvelopePayload(abstractResponse)
            new EnvelopeResponse(responseBytes, Errors.NONE)
          }
          request.context.buildResponseSend(envelopeResponse)
        case None =>
          context.buildResponseSend(abstractResponse)
      }
    }

    def responseNode(response: AbstractResponse): Option[JsonNode] = {
      if (RequestChannel.isRequestLoggingEnabled)
        Some(RequestConvertToJson.response(response, context.apiVersion))
      else
        None
    }

    def headerForLoggingOrThrottling(): RequestHeader = {
      envelope match {
        case Some(request) =>
          request.context.header
        case None =>
          context.header
      }
    }

    def requestDesc(details: Boolean): String = {
      val forwardDescription = envelope.map { request =>
        s"Forwarded request: ${request.context} "
      }.getOrElse("")
      s"$forwardDescription$header -- ${loggableRequest.toString(details)}"
    }

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T], @nowarn("cat=unused") nn: NotNothing[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    def loggableRequest: AbstractRequest = {

      bodyAndSize.request match {
        case alterConfigs: AlterConfigsRequest =>
          val newData = alterConfigs.data().duplicate()
          newData.resources().forEach(resource => {
            val resourceType = ConfigResource.Type.forId(resource.resourceType())
            resource.configs().forEach(config => {
              config.setValue(KafkaConfig.loggableValue(resourceType, config.name(), config.value()))
            })
          })
          new AlterConfigsRequest(newData, alterConfigs.version())

        case alterConfigs: IncrementalAlterConfigsRequest =>
          val newData = alterConfigs.data().duplicate()
          newData.resources().forEach(resource => {
            val resourceType = ConfigResource.Type.forId(resource.resourceType())
            resource.configs().forEach(config => {
              config.setValue(KafkaConfig.loggableValue(resourceType, config.name(), config.value()))
            })
          })
          new IncrementalAlterConfigsRequest.Builder(newData).build(alterConfigs.version())

        case _ =>
          bodyAndSize.request
      }
    }

    trace(s"Processor $processor received request: ${requestDesc(true)}")

    def requestThreadTimeNanos: Long = {
      if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds
      math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L)
    }

    def updateRequestMetrics(networkThreadTimeNanos: Long, response: Response): Unit = {
      val endTimeNanos = Time.SYSTEM.nanoseconds

      /**
       * Converts nanos to millis with micros precision as additional decimal places in the request log have low
       * signal to noise ratio. When it comes to metrics, there is little difference either way as we round the value
       * to the nearest long.
       */
      def nanosToMs(nanos: Long): Double = {
        val positiveNanos = math.max(nanos, 0)
        TimeUnit.NANOSECONDS.toMicros(positiveNanos).toDouble / TimeUnit.MILLISECONDS.toMicros(1)
      }

      val requestQueueTimeMs = nanosToMs(requestDequeueTimeNanos - startTimeNanos)
      val callbackRequestTimeNanos = callbackRequestCompleteTimeNanos.getOrElse(0L) - callbackRequestDequeueTimeNanos.getOrElse(0L)
      val apiLocalTimeMs = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos + callbackRequestTimeNanos)
      val apiRemoteTimeMs = nanosToMs(responseCompleteTimeNanos - apiLocalCompleteTimeNanos - callbackRequestTimeNanos)
      val responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
      val responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
      val messageConversionsTimeMs = nanosToMs(messageConversionsTimeNanos)
      val totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos)
      val overrideMetricNames =
        if (header.apiKey == ApiKeys.FETCH) {
          val specifiedMetricName =
            if (body[FetchRequest].isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          Seq(specifiedMetricName, header.apiKey.name)
        } else if (header.apiKey == ApiKeys.ADD_PARTITIONS_TO_TXN && body[AddPartitionsToTxnRequest].allVerifyOnlyRequest) {
            Seq(RequestMetrics.verifyPartitionsInTxnMetricName)
        } else {
          Seq(header.apiKey.name)
        }
      overrideMetricNames.foreach { metricName =>
        val m = metrics(metricName)
        m.requestRate(header.apiVersion).mark()
        m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs))
        m.localTimeHist.update(Math.round(apiLocalTimeMs))
        m.remoteTimeHist.update(Math.round(apiRemoteTimeMs))
        m.throttleTimeHist.update(apiThrottleTimeMs)
        m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs))
        m.responseSendTimeHist.update(Math.round(responseSendTimeMs))
        m.totalTimeHist.update(Math.round(totalTimeMs))
        m.requestBytesHist.update(sizeOfBodyInBytes)
        m.messageConversionsTimeHist.foreach(_.update(Math.round(messageConversionsTimeMs)))
        m.tempMemoryBytesHist.foreach(_.update(temporaryMemoryBytes))
      }

      // Records network handler thread usage. This is included towards the request quota for the
      // user/client. Throttling is only performed when request handler thread usage
      // is recorded, just before responses are queued for delivery.
      // The time recorded here is the time spent on the network thread for receiving this request
      // and sending the response. Note that for the first request on a connection, the time includes
      // the total time spent on authentication, which may be significant for SASL/SSL.
      recordNetworkThreadTimeCallback.foreach(record => record(networkThreadTimeNanos))

      if (isRequestLoggingEnabled) {
        val desc = RequestConvertToJson.requestDescMetrics(header, requestLog, response.responseLog,
          context, session, isForwarded,
          totalTimeMs, requestQueueTimeMs, apiLocalTimeMs,
          apiRemoteTimeMs, apiThrottleTimeMs, responseQueueTimeMs,
          responseSendTimeMs, temporaryMemoryBytes,
          messageConversionsTimeMs)
        requestLogger.debug("Completed request:" + desc.toString)
      }
    }

    def releaseBuffer(): Unit = {
      envelope match {
        case Some(request) =>
          request.releaseBuffer()
        case None =>
          if (buffer != null) {
            memoryPool.release(buffer)
            buffer = null
          }
      }
    }

    override def toString = s"Request(processor=$processor, " +
      s"connectionId=${context.connectionId}, " +
      s"session=$session, " +
      s"listenerName=${context.listenerName}, " +
      s"securityProtocol=${context.securityProtocol}, " +
      s"buffer=$buffer, " +
      s"envelope=$envelope)"

  }

  sealed abstract class Response(val request: Request) {

    def processor: Int = request.processor

    def responseLog: Option[JsonNode] = None

    def onComplete: Option[Send => Unit] = None
  }

  /** responseLogValue should only be defined if request logging is enabled */
  class SendResponse(request: Request,
                     val responseSend: Send,
                     val responseLogValue: Option[JsonNode],
                     val onCompleteCallback: Option[Send => Unit]) extends Response(request) {
    override def responseLog: Option[JsonNode] = responseLogValue

    override def onComplete: Option[Send => Unit] = onCompleteCallback

    override def toString: String =
      s"Response(type=Send, request=$request, send=$responseSend, asString=$responseLogValue)"
  }

  class NoOpResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=NoOp, request=$request)"
  }

  class CloseConnectionResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=CloseConnection, request=$request)"
  }

  class StartThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=StartThrottling, request=$request)"
  }

  class EndThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=EndThrottling, request=$request)"
  }
}

class RequestChannel(val queueSize: Int,
                     val metricNamePrefix: String,
                     time: Time,
                     val metrics: RequestChannel.Metrics) {
  import RequestChannel._
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  // the size of request Queue, default 500
  // request 队列大小: 收纳的是BaseRequest类型
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)

  // processors in RequestChannel, default 3
  // 处理线程
  private val processors = new ConcurrentHashMap[Int, Processor]()
  val requestQueueSizeMetricName = metricNamePrefix.concat(RequestQueueSizeMetric)
  val responseQueueSizeMetricName = metricNamePrefix.concat(ResponseQueueSizeMetric)
  //
  private val callbackQueue = new ArrayBlockingQueue[BaseRequest](queueSize)

  metricsGroup.newGauge(requestQueueSizeMetricName, () => requestQueue.size)

  metricsGroup.newGauge(responseQueueSizeMetricName, () => {
    processors.values.asScala.foldLeft(0) {(total, processor) =>
      total + processor.responseQueueSize
    }
  })

  def addProcessor(processor: Processor): Unit = {
    // 1. add processor
    if (processors.putIfAbsent(processor.id, processor) != null)
      warn(s"Unexpected processor with processorId ${processor.id}")
    // 2. focus on responseQueueSize metric in processor
    metricsGroup.newGauge(responseQueueSizeMetricName, () => processor.responseQueueSize,
      Map(ProcessorMetricTag -> processor.id.toString).asJava)
  }

  // Contrary to addProcessor
  def removeProcessor(processorId: Int): Unit = {
    processors.remove(processorId)
    metricsGroup.removeMetric(responseQueueSizeMetricName, Map(ProcessorMetricTag -> processorId.toString).asJava)
  }

  /**
   * 新增 Request
   * Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request): Unit = {
    requestQueue.put(request)
  }

  def closeConnection(
    request: RequestChannel.Request,
    errorCounts: java.util.Map[Errors, Integer]
  ): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    sendResponse(new RequestChannel.CloseConnectionResponse(request))
  }

  def sendResponse(
    request: RequestChannel.Request, // request
    response: AbstractResponse, // response
    onComplete: Option[Send => Unit] // onComplete method
  ): Unit = {
    updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala)
    //  1. 编码response
    //  2. 将response交给processor的responseQueue
    sendResponse(new RequestChannel.SendResponse(
      request,
      request.buildResponseSend(response), // 此处完成了response的编码
      request.responseNode(response),
      onComplete
    ))
  }

  def sendNoOpResponse(request: RequestChannel.Request): Unit = {
    sendResponse(new network.RequestChannel.NoOpResponse(request))
  }

  def startThrottling(request: RequestChannel.Request): Unit = {
    sendResponse(new RequestChannel.StartThrottlingResponse(request))
  }

  def endThrottling(request: RequestChannel.Request): Unit = {
    sendResponse(new EndThrottlingResponse(request))
  }

  /** 给客户端发送请求
   *  将response交给processor的responseQueue
   * Send a response back to the socket server to be sent over the network */
  private[network] def sendResponse(response: RequestChannel.Response): Unit = {
    // 1. if need Traced
    if (isTraceEnabled) {
      val requestHeader = response.request.headerForLoggingOrThrottling()
      val message = response match {
        case sendResponse: SendResponse =>
          s"Sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} of ${sendResponse.responseSend.size} bytes."
        case _: NoOpResponse =>
          s"Not sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} as it's not required."
        case _: CloseConnectionResponse =>
          s"Closing connection for client ${requestHeader.clientId} due to error during ${requestHeader.apiKey}."
        case _: StartThrottlingResponse =>
          s"Notifying channel throttling has started for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
        case _: EndThrottlingResponse =>
          s"Notifying channel throttling has ended for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
      }
      trace(message)
    }
    // 2. Match the type of response and perform corresponding processing
    response match {
      // We should only send one of the following per request
      case _: SendResponse | _: NoOpResponse | _: CloseConnectionResponse =>
        val request = response.request
        val timeNanos = time.nanoseconds()
        // calculate response Complete TimeNanos
        request.responseCompleteTimeNanos = timeNanos
        // calculate apiLocal Complete TimeNanos
        if (request.apiLocalCompleteTimeNanos == -1L)
          request.apiLocalCompleteTimeNanos = timeNanos
        // If this callback was executed after KafkaApis returned we will need to adjust the callback completion time here.
        if (request.callbackRequestDequeueTimeNanos.isDefined && request.callbackRequestCompleteTimeNanos.isEmpty)
          request.callbackRequestCompleteTimeNanos = Some(time.nanoseconds())
      // For a given request, these may happen in addition to one in the previous section, skip updating the metrics
      case _: StartThrottlingResponse | _: EndThrottlingResponse => ()
    }

    // 最后交给了Processor
    // 3. find the response correspond processor
    // The corresponding request and response need to be the same processor
    val processor = processors.get(response.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    if (processor != null) {
      // add the Response to the processor.responseQueue
      processor.enqueueResponse(response)
    }
  }

  /** Get the next request or block until specified time has elapsed 
   *  Check the callback queue and execute first if present since these
   *  requests have already waited in line. */
  def receiveRequest(timeout: Long): RequestChannel.BaseRequest = {
    val callbackRequest = callbackQueue.poll()
    if (callbackRequest != null)
      callbackRequest
    else {
      // 取出下一个Request
      val request = requestQueue.poll(timeout, TimeUnit.MILLISECONDS)
      request match {
        case WakeupRequest => callbackQueue.poll()
        case _ => request
      }
    }
  }

  /**
   * 取请求
   * Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.BaseRequest =
    requestQueue.take()

  def updateErrorMetrics(apiKey: ApiKeys, errors: collection.Map[Errors, Integer]): Unit = {
    errors.forKeyValue { (error, count) =>
      metrics(apiKey.name).markErrorMeter(error, count)
    }
  }

  def clear(): Unit = {
    requestQueue.clear()
    callbackQueue.clear()
  }

  def shutdown(): Unit = {
    clear()
    metrics.close()
  }

  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest)

  def sendCallbackRequest(request: CallbackRequest): Unit = {
    callbackQueue.put(request)
    if (!requestQueue.offer(RequestChannel.WakeupRequest))
      trace("Wakeup request could not be added to queue. This means queue is full, so we will still process callback.")
  }

}

object RequestMetrics {
  val consumerFetchMetricName = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName = ApiKeys.FETCH.name + "Follower"

  val verifyPartitionsInTxnMetricName = ApiKeys.ADD_PARTITIONS_TO_TXN.name + "Verification"

  val RequestsPerSec = "RequestsPerSec"
  val RequestQueueTimeMs = "RequestQueueTimeMs"
  val LocalTimeMs = "LocalTimeMs"
  val RemoteTimeMs = "RemoteTimeMs"
  val ThrottleTimeMs = "ThrottleTimeMs"
  val ResponseQueueTimeMs = "ResponseQueueTimeMs"
  val ResponseSendTimeMs = "ResponseSendTimeMs"
  val TotalTimeMs = "TotalTimeMs"
  val RequestBytes = "RequestBytes"
  val MessageConversionsTimeMs = "MessageConversionsTimeMs"
  val TemporaryMemoryBytes = "TemporaryMemoryBytes"
  val ErrorsPerSec = "ErrorsPerSec"
}

class RequestMetrics(name: String) {

  import RequestMetrics._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val tags = Map("request" -> name).asJava
  val requestRateInternal = new Pool[Short, Meter]()
  // time a request spent in a request queue
  val requestQueueTimeHist = metricsGroup.newHistogram(RequestQueueTimeMs, true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = metricsGroup.newHistogram(LocalTimeMs, true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = metricsGroup.newHistogram(RemoteTimeMs, true, tags)
  // time a request is throttled, not part of the request processing time (throttling is done at the client level
  // for clients that support KIP-219 and by muting the channel for the rest)
  val throttleTimeHist = metricsGroup.newHistogram(ThrottleTimeMs, true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = metricsGroup.newHistogram(ResponseQueueTimeMs, true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = metricsGroup.newHistogram(ResponseSendTimeMs, true, tags)
  val totalTimeHist = metricsGroup.newHistogram(TotalTimeMs, true, tags)
  // request size in bytes
  val requestBytesHist = metricsGroup.newHistogram(RequestBytes, true, tags)
  // time for message conversions (only relevant to fetch and produce requests)
  val messageConversionsTimeHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(metricsGroup.newHistogram(MessageConversionsTimeMs, true, tags))
    else
      None
  // Temporary memory allocated for processing request (only populated for fetch and produce requests)
  // This shows the memory allocated for compression/conversions excluding the actual request size
  val tempMemoryBytesHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(metricsGroup.newHistogram(TemporaryMemoryBytes, true, tags))
    else
      None

  private val errorMeters = mutable.Map[Errors, ErrorMeter]()
  Errors.values.foreach(error => errorMeters.put(error, new ErrorMeter(name, error)))

  def requestRate(version: Short): Meter =
    requestRateInternal.getAndMaybePut(version, metricsGroup.newMeter(RequestsPerSec, "requests", TimeUnit.SECONDS, tagsWithVersion(version)))

  private def tagsWithVersion(version: Short): java.util.Map[String, String] = {
    val nameAndVersionTags = new util.HashMap[String, String](tags.size() + 1)
    nameAndVersionTags.putAll(tags)
    nameAndVersionTags.put("version", version.toString)
    nameAndVersionTags
  }

  class ErrorMeter(name: String, error: Errors) {
    private val tags = Map("request" -> name, "error" -> error.name).asJava

    @volatile private var meter: Meter = _

    def getOrCreateMeter(): Meter = {
      if (meter != null)
        meter
      else {
        synchronized {
          if (meter == null)
             meter = metricsGroup.newMeter(ErrorsPerSec, "requests", TimeUnit.SECONDS, tags)
          meter
        }
      }
    }

    def removeMeter(): Unit = {
      synchronized {
        if (meter != null) {
          metricsGroup.removeMetric(ErrorsPerSec, tags)
          meter = null
        }
      }
    }
  }

  def markErrorMeter(error: Errors, count: Int): Unit = {
    errorMeters(error).getOrCreateMeter().mark(count.toLong)
  }

  def removeMetrics(): Unit = {
    for (version <- requestRateInternal.keys) {
      metricsGroup.removeMetric(RequestsPerSec, tagsWithVersion(version))
    }
    metricsGroup.removeMetric(RequestQueueTimeMs, tags)
    metricsGroup.removeMetric(LocalTimeMs, tags)
    metricsGroup.removeMetric(RemoteTimeMs, tags)
    metricsGroup.removeMetric(RequestsPerSec, tags)
    metricsGroup.removeMetric(ThrottleTimeMs, tags)
    metricsGroup.removeMetric(ResponseQueueTimeMs, tags)
    metricsGroup.removeMetric(TotalTimeMs, tags)
    metricsGroup.removeMetric(ResponseSendTimeMs, tags)
    metricsGroup.removeMetric(RequestBytes, tags)
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name) {
      metricsGroup.removeMetric(MessageConversionsTimeMs, tags)
      metricsGroup.removeMetric(TemporaryMemoryBytes, tags)
    }
    errorMeters.values.foreach(_.removeMeter())
    errorMeters.clear()
  }
}
