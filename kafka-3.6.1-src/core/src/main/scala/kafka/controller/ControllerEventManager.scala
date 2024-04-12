/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import com.yammer.metrics.core.Timer

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.ShutdownableThread

import scala.collection._

object ControllerEventManager {
  // ThreadName
  val ControllerEventThreadName = "controller-event-thread"
  // Metrics
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

trait ControllerEventProcessor {
  // get a Controller Event -> Process event in order
  def process(event: ControllerEvent): Unit
  // get a Controller Event -> Process event in a preemptive manner -> Only supports type 2 events: ShutdownEventThread + Expire
  def preempt(event: ControllerEvent): Unit
}

/**
 * @param event: ControllerEvent
 * @param enqueueTimeMs: the timestamp when ControllerEvent putted to queue
 */
class QueuedEvent(val event: ControllerEvent,
                  val enqueueTimeMs: Long) {

  // mark whether the ControllerEvent begin to handle
  val processingStarted = new CountDownLatch(1)

  // mark whether the event has been processed
  val spent = new AtomicBoolean(false)

  /**
   * handle ControllerEvent
   * @param processor
   */
  def process(processor: ControllerEventProcessor): Unit = {
    // 1. if the event has been processed, return directly
    if (spent.getAndSet(true))
      return
    // 2. mark the ControllerEvent begin to handle
    processingStarted.countDown()
    // 3. call ControllerEventProcessor to process specific event
    processor.process(event)
  }

  /**
   * handle ControllerEvent preemptive
   * @param processor
   */
  def preempt(processor: ControllerEventProcessor): Unit = {
    // 1. if the event has been processed, return directly
    if (spent.getAndSet(true))
      return
    // 2. call ControllerEventProcessor preempt() to handle event
    processor.preempt(event)
  }

  /**
   * Block waiting for ControllerEvent processing to complete
   */
  def awaitProcessing(): Unit = {
    processingStarted.await()
  }

  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

/**
 * @param controllerId: the id of controllerId
 * @param processor: ControllerEventProcessor, contain process(event) and preempt(event)
 * @param time
 * @param rateAndTimeMetrics: Metrics for ControllerEventProcessor rate and time
 * @param eventQueueTimeTimeoutMs: ControllerEvent in Queue Timeout Ms
 */
class ControllerEventManager(controllerId: Int,
                             processor: ControllerEventProcessor,
                             time: Time,
                             rateAndTimeMetrics: Map[ControllerState, Timer],
                             eventQueueTimeTimeoutMs: Long = 300000) {
  import ControllerEventManager._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  // @volatile param, ControllerState--Idle
  @volatile private var _state: ControllerState = ControllerState.Idle

  // A reentrant lock to ensure LinkedBlockingQueue operations in a multi-threaded environment
  private val putLock = new ReentrantLock()

  // queue use to save ControllerEvent
  private val queue = new LinkedBlockingQueue[QueuedEvent]

  // Visible for test
  // ** ControllerEventThread to handle event in LinkedBlockingQueue
  private[controller] var thread = new ControllerEventThread(ControllerEventThreadName)

  private val eventQueueTimeHist = metricsGroup.newHistogram(EventQueueTimeMetricName)

  metricsGroup.newGauge(EventQueueSizeMetricName, () => queue.size)

  def state: ControllerState = _state

  // start ControllerEventThread
  def start(): Unit = thread.start()
  // close ControllerEventThread
  def close(): Unit = {
    try {
      // 1.
      thread.initiateShutdown()
      // 2. Used for high-priority preemptive ControllerEvent processing
      clearAndPut(ShutdownEventThread)
      // 3. use this API to wait until the shutdown is complete.
      thread.awaitShutdown()
    } finally {
      metricsGroup.removeMetric(EventQueueTimeMetricName)
      metricsGroup.removeMetric(EventQueueSizeMetricName)
    }
  }

  /**
   * put ControllerEvent to LinkedBlockingQueue
   * @param event
   * @return
   */
  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    // 1. construct queuedEvent
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    // 2. put ControllerEvent to LinkedBlockingQueue
    queue.put(queuedEvent)
    // 3. return queuedEvent
    queuedEvent
  }

  /**
   * Used for high-priority preemptive ControllerEvent processing
   * @param event
   * @return
   */
  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock){
    // 1. construct an ArrayList to preserve "Preempted ControllerEvent"
    val preemptedEvents = new ArrayList[QueuedEvent]()
    queue.drainTo(preemptedEvents)
    // 2. "Preempted ControllerEvent" to execute actions when preempted
    preemptedEvents.forEach(_.preempt(processor))
    // 3. put high-priority preemptive ControllerEvent to LinkedBlockingQueue
    put(event)
  }

  def isEmpty: Boolean = queue.isEmpty

  class ControllerEventThread(name: String)
    extends ShutdownableThread(
      name, false, s"[ControllerEventThread controllerId=$controllerId] ")
      with Logging {

    logIdent = logPrefix

    override def doWork(): Unit = {
      // 1. get event from queue
      val dequeued = pollFromEventQueue()
      // 2. handle event
      dequeued.event match {
        // case1: ShutdownEventThread
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        // case2: common controllerEvent
        case controllerEvent =>
          _state = controllerEvent.state
          // update the "How long the event is kept in the queue"
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

          try {
            // call ControllerEventProcessor to process event
            def process(): Unit = dequeued.process(processor)

            // Calculate processing rate
            rateAndTimeMetrics.get(state) match {
              // delayed scheduling
              case Some(timer) => timer.time(() => process())
              case None => process()
            }
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
          }

          // update ControllerState -> Idle
          _state = ControllerState.Idle
      }
    }
  }

  /**
   * get ControllerEvent from queue
   * @return
   */
  private def pollFromEventQueue(): QueuedEvent = {
    // 1. get count
    val count = eventQueueTimeHist.count()
    // 2.
    if (count != 0) {
      // 2.1 get ControllerEvent
      val event  = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
      if (event == null) {
        eventQueueTimeHist.clear()
        // Block for some time until there is an event in the queue
        queue.take()
      } else {
        // event not null, Return directly
        event
      }
    } else {
      // Block for some time until there is an event in the queue
      queue.take()
    }
  }

}
