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
package org.apache.kafka.server.util.timer;

import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SystemTimer implements Timer {
    // timeout timer
    // Single Thread pool to do TimerTask
    private final ExecutorService taskExecutor;
    // queue save all TimerTaskList
    private final DelayQueue<TimerTaskList> delayQueue;
    // the sum of all TimerTasks
    private final AtomicInteger taskCounter;
    // TimingWheel
    private final TimingWheel timingWheel;

    // Locks used to protect data structures while ticking
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    public SystemTimer(String executorName) {
        this(executorName, 1, 20, Time.SYSTEM.hiResClockMs());
    }

    public SystemTimer(
        String executorName,
        long tickMs,
        int wheelSize,
        long startMs
    ) {
//  执行”延时请求“的”回调逻辑“的线程
//      一个单线程的，且固定线程数的线程池。为什么要设置为单线程呢？如果某个应用的回调阻塞了，那岂不是所有线程池中的回调均会阻塞吗？
//      的确是这样，不过考虑到这个线程池做的工作只是回调，一般是网络发送模块，数据其实都是已经准备好的，TPS响应是非常快的，因此通常也不会成为瓶颈
        this.taskExecutor = Executors.newFixedThreadPool(1,
            runnable -> KafkaThread.nonDaemon("executor-" + executorName, runnable));
        this.delayQueue = new DelayQueue<>();
        this.taskCounter = new AtomicInteger(0);
        // 初始化时间轮
        this.timingWheel = new TimingWheel(
            tickMs,
            wheelSize,
            startMs,
            taskCounter,
            delayQueue
        );
    }

    public void add(TimerTask timerTask) {
        // 1. get readLock first
        readLock.lock();
        try {
            // 2. call addTimerTaskEntry() to add TimerTask to TimingWheel
            addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs()));
        } finally {
            // 3. release readLock
            readLock.unlock();
        }
    }

    private void addTimerTaskEntry(TimerTaskEntry timerTaskEntry) {
        if (!timingWheel.add(timerTaskEntry)) {
            // Already expired or cancelled
            if (!timerTaskEntry.cancelled()) {
                // add timerTask to ExecutorService
                taskExecutor.submit(timerTaskEntry.timerTask);
            }
        }
    }

    /**
     * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
     * waits up to timeoutMs before giving up.
     */
    public boolean advanceClock(long timeoutMs) throws InterruptedException {
        // 调用延迟队列的poll操作，用来获取那些已经超时的TimerTaskList
        TimerTaskList bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (bucket != null) {
            writeLock.lock();
            try {
                while (bucket != null) {
                    // only update currentTimeMs for TimingWheel
                    timingWheel.advanceClock(bucket.getExpiration());
                    // get all TimerTaskEntries of this bucket and do "addTimerTaskEntry" for each TimerTaskEntry
                    bucket.flush(this::addTimerTaskEntry);
                    // get next bucket
                    bucket = delayQueue.poll();
                }
            } finally {
                writeLock.unlock();
            }
            return true;
        } else {
            return false;
        }
    }

    public int size() {
        return taskCounter.get();
    }

    @Override
    public void close() {
        taskExecutor.shutdown();
    }
}
