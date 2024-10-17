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
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Time;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * 公平的：内存先分配给等待时间最长的线程，直到它有足够的内存
 * Q：为啥这么设计？
 * A：“当一个线程请求一块大内存，会被阻塞直到多个ByteBuffer被释放”，这种情况采用公平设计，可以防止线程出现饥饿或死锁
 *
 * 这里也是一个可能导致Producer阻塞的地方
 * </ol>
 */
public class BufferPool {

    static final String WAIT_TIME_SENSOR_NAME = "bufferpool-wait-time";
    // totalMemory of BufferPool: default 32M
    private final long totalMemory;
    // single ByteBuffer size: default 16k
    private final int poolableSize;
    // There may be situations where multiple threads allocate and recycle ByteBuffer concurrently.
    // Use ReentrantLock to control concurrency, and ensure thread safety
    private final ReentrantLock lock;
    // Fixed-size ByteBuffer objects cached
    private final Deque<ByteBuffer> free;
    // Record the Condition objects corresponding to "threads that are blocked because they cannot apply for enough space"
    private final Deque<Condition> waiters;
    /** Total available memory is the sum of nonPooledAvailableMemory and the number of byte buffers in free * poolableSize.  */
    // Total available memory = nonPooledAvailableMemory  + free * poolableSize
    // non Pooled Available Memory
    private long nonPooledAvailableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;
    private boolean closed;

    /**
     * Create a new buffer pool
     *
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        this.totalMemory = memory;
        // Producer构造函数中RecordAccumulator初始化时nonPooledAvailableMemory就是totalMemory
        // 需要的时候再实际分出ByteBuffer内存块
        this.nonPooledAvailableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor(WAIT_TIME_SENSOR_NAME);
        MetricName rateMetricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        MetricName totalMetricName = metrics.metricName("bufferpool-wait-time-total",
                                                   metricGrpName,
                                                   "*Deprecated* The total time an appender waits for space allocation.");
        MetricName totalNsMetricName = metrics.metricName("bufferpool-wait-time-ns-total",
                                                    metricGrpName,
                                                    "The total time in nanoseconds an appender waits for space allocation.");

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        MetricName bufferExhaustedRateMetricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        MetricName bufferExhaustedTotalMetricName = metrics.metricName("buffer-exhausted-total", metricGrpName, "The total number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(new Meter(bufferExhaustedRateMetricName, bufferExhaustedTotalMetricName));

        this.waitTime.add(new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName));
        this.waitTime.add(new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalNsMetricName));
        this.closed = false;
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     *
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        // 1. judge size cannot exceed totalMemory
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");
        // 2. initialize  new ByteBuffer
        ByteBuffer buffer = null;
        // 3. add lock
        this.lock.lock();
        // 4. if BufferPool is closed, throw Exception
        if (this.closed) {
            this.lock.unlock();
            throw new KafkaException("Producer closed while allocating memory");
        }
        // 5. do allocate
        try {
            // 5.1 [allocate case 1]: Get it directly from free deque： 情况1：最好的一种情况，free队列不空，且申请的内存大小和poolableSize一样
            // check if we have a free buffer of the right size pooled
            if (size == poolableSize && !this.free.isEmpty())
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            int freeListSize = freeSize() * this.poolableSize;
            // 第二种情况：BufferPool能分配用户需要大小的ByteBuffer，你别管我从哪扣出来，反正我非池化的和free队列加起来可用的肯定大于你的
            if (this.nonPooledAvailableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request, but need to allocate the buffer
                freeUp(size); // 判断非池化可用够不够，不够的话需要从free中扣出来补足
                // Q:这里可能有一点疑问，我还啥都没干呢，还没从非池化可用中分内存的，怎么就标记非池化可用减少了？
                // A:这种情况会在方法最后调safeAllocateByteBuffer(size)
                this.nonPooledAvailableMemory -= size;
            } else {
                // 第三种情况：BufferPool不能分配用户需要大小的ByteBuffer了，得有人等了
                // we are out of memory and will have to block
                int accumulated = 0;
                Condition moreMemory = this.lock.newCondition();
                try {
                    // 计算最长阻塞时间
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    // 把这个需要的人，放到“需要者队列”最后，保证公平
                    this.waiters.addLast(moreMemory);
                    // loop over and over until we have a buffer or have reserved
                    // enough memory to allocate one
                    while (accumulated < size) {
                        long startWaitNs = time.nanoseconds();
                        long timeNs;
                        boolean waitingTimeElapsed;
                        try {
                            // 当前线程阻塞等待至最长阻塞时间
                            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                        } finally {
                            long endWaitNs = time.nanoseconds();
                            timeNs = Math.max(0L, endWaitNs - startWaitNs);
                            recordWaitTime(timeNs);
                        }

                        if (this.closed)
                            throw new KafkaException("Producer closed while allocating memory");

                        if (waitingTimeElapsed) {
                            this.metrics.sensor("buffer-exhausted-records").record();
                            throw new BufferExhaustedException("Failed to allocate " + size + " bytes within the configured max blocking time "
                                + maxTimeToBlockMs + " ms. Total memory: " + totalMemory() + " bytes. Available memory: " + availableMemory()
                                + " bytes. Poolable size: " + poolableSize() + " bytes");
                        }

                        remainingTimeToBlockNs -= timeNs;

                        // check if we can satisfy this request from the free list,
                        // otherwise allocate memory
                        if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                            // just grab a buffer from the free list
                            buffer = this.free.pollFirst();
                            accumulated = size;
                        } else {
                            // we'll need to allocate memory, but we may only get
                            // part of what we need on this iteration
                            freeUp(size - accumulated);
                            int got = (int) Math.min(size - accumulated, this.nonPooledAvailableMemory);
                            this.nonPooledAvailableMemory -= got;
                            accumulated += got;
                        }
                    }
                    // Don't reclaim memory on throwable since nothing was thrown
                    accumulated = 0;
                } finally {
                    // 2种情况
                    // 情况1：出异常了，把想分配的还给nonPooledAvailableMemory，其实也就是nonPooledAvailableMemory值大小标记复原 +  移除当前等待者（都出异常了，还等个啥）
                    // When this loop was not able to successfully terminate don't loose available memory
                    this.nonPooledAvailableMemory += accumulated;
                    // 情况2：没出异常，accumulated已经被置为0了，也没什么影响 + 移除当前等待者（下面步骤就会为你分ByteBuffer了）
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            // signal any additional waiters if there is more memory left
            // over for them
            try {
                if (!(this.nonPooledAvailableMemory == 0 && this.free.isEmpty()) && !this.waiters.isEmpty())
                    this.waiters.peekFirst().signal();
            } finally {
                // Another finally... otherwise find bugs complains
                lock.unlock();
            }
        }

        if (buffer == null) // buffer == null 表示没能从free中取出一块现成的，就要从“非池化可用内存”，也就是“堆”中分一块出来了
            // 但是，走到这里“理论上”我是可以分给你size大小的内存了
            return safeAllocateByteBuffer(size);
        else
            return buffer; // 从free列表取的，返回就好
    }

    // Protected for testing
    protected void recordWaitTime(long timeNs) {
        this.waitTime.record(timeNs, time.milliseconds());
    }

    /**
     * Allocate a buffer.  If buffer allocation fails (e.g. because of OOM) then return the size count back to
     * available memory and signal the next waiter if it exists.
     */
    private ByteBuffer safeAllocateByteBuffer(int size) {
        boolean error = true;
        try {
            // call allocateByteBuffer(...)
            ByteBuffer buffer = allocateByteBuffer(size);
            error = false;
            return buffer;
        } finally {
//  情况1:
//  正常执行：如果 allocateByteBuffer(size) 成功执行，并且没有抛出异常，那么 error 变量会被设置为 false，然后 try 块正常结束，finally 块会执行。但由于 error 变量为 false，finally 块中的条件判断 if (error) 不会执行，因此 finally 块中的代码不会执行。
//  情况2:
//  异常发生：如果在调用 allocateByteBuffer(size) 时抛出了异常，那么 try 块会立即终止，并且控制流会跳转到 finally 块。在这种情况下，error 变量仍然为 true（因为它在 try 块中没有被设置为 false），所以 finally 块中的条件判断 if (error) 会执行，并且会执行 finally 块中的代码，将内存返回到非池化可用内存中，并可能唤醒等待的线程。
//  情况3:
//  在 try 块中显式抛出异常：如果在 try 块中有代码显式地抛出了异常（例如 throw new Exception()），那么这个异常会被 finally 块捕获，并且 finally 块会执行。同样，由于 error 变量为 true，finally 块中的代码会执行。
//  情况4:
//  在 try 块中返回：如果在 try 块中有 return 语句，并且没有发生异常，那么 finally 块仍然会执行。但由于 error 变量被设置为 false，finally 块中的代码不会执行。
            if (error) {
                // error == true means "allocate failed" so should return buffer to nonPooledAvailableMemory
                this.lock.lock();
                try {
                    // return buffer to nonPooledAvailableMemory
                    // 走到这里就代表分内存时出异常了，没能从非池化可用中分出内存，前面标记了分出去nonPooledAvailableMemory -= size
                    // 这边标记还回去：nonPooledAvailableMemory += size
                    this.nonPooledAvailableMemory += size;
                    if (!this.waiters.isEmpty())
                        // Wake up the waiting thread
                        this.waiters.peekFirst().signal();
                } finally {
                    this.lock.unlock();
                }
            }
        }
    }

    // Protected for testing.
    protected ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     * 非池化可用内存 够 分给用户需要的内存 就什么都不干
     * 不然就一直从free队列中扣，直到 够
     */
    private void freeUp(int size) {
        // free队列中不为空且“非池化可用”比用户需要的少
        while (!this.free.isEmpty() && this.nonPooledAvailableMemory < size)
            // 从free队列中抠出来，补给“非池化可用内存”，直到“非池化可用内存”够分给用户需要的
            this.nonPooledAvailableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     *
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this may be smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        // 1. add lock
        lock.lock();
        try {
            // 2. deallocate ByteBuffer
            // 2.1. case1: if ByteBuffer to deallocate sizes == poolableSize(16k) -> clear and put to the free deque
            if (size == this.poolableSize && size == buffer.capacity()) {
                // clear
                buffer.clear(); // 情况1 等于poolableSize大小的ByteBuffer=》只清空内容，不GC，放回free列表复用
                // put to the free deque
                this.free.add(buffer);
            } else {
            // 2.2. case2: The action of recycling the ByteBuffer is left to the JVM to complete.
                // add the nonPooledAvailableMemory
                this.nonPooledAvailableMemory += size; // 情况1 不等于poolableSize大小的ByteBuffer=》 等GC，标记nonPooledAvailableMemory增加
            }
            // 3. Wake up the first blocked thread in waiters
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal(); // 回收完内存会signal一下阻塞的线程
        } finally {
        // 4. unlock
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        if (buffer != null)
            deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory + freeSize() * (long) this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    // Protected for testing.
    protected int freeSize() {
        return this.free.size();
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }

    /**
     * Closes the buffer pool. Memory will be prevented from being allocated, but may be deallocated. All allocations
     * awaiting available memory will be notified to abort.
     */
    public void close() {
        this.lock.lock();
        this.closed = true;
        try {
            for (Condition waiter : this.waiters)
                waiter.signal();
        } finally {
            this.lock.unlock();
        }
    }
}
