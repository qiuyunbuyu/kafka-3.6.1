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
package org.apache.kafka.common.record;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Crc32C;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.OptionalLong;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;

/**
 * RecordBatch implementation for magic 2 and above. The schema is given below:
 *
 * RecordBatch =>
 *  BaseOffset => Int64
 *  Length => Int32
 *  PartitionLeaderEpoch => Int32
 *  Magic => Int8
 *  CRC => Uint32
 *  Attributes => Int16
 *  LastOffsetDelta => Int32 // also serves as LastSequenceDelta
 *  BaseTimestamp => Int64
 *  MaxTimestamp => Int64
 *  ProducerId => Int64
 *  ProducerEpoch => Int16
 *  BaseSequence => Int32 ”以上为v2版本ProducerBatch的‘头字段’，”以下为单条Record“
 *  Records => [Record]
 *
 * Note that when compression is enabled (see attributes below), the compressed record data is serialized
 * directly following the count of the number of records.
 *
 * The CRC covers the data from the attributes to the end of the batch (i.e. all the bytes that follow the CRC). It is
 * located after the magic byte, which means that clients must parse the magic byte before deciding how to interpret
 * the bytes between the batch length and the magic byte. The partition leader epoch field is not included in the CRC
 * computation to avoid the need to recompute the CRC when this field is assigned for every batch that is received by
 * the broker. The CRC-32C (Castagnoli) polynomial is used for the computation.
 *
 * On Compaction: Unlike the older message formats, magic v2 and above preserves the first and last offset/sequence
 * numbers from the original batch when the log is cleaned. This is required in order to be able to restore the
 * producer's state when the log is reloaded. If we did not retain the last sequence number, then following
 * a partition leader failure, once the new leader has rebuilt the producer state from the log, the next sequence
 * expected number would no longer be in sync with what was written by the client. This would cause an
 * unexpected OutOfOrderSequence error, which is typically fatal. The base sequence number must be preserved for
 * duplicate checking: the broker checks incoming Produce requests for duplicates by verifying that the first and
 * last sequence numbers of the incoming batch match the last from that producer.
 *
 * Note that if all of the records in a batch are removed during compaction, the broker may still retain an empty
 * batch header in order to preserve the producer sequence information as described above. These empty batches
 * are retained only until either a new sequence number is written by the corresponding producer or the producerId
 * is expired from lack of activity.
 *
 * There is no similar need to preserve the timestamp from the original batch after compaction. The BaseTimestamp
 * field therefore reflects the timestamp of the first record in the batch in most cases. If the batch is empty, the
 * BaseTimestamp will be set to -1 (NO_TIMESTAMP). If the delete horizon flag is set to 1, the BaseTimestamp
 * will be set to the time at which tombstone records and aborted transaction markers in the batch should be removed.
 *
 * Similarly, the MaxTimestamp field reflects the maximum timestamp of the current records if the timestamp type
 * is CREATE_TIME. For LOG_APPEND_TIME, on the other hand, the MaxTimestamp field reflects the timestamp set
 * by the broker and is preserved after compaction. Additionally, the MaxTimestamp of an empty batch always retains
 * the previous value prior to becoming empty.
 *
 * The current attributes are given below:
 *
 *  ---------------------------------------------------------------------------------------------------------------------------
 *  | Unused (7-15) | Delete Horizon Flag (6) | Control (5) | Transactional (4) | Timestamp Type (3) | Compression Type (0-2) |
 *  ---------------------------------------------------------------------------------------------------------------------------
 */
public class DefaultRecordBatch extends AbstractRecordBatch implements MutableRecordBatch {
    static final int BASE_OFFSET_OFFSET = 0;
    static final int BASE_OFFSET_LENGTH = 8;
    static final int LENGTH_OFFSET = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
    static final int LENGTH_LENGTH = 4;
    static final int PARTITION_LEADER_EPOCH_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH;
    static final int PARTITION_LEADER_EPOCH_LENGTH = 4;
    static final int MAGIC_OFFSET = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
    static final int MAGIC_LENGTH = 1;
    public static final int CRC_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    static final int CRC_LENGTH = 4;
    static final int ATTRIBUTES_OFFSET = CRC_OFFSET + CRC_LENGTH;
    static final int ATTRIBUTE_LENGTH = 2;
    public static final int LAST_OFFSET_DELTA_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    static final int LAST_OFFSET_DELTA_LENGTH = 4;
    static final int BASE_TIMESTAMP_OFFSET = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
    static final int BASE_TIMESTAMP_LENGTH = 8;
    static final int MAX_TIMESTAMP_OFFSET = BASE_TIMESTAMP_OFFSET + BASE_TIMESTAMP_LENGTH;
    static final int MAX_TIMESTAMP_LENGTH = 8;
    static final int PRODUCER_ID_OFFSET = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
    static final int PRODUCER_ID_LENGTH = 8;
    static final int PRODUCER_EPOCH_OFFSET = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
    static final int PRODUCER_EPOCH_LENGTH = 2;
    static final int BASE_SEQUENCE_OFFSET = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
    static final int BASE_SEQUENCE_LENGTH = 4;
    public static final int RECORDS_COUNT_OFFSET = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
    static final int RECORDS_COUNT_LENGTH = 4;
    static final int RECORDS_OFFSET = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
    public static final int RECORD_BATCH_OVERHEAD = RECORDS_OFFSET;

    private static final byte COMPRESSION_CODEC_MASK = 0x07;
    private static final byte TRANSACTIONAL_FLAG_MASK = 0x10;
    private static final int CONTROL_FLAG_MASK = 0x20;
    private static final byte DELETE_HORIZON_FLAG_MASK = 0x40;
    private static final byte TIMESTAMP_TYPE_MASK = 0x08;

    private final ByteBuffer buffer;

    DefaultRecordBatch(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    @Override
    public void ensureValid() {
        if (sizeInBytes() < RECORD_BATCH_OVERHEAD)
            throw new CorruptRecordException("Record batch is corrupt (the size " + sizeInBytes() +
                    " is smaller than the minimum allowed overhead " + RECORD_BATCH_OVERHEAD + ")");

        if (!isValid())
            throw new CorruptRecordException("Record is corrupt (stored crc = " + checksum()
                    + ", computed crc = " + computeChecksum() + ")");
    }

    /**
     * Gets the base timestamp of the batch which is used to calculate the record timestamps from the deltas.
     * 
     * @return The base timestamp
     */
    public long baseTimestamp() {
        return buffer.getLong(BASE_TIMESTAMP_OFFSET);
    }

    @Override
    public long maxTimestamp() {
        return buffer.getLong(MAX_TIMESTAMP_OFFSET);
    }

    @Override
    public TimestampType timestampType() {
        return (attributes() & TIMESTAMP_TYPE_MASK) == 0 ? TimestampType.CREATE_TIME : TimestampType.LOG_APPEND_TIME;
    }

    @Override
    public long baseOffset() {
        return buffer.getLong(BASE_OFFSET_OFFSET);
    }

    @Override
    public long lastOffset() {
        return baseOffset() + lastOffsetDelta();
    }

    @Override
    public long producerId() {
        return buffer.getLong(PRODUCER_ID_OFFSET);
    }

    @Override
    public short producerEpoch() {
        return buffer.getShort(PRODUCER_EPOCH_OFFSET);
    }

    @Override
    public int baseSequence() {
        return buffer.getInt(BASE_SEQUENCE_OFFSET);
    }

    private int lastOffsetDelta() {
        return buffer.getInt(LAST_OFFSET_DELTA_OFFSET);
    }

    @Override
    public int lastSequence() {
        int baseSequence = baseSequence();
        if (baseSequence == RecordBatch.NO_SEQUENCE)
            return RecordBatch.NO_SEQUENCE;
        return incrementSequence(baseSequence, lastOffsetDelta());
    }

    @Override
    public CompressionType compressionType() {
        return CompressionType.forId(attributes() & COMPRESSION_CODEC_MASK);
    }

    @Override
    public int sizeInBytes() {
        return LOG_OVERHEAD + buffer.getInt(LENGTH_OFFSET);
    }

    private int count() {
        return buffer.getInt(RECORDS_COUNT_OFFSET);
    }

    @Override
    public Integer countOrNull() {
        return count();
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.put(this.buffer.duplicate());
    }

    @Override
    public void writeTo(ByteBufferOutputStream outputStream) {
        outputStream.write(this.buffer.duplicate());
    }

    @Override
    public boolean isTransactional() {
        return (attributes() & TRANSACTIONAL_FLAG_MASK) > 0;
    }

    private boolean hasDeleteHorizonMs() {
        return (attributes() & DELETE_HORIZON_FLAG_MASK) > 0;
    }

    @Override
    public OptionalLong deleteHorizonMs() {
        if (hasDeleteHorizonMs())
            return OptionalLong.of(buffer.getLong(BASE_TIMESTAMP_OFFSET));
        else
            return OptionalLong.empty();
    }

    @Override
    public boolean isControlBatch() {
        return (attributes() & CONTROL_FLAG_MASK) > 0;
    }

    @Override
    public int partitionLeaderEpoch() {
        return buffer.getInt(PARTITION_LEADER_EPOCH_OFFSET);
    }

    public InputStream recordInputStream(BufferSupplier bufferSupplier) {
        final ByteBuffer buffer = this.buffer.duplicate();
        buffer.position(RECORDS_OFFSET);
        return compressionType().wrapForInput(buffer, magic(), bufferSupplier);
    }

    private CloseableIterator<Record> compressedIterator(BufferSupplier bufferSupplier, boolean skipKeyValue) {
        final InputStream inputStream = recordInputStream(bufferSupplier);

        if (skipKeyValue) {
            return new StreamRecordIterator(inputStream) {
                @Override
                protected Record doReadRecord(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime) throws IOException {
                    return DefaultRecord.readPartiallyFrom(inputStream, baseOffset, baseTimestamp, baseSequence, logAppendTime);
                }
            };
        } else {
            return new StreamRecordIterator(inputStream) {
                @Override
                protected Record doReadRecord(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime) throws IOException {
                    return DefaultRecord.readFrom(inputStream, baseOffset, baseTimestamp, baseSequence, logAppendTime);
                }
            };
        }
    }

    private CloseableIterator<Record> uncompressedIterator() {
        final ByteBuffer buffer = this.buffer.duplicate();
        buffer.position(RECORDS_OFFSET);
        return new RecordIterator() {
            @Override
            protected Record readNext(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime) {
                try {
                    return DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, baseSequence, logAppendTime);
                } catch (BufferUnderflowException e) {
                    throw new InvalidRecordException("Incorrect declared batch size, premature EOF reached");
                }
            }
            @Override
            protected boolean ensureNoneRemaining() {
                return !buffer.hasRemaining();
            }
            @Override
            public void close() {}
        };
    }

    @Override
    public Iterator<Record> iterator() {
        if (count() == 0)
            return Collections.emptyIterator();

        if (!isCompressed())
            return uncompressedIterator();

        // for a normal iterator, we cannot ensure that the underlying compression stream is closed,
        // so we decompress the full record set here. Use cases which call for a lower memory footprint
        // can use `streamingIterator` at the cost of additional complexity
        try (CloseableIterator<Record> iterator = compressedIterator(BufferSupplier.NO_CACHING, false)) {
            List<Record> records = new ArrayList<>(count());
            while (iterator.hasNext())
                records.add(iterator.next());
            return records.iterator();
        }
    }

    @Override
    public CloseableIterator<Record> skipKeyValueIterator(BufferSupplier bufferSupplier) {
        if (count() == 0) {
            return CloseableIterator.wrap(Collections.emptyIterator());
        }

        /*
         * For uncompressed iterator, it is actually not worth skipping key / value / headers at all since
         * its ByteBufferInputStream's skip() function is less efficient compared with just reading it actually
         * as it will allocate new byte array.
         */
        if (!isCompressed())
            return uncompressedIterator();

        // we define this to be a closable iterator so that caller (i.e. the log validator) needs to close it
        // while we can save memory footprint of not decompressing the full record set ahead of time
        return compressedIterator(bufferSupplier, true);
    }

    @Override
    public CloseableIterator<Record> streamingIterator(BufferSupplier bufferSupplier) {
        if (isCompressed())
            return compressedIterator(bufferSupplier, false);
        else
            return uncompressedIterator();
    }

    @Override
    public void setLastOffset(long offset) {
        buffer.putLong(BASE_OFFSET_OFFSET, offset - lastOffsetDelta());
    }

    @Override
    public void setMaxTimestamp(TimestampType timestampType, long maxTimestamp) {
        long currentMaxTimestamp = maxTimestamp();
        // We don't need to recompute crc if the timestamp is not updated.
        if (timestampType() == timestampType && currentMaxTimestamp == maxTimestamp)
            return;

        byte attributes = computeAttributes(compressionType(), timestampType, isTransactional(), isControlBatch(), hasDeleteHorizonMs());
        buffer.putShort(ATTRIBUTES_OFFSET, attributes);
        buffer.putLong(MAX_TIMESTAMP_OFFSET, maxTimestamp);
        long crc = computeChecksum();
        ByteUtils.writeUnsignedInt(buffer, CRC_OFFSET, crc);
    }

    @Override
    public void setPartitionLeaderEpoch(int epoch) {
        buffer.putInt(PARTITION_LEADER_EPOCH_OFFSET, epoch);
    }

    @Override
    public long checksum() {
        return ByteUtils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    public boolean isValid() {
        return sizeInBytes() >= RECORD_BATCH_OVERHEAD && checksum() == computeChecksum();
    }

    private long computeChecksum() {
        return Crc32C.compute(buffer, ATTRIBUTES_OFFSET, buffer.limit() - ATTRIBUTES_OFFSET);
    }

    private byte attributes() {
        // note we're not using the second byte of attributes
        return (byte) buffer.getShort(ATTRIBUTES_OFFSET);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DefaultRecordBatch that = (DefaultRecordBatch) o;
        return Objects.equals(buffer, that.buffer);
    }

    @Override
    public int hashCode() {
        return buffer != null ? buffer.hashCode() : 0;
    }

    private static byte computeAttributes(CompressionType type, TimestampType timestampType,
                                          boolean isTransactional, boolean isControl, boolean isDeleteHorizonSet) {
        if (timestampType == TimestampType.NO_TIMESTAMP_TYPE)
            throw new IllegalArgumentException("Timestamp type must be provided to compute attributes for message " +
                    "format v2 and above");

        byte attributes = isTransactional ? TRANSACTIONAL_FLAG_MASK : 0;
        if (isControl)
            attributes |= CONTROL_FLAG_MASK;
        if (type.id > 0)
            attributes |= (byte) (COMPRESSION_CODEC_MASK & type.id);
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            attributes |= TIMESTAMP_TYPE_MASK;
        if (isDeleteHorizonSet)
            attributes |= DELETE_HORIZON_FLAG_MASK;
        return attributes;
    }

    public static void writeEmptyHeader(ByteBuffer buffer,
                                        byte magic,
                                        long producerId,
                                        short producerEpoch,
                                        int baseSequence,
                                        long baseOffset,
                                        long lastOffset,
                                        int partitionLeaderEpoch,
                                        TimestampType timestampType,
                                        long timestamp,
                                        boolean isTransactional,
                                        boolean isControlRecord) {
        int offsetDelta = (int) (lastOffset - baseOffset);
        writeHeader(buffer, baseOffset, offsetDelta, DefaultRecordBatch.RECORD_BATCH_OVERHEAD, magic,
                    CompressionType.NONE, timestampType, RecordBatch.NO_TIMESTAMP, timestamp, producerId,
                    producerEpoch, baseSequence, isTransactional, isControlRecord, false, partitionLeaderEpoch, 0);
    }

    public static void writeHeader(ByteBuffer buffer,
                                   long baseOffset,
                                   int lastOffsetDelta,
                                   int sizeInBytes,
                                   byte magic,
                                   CompressionType compressionType,
                                   TimestampType timestampType,
                                   long baseTimestamp,
                                   long maxTimestamp,
                                   long producerId,
                                   short epoch,
                                   int sequence,
                                   boolean isTransactional,
                                   boolean isControlBatch,
                                   boolean isDeleteHorizonSet,
                                   int partitionLeaderEpoch,
                                   int numRecords) {
        if (magic < RecordBatch.CURRENT_MAGIC_VALUE)
            throw new IllegalArgumentException("Invalid magic value " + magic);
        if (baseTimestamp < 0 && baseTimestamp != NO_TIMESTAMP)
            throw new IllegalArgumentException("Invalid message timestamp " + baseTimestamp);

        short attributes = computeAttributes(compressionType, timestampType, isTransactional, isControlBatch, isDeleteHorizonSet);

        int position = buffer.position();
        buffer.putLong(position + BASE_OFFSET_OFFSET, baseOffset);
        buffer.putInt(position + LENGTH_OFFSET, sizeInBytes - LOG_OVERHEAD);
        buffer.putInt(position + PARTITION_LEADER_EPOCH_OFFSET, partitionLeaderEpoch);
        buffer.put(position + MAGIC_OFFSET, magic);
        buffer.putShort(position + ATTRIBUTES_OFFSET, attributes);
        buffer.putLong(position + BASE_TIMESTAMP_OFFSET, baseTimestamp);
        buffer.putLong(position + MAX_TIMESTAMP_OFFSET, maxTimestamp);
        buffer.putInt(position + LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta);
        buffer.putLong(position + PRODUCER_ID_OFFSET, producerId);
        buffer.putShort(position + PRODUCER_EPOCH_OFFSET, epoch);
        buffer.putInt(position + BASE_SEQUENCE_OFFSET, sequence);
        buffer.putInt(position + RECORDS_COUNT_OFFSET, numRecords);
        long crc = Crc32C.compute(buffer, ATTRIBUTES_OFFSET, sizeInBytes - ATTRIBUTES_OFFSET);
        buffer.putInt(position + CRC_OFFSET, (int) crc);
        buffer.position(position + RECORD_BATCH_OVERHEAD);
    }

    @Override
    public String toString() {
        return "RecordBatch(magic=" + magic() + ", offsets=[" + baseOffset() + ", " + lastOffset() + "], " +
                "sequence=[" + baseSequence() + ", " + lastSequence() + "], " +
                "isTransactional=" + isTransactional() + ", isControlBatch=" + isControlBatch() + ", " +
                "compression=" + compressionType() + ", timestampType=" + timestampType() + ", crc=" + checksum() + ")";
    }

    public static int sizeInBytes(long baseOffset, Iterable<Record> records) {
        Iterator<Record> iterator = records.iterator();
        if (!iterator.hasNext())
            return 0;

        int size = RECORD_BATCH_OVERHEAD;
        Long baseTimestamp = null;
        while (iterator.hasNext()) {
            Record record = iterator.next();
            int offsetDelta = (int) (record.offset() - baseOffset);
            if (baseTimestamp == null)
                baseTimestamp = record.timestamp();
            long timestampDelta = record.timestamp() - baseTimestamp;
            size += DefaultRecord.sizeInBytes(offsetDelta, timestampDelta, record.key(), record.value(),
                    record.headers());
        }
        return size;
    }

    public static int sizeInBytes(Iterable<SimpleRecord> records) {
        Iterator<SimpleRecord> iterator = records.iterator();
        if (!iterator.hasNext())
            return 0;

        int size = RECORD_BATCH_OVERHEAD;
        int offsetDelta = 0;
        Long baseTimestamp = null;
        while (iterator.hasNext()) {
            SimpleRecord record = iterator.next();
            if (baseTimestamp == null)
                baseTimestamp = record.timestamp();
            long timestampDelta = record.timestamp() - baseTimestamp;
            size += DefaultRecord.sizeInBytes(offsetDelta++, timestampDelta, record.key(), record.value(),
                    record.headers());
        }
        return size;
    }

    /**
     * Get an upper bound on the size of a batch with only a single record using a given key and value. This
     * is only an estimate because it does not take into account additional overhead from the compression
     * algorithm used.
     */
    static int estimateBatchSizeUpperBound(ByteBuffer key, ByteBuffer value, Header[] headers) {
        return RECORD_BATCH_OVERHEAD + DefaultRecord.recordSizeUpperBound(key, value, headers);
    }

    public static int incrementSequence(int sequence, int increment) {
        if (sequence > Integer.MAX_VALUE - increment)
            return increment - (Integer.MAX_VALUE - sequence) - 1;
        return sequence + increment;
    }

    public static int decrementSequence(int sequence, int decrement) {
        if (sequence < decrement)
            return Integer.MAX_VALUE - (decrement - sequence) + 1;
        return sequence - decrement;
    }

    // visible for testing
    abstract class RecordIterator implements CloseableIterator<Record> {
        private final Long logAppendTime;
        private final long baseOffset;
        private final long baseTimestamp;
        private final int baseSequence;
        private final int numRecords;
        private int readRecords = 0;

        RecordIterator() {
            this.logAppendTime = timestampType() == TimestampType.LOG_APPEND_TIME ? maxTimestamp() : null;
            this.baseOffset = baseOffset();
            this.baseTimestamp = baseTimestamp();
            this.baseSequence = baseSequence();
            int numRecords = count();
            if (numRecords < 0)
                throw new InvalidRecordException("Found invalid record count " + numRecords + " in magic v" +
                        magic() + " batch");
            this.numRecords = numRecords;
        }

        @Override
        public boolean hasNext() {
            return readRecords < numRecords;
        }

        @Override
        public Record next() {
            if (readRecords >= numRecords)
                throw new NoSuchElementException();

            readRecords++;
            Record rec = readNext(baseOffset, baseTimestamp, baseSequence, logAppendTime);
            if (readRecords == numRecords) {
                // Validate that the actual size of the batch is equal to declared size
                // by checking that after reading declared number of items, there no items left
                // (overflow case, i.e. reading past buffer end is checked elsewhere).
                if (!ensureNoneRemaining())
                    throw new InvalidRecordException("Incorrect declared batch size, records still remaining in file");
            }
            return rec;
        }

        protected abstract Record readNext(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime);

        protected abstract boolean ensureNoneRemaining();

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    // visible for testing
    abstract class StreamRecordIterator extends RecordIterator {
        private final InputStream inputStream;

        StreamRecordIterator(InputStream inputStream) {
            super();
            this.inputStream = inputStream;
        }

        abstract Record doReadRecord(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime) throws IOException;

        @Override
        protected Record readNext(long baseOffset, long baseTimestamp, int baseSequence, Long logAppendTime) {
            try {
                return doReadRecord(baseOffset, baseTimestamp, baseSequence, logAppendTime);
            } catch (IllegalArgumentException e) {
                throw new InvalidRecordException("Incorrect declared batch size, premature EOF reached", e);
            } catch (IOException e) {
                throw new KafkaException("Failed to decompress record stream", e);
            }
        }

        @Override
        protected boolean ensureNoneRemaining() {
            try {
                return inputStream.read() == -1;
            } catch (IOException e) {
                throw new KafkaException("Error checking for remaining bytes after reading batch", e);
            }
        }

        @Override
        public void close() {
            try {
                inputStream.close();
            } catch (IOException e) {
                throw new KafkaException("Failed to close record stream", e);
            }
        }
    }

    static class DefaultFileChannelRecordBatch extends FileLogInputStream.FileChannelRecordBatch {

        DefaultFileChannelRecordBatch(long offset,
                                      byte magic,
                                      FileRecords fileRecords,
                                      int position,
                                      int batchSize) {
            super(offset, magic, fileRecords, position, batchSize);
        }

        @Override
        protected RecordBatch toMemoryRecordBatch(ByteBuffer buffer) {
            return new DefaultRecordBatch(buffer);
        }

        @Override
        public long baseOffset() {
            return offset;
        }

        @Override
        public long lastOffset() {
            return loadBatchHeader().lastOffset();
        }

        @Override
        public long producerId() {
            return loadBatchHeader().producerId();
        }

        @Override
        public short producerEpoch() {
            return loadBatchHeader().producerEpoch();
        }

        @Override
        public int baseSequence() {
            return loadBatchHeader().baseSequence();
        }

        @Override
        public int lastSequence() {
            return loadBatchHeader().lastSequence();
        }

        @Override
        public Integer countOrNull() {
            return loadBatchHeader().countOrNull();
        }

        @Override
        public boolean isTransactional() {
            return loadBatchHeader().isTransactional();
        }

        @Override
        public OptionalLong deleteHorizonMs() {
            return loadBatchHeader().deleteHorizonMs();
        }

        @Override
        public boolean isControlBatch() {
            return loadBatchHeader().isControlBatch();
        }

        @Override
        public int partitionLeaderEpoch() {
            return loadBatchHeader().partitionLeaderEpoch();
        }

        @Override
        protected int headerSize() {
            return RECORD_BATCH_OVERHEAD;
        }
    }

}
