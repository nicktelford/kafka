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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatchInterface;

import java.io.IOException;
import java.util.Objects;

class TimestampedSegment extends RocksDBTimestampedStore implements Comparable<TimestampedSegment>, Segment {
    static class Transaction extends RocksDBTransaction<TimestampedSegment>
            implements Comparable<TimestampedSegment.Transaction>, Segment {

        Transaction(final ColumnFamilyAccessor cf, final Position position) {
            super(cf, position);
        }

        @Override
        public int compareTo(final Transaction other) {
            return store.compareTo(other.store);
        }

        @Override
        public void deleteRange(final Bytes keyFrom, final Bytes keyTo) {
            cf.deleteRange(accessor, keyFrom.get(), Bytes.increment(keyTo).get());
        }

        @Override
        public void addToBatch(final KeyValue<byte[], byte[]> record, final WriteBatchInterface batch)
                throws RocksDBException {
            cf.addToBatch(record.key, record.value, batch);
        }

        @Override
        public void write(final WriteBatchInterface batch) throws RocksDBException {
            // non-transactional
            store.write(batch);
        }

        @Override
        public void destroy() throws IOException {
            // non-transactional, called after close()
            store.destroy();
        }
    }

    public final long id;

    TimestampedSegment(final String segmentName,
                       final String windowName,
                       final long id,
                       final RocksDBMetricsRecorder metricsRecorder) {
        super(segmentName, windowName, metricsRecorder);
        this.id = id;
    }

    @Override
    public void destroy() throws IOException {
        Utils.delete(dbDir);
    }

    @Override
    public void deleteRange(final Bytes keyFrom, final Bytes keyTo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(final TimestampedSegment segment) {
        return Long.compare(id, segment.id);
    }

    @Override
    public String toString() {
        return "TimestampedSegment(id=" + id + ", name=" + name() + ")";
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final TimestampedSegment segment = (TimestampedSegment) obj;
        return id == segment.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public RocksDBTransaction<? extends RocksDBStore> createTransaction() {
        return new Transaction(cf, position);
    }
}
