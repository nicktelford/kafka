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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatchInterface;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class TransactionalRocksDBStore extends TransactionalKeyValueStore<RocksDBStore, Bytes, byte[]> implements BatchWritingStore {
    public TransactionalRocksDBStore(final RocksDBStore wrapped) {
        super(wrapped);
    }

    public TransactionalRocksDBStore(final RocksDBStore wrapped, final Supplier<Thread> currentThread) {
        super(wrapped, currentThread);
    }

    void openDB(final Map<String, Object> configs, final File stateDir) {
        wrapped().openDB(configs, stateDir);
    }

    @Override
    public void addToBatch(final KeyValue<byte[], byte[]> record, final WriteBatchInterface batch) throws RocksDBException {
        ((BatchWritingStore) currentTransaction()).addToBatch(record, batch);
    }

    @Override
    public void addOffsetsToBatch(final Map<TopicPartition, Long> changelogOffsets, final WriteBatchInterface batch) throws RocksDBException {
        ((BatchWritingStore) currentTransaction()).addOffsetsToBatch(changelogOffsets, batch);
    }

    @Override
    public void write(final WriteBatchInterface batch) throws RocksDBException {
        ((BatchWritingStore) currentTransaction()).write(batch);
    }

    // the following methods all exist on both RocksDBStore and RocksDBTransaction, however, they do not share a common
    // superclass, therefore, we need to use reflection to determine which method to dispatch

    @SuppressWarnings("unchecked")
    synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                       final Bytes to,
                                                       final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        final KeyValueStore<Bytes, byte[]> current = currentTransaction();
        if (current instanceof RocksDBTransaction) {
            return ((RocksDBTransaction<RocksDBStore>) current).range(from, to, openIterators);
        } else {
            return ((RocksDBStore) current).range(from, to, openIterators);
        }
    }


    @SuppressWarnings("unchecked")
    <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix,
                                                                             final PS prefixKeySerializer,
                                                                             final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        final KeyValueStore<Bytes, byte[]> current = currentTransaction();
        if (current instanceof RocksDBTransaction) {
            return ((RocksDBTransaction<RocksDBStore>) current).prefixScan(prefix, prefixKeySerializer, openIterators);
        } else {
            return ((RocksDBStore) current).prefixScan(prefix, prefixKeySerializer, openIterators);
        }
    }

    @SuppressWarnings("unchecked")
    void deleteRange(final Bytes keyFrom, final Bytes keyTo) {
        final KeyValueStore<Bytes, byte[]> current = currentTransaction();
        if (current instanceof RocksDBTransaction) {
            ((RocksDBTransaction<RocksDBStore>) current).deleteRange(keyFrom, keyTo);
        } else {
            ((RocksDBStore) current).deleteRange(keyFrom, keyTo);
        }
    }
}
