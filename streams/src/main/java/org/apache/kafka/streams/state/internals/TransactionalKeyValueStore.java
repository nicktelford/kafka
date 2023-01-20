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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.function.Supplier;
import java.util.Map;

public class TransactionalKeyValueStore<S extends KeyValueStore<K, V>, K, V>
        extends WrappedStateStore<S, K, V> implements KeyValueStore<K, V> {
    KeyValueStore<K, V> currentTransaction;

    private final Supplier<Thread> currentThread;

    protected KeyValueStore<K, V> currentTransaction() {
        if (currentThread.get() instanceof StreamThread) {
            if (currentTransaction == null) {
                currentTransaction = newTransaction();
            }
            return currentTransaction;
        } else {
            return wrapped();
        }
    }

    public TransactionalKeyValueStore(final S wrapped) {
        this(wrapped, Thread::currentThread);
    }

    public TransactionalKeyValueStore(final S wrapped, final Supplier<Thread> currentThread) {
        super(wrapped);
        this.currentThread = currentThread;
    }

    @Override @SuppressWarnings("unchecked")
    public KeyValueStore<K, V> newTransaction() {
        final StateStore transaction = wrapped().newTransaction();
        if (transaction instanceof KeyValueStore) {
            return (KeyValueStore<K, V>) transaction;
        } else {
            throw new IllegalArgumentException("New Transactions created by " +
                    wrapped().getClass().getSimpleName() + " must implement KeyValueStore");
        }
    }

    @Override @SuppressWarnings("unchecked")
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        currentTransaction().commit(changelogOffsets);
        currentTransaction = null;
    }

    @Override
    public void close() {
        if (currentTransaction != null) currentTransaction.close();
        super.close();
    }

    @Override
    public V get(final K key) {
        return currentTransaction().get(key);
    }

    @Override
    public KeyValueIterator<K, V> range(final K from, final K to) {
        return currentTransaction().range(from, to);
    }

    @Override
    public KeyValueIterator<K, V> reverseRange(final K from, final K to) {
        return currentTransaction().reverseRange(from, to);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return currentTransaction().all();
    }

    @Override
    public KeyValueIterator<K, V> reverseAll() {
        return currentTransaction().reverseAll();
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, V> prefixScan(final P prefix,
                                                                           final PS prefixKeySerializer) {
        return currentTransaction().prefixScan(prefix, prefixKeySerializer);
    }

    @Override
    public void put(final K key, final V value) {
        currentTransaction().put(key, value);
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        return currentTransaction().putIfAbsent(key, value);
    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        currentTransaction().putAll(entries);
    }

    @Override
    public V delete(final K key) {
        return currentTransaction().delete(key);
    }

    @Override
    public long approximateNumEntries() {
        return currentTransaction().approximateNumEntries();
    }
}
