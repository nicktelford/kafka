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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.AbstractTransaction;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatchWithIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

class RocksDBTransaction<S extends RocksDBStore> extends AbstractTransaction<S> implements KeyValueStore<Bytes, byte[]> {

    static class BatchedDBAccessor implements RocksDBStore.DBAccessor {

        private final RocksDB db;
        private final WriteBatchWithIndex batch;
        private final ReadOptions rOptions;
        private long uncommittedBytes;

        BatchedDBAccessor(final RocksDB db, final WriteBatchWithIndex batch, final ReadOptions rOptions) {
            this.db = db;
            this.batch = batch;
            this.rOptions = rOptions;
        }

        @Override
        public byte[] get(final ColumnFamilyHandle columnFamily, final byte[] key) throws RocksDBException {
            return batch.getFromBatchAndDB(db, columnFamily, rOptions, key);
        }

        @Override
        public RocksIterator newIterator(final ColumnFamilyHandle columnFamily) {
            return batch.newIteratorWithBase(columnFamily, db.newIterator(columnFamily));
        }

        @Override
        public void put(final ColumnFamilyHandle columnFamily, final byte[] key, final byte[] value) throws RocksDBException {
            batch.put(columnFamily, key, value);
            uncommittedBytes += key.length + value.length;
        }

        @Override
        public void delete(final ColumnFamilyHandle columnFamily, final byte[] key) throws RocksDBException {
            batch.delete(columnFamily, key);
            uncommittedBytes += key.length;
        }

        @Override
        public void deleteRange(final ColumnFamilyHandle columnFamily, final byte[] from, final byte[] to) throws RocksDBException {
            batch.deleteRange(columnFamily, from, to);
            uncommittedBytes += from.length + to.length;
        }

        @Override
        public long approximateNumEntries(final ColumnFamilyHandle columnFamily) throws RocksDBException {
            return batch.count() + db.getLongProperty(columnFamily, "rocksdb.estimate-num-keys");
        }

        @Override
        public long approximateNumUncommittedBytes() {
            return uncommittedBytes;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RocksDBTransaction.class);
    protected final RocksDBStore.ColumnFamilyAccessor cf;
    protected BatchedDBAccessor accessor;
    protected final Position position;
    private final WriteBatchWithIndex batch = new WriteBatchWithIndex(true);
    final Set<KeyValueIterator<Bytes, byte[]>> openIterators = new HashSet<>();

    RocksDBTransaction(final RocksDBStore.ColumnFamilyAccessor cf, final Position position) {
        this.cf = cf;
        this.position = position;
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        super.init(context, root);
        this.accessor = new BatchedDBAccessor(store.db, batch, store.rOptions);
    }

    @Override
    public StateStore newTransaction() {
        if (isOpen || !batch.isOwningHandle()) return super.newTransaction();
        isOpen = true;
        return this;
    }

    @Override
    public byte[] get(final Bytes key) {
        validateIsOpen();
        try {
            return cf.get(accessor, key.get());
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while getting value for key from store " + store.name, e);
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        return range(from, to, true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        return range(from, to, false);
    }

    KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to, final boolean forward) {
        validateIsOpen();
        if (Objects.nonNull(from) && Objects.nonNull(to) && from.compareTo(to) > 0) {
            log.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                    + "This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }
        final ManagedKeyValueIterator<Bytes, byte[]> rocksDBRangeIterator = cf.range(accessor, from, to, forward);
        openIterators.add(rocksDBRangeIterator);
        rocksDBRangeIterator.onClose(() -> openIterators.remove(rocksDBRangeIterator));

        return rocksDBRangeIterator;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return all(true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        return all(false);
    }

    KeyValueIterator<Bytes, byte[]> all(final boolean forward) {
        validateIsOpen();
        final ManagedKeyValueIterator<Bytes, byte[]> rocksDbIterator = cf.all(accessor, forward);
        openIterators.add(rocksDbIterator);
        rocksDbIterator.onClose(() -> openIterators.remove(rocksDbIterator));
        return rocksDbIterator;
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix,
                                                                                    final PS prefixKeySerializer) {
        validateIsOpen();
        Objects.requireNonNull(prefix, "prefix cannot be null");
        Objects.requireNonNull(prefixKeySerializer, "prefixKeySerializer cannot be null");
        final Bytes prefixBytes = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));

        final ManagedKeyValueIterator<Bytes, byte[]> rocksDbPrefixSeekIterator = cf.prefixScan(accessor, prefixBytes);
        openIterators.add(rocksDbPrefixSeekIterator);
        rocksDbPrefixSeekIterator.onClose(() -> openIterators.remove(rocksDbPrefixSeekIterator));

        return rocksDbPrefixSeekIterator;
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        validateIsOpen();
        Objects.requireNonNull(key, "key cannot be null");
        cf.put(accessor, key.get(), value);
        StoreQueryUtils.updatePosition(position, context);
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        final byte[] originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        try {
            cf.prepareBatch(entries, batch);
            StoreQueryUtils.updatePosition(position, context);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while batch writing to store " + store.name, e);
        }
    }

    @Override
    public byte[] delete(final Bytes key) {
        validateIsOpen();
        Objects.requireNonNull(key, "key cannot be null");
        final byte[] oldValue;
        try {
            oldValue = cf.getOnly(accessor, key.get());
        } catch (final RocksDBException e) {
            // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new ProcessorStateException("Error while getting value for key from store " + store.name, e);
        }
        put(key, null);
        return oldValue;
    }

    @Override
    public long approximateNumEntries() {
        validateIsOpen();
        try {
            return cf.approximateNumEntries(accessor);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while approximating number of entries in store " + store.name, e);
        }
    }

    @Override
    public long approximateNumUncommittedBytes() {
        return accessor.approximateNumUncommittedBytes();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound, final QueryConfig config) {
        return StoreQueryUtils.handleBasicQueries(
                query,
                positionBound,
                config,
                this,
                position,
                context
        );
    }

    @Override
    public Position getPosition() {
        return position;
    }

    @Override
    public void commitTransaction() {
        try {
            // write records from batch and flush column-families
            store.write(batch);
            cf.flush();

            // update Store Position by merging this Transactions' Position in to it
            store.getPosition().merge(position);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while executing flush from store " + store.name, e);
        } finally {
            closeOpenIterators();
            batch.clear();
            accessor.uncommittedBytes = 0;
        }
    }

    @Override
    public void closeTransaction() {
        try {
            closeOpenIterators();
        } finally {
            batch.close();
        }
    }

    private void closeOpenIterators() {
        final HashSet<KeyValueIterator<Bytes, byte[]>> iterators;
        synchronized (openIterators) {
            iterators = new HashSet<>(openIterators);
        }
        if (iterators.size() != 0) {
            log.warn("Closing {} open iterators for transaction on store {}", iterators.size(), store.name);
            for (final KeyValueIterator<Bytes, byte[]> iterator : iterators) {
                iterator.close();
            }
        }
    }
}
