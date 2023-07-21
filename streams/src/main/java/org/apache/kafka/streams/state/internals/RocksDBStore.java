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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchInterface;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;
import static org.apache.kafka.streams.StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.getMetricsImpl;

/**
 * A persistent key-value store based on RocksDB.
 */
public class RocksDBStore implements KeyValueStore<Bytes, byte[]>, BatchWritingStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBStore.class);

    private static final CompressionType COMPRESSION_TYPE = CompressionType.NO_COMPRESSION;
    private static final CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
    private static final long WRITE_BUFFER_SIZE = 16 * 1024 * 1024L;
    private static final long BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
    private static final long BLOCK_SIZE = 4096L;
    private static final int MAX_WRITE_BUFFERS = 3;

    protected static final byte[] OFFSETS_COLUMN_FAMILY_NAME = "__offsets".getBytes(StandardCharsets.UTF_8);
    private static final StringSerializer TOPIC_SERIALIZER = new StringSerializer();
    private static final LongSerializer OFFSET_SERIALIZER = new LongSerializer();
    private static final LongDeserializer OFFSET_DESERIALIZER = new LongDeserializer();

    static final String DB_FILE_DIR = "rocksdb";

    protected StateStoreContext context;
    final String name;
    private final String parentDir;
    final Set<KeyValueIterator<Bytes, byte[]>> openIterators = Collections.synchronizedSet(new HashSet<>());
    private boolean consistencyEnabled = false;

    // VisibleForTesting
    protected File dbDir;
    RocksDB db;
    DBAccessor accessor;
    ColumnFamilyHandle offsetsCF;
    ColumnFamilyAccessor cf;

    // the following option objects will be created in openDB and closed in the close() method
    private RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter userSpecifiedOptions;
    WriteOptions wOptions;
    FlushOptions fOptions;
    ReadOptions rOptions;
    private Cache cache;
    private BloomFilter filter;
    private Statistics statistics;

    private RocksDBConfigSetter configSetter;
    private boolean userSpecifiedStatistics = false;

    private final RocksDBMetricsRecorder metricsRecorder;
    // if true, then open iterators (for range, prefix scan, and other operations) will be
    // managed automatically (by this store instance). if false, then these iterators must be
    // managed elsewhere (by the caller of those methods).
    private final boolean autoManagedIterators;

    protected volatile boolean open = false;
    protected Position position;

    public RocksDBStore(final String name,
                        final String metricsScope) {
        this(name, DB_FILE_DIR, new RocksDBMetricsRecorder(metricsScope, name));
    }

    RocksDBStore(final String name,
                 final String parentDir,
                 final RocksDBMetricsRecorder metricsRecorder) {
        this(name, parentDir, metricsRecorder, true);
    }

    RocksDBStore(final String name,
                 final String parentDir,
                 final RocksDBMetricsRecorder metricsRecorder,
                 final boolean autoManagedIterators) {
        this.name = name;
        this.parentDir = parentDir;
        this.metricsRecorder = metricsRecorder;
        this.autoManagedIterators = autoManagedIterators;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        if (context instanceof StateStoreContext) {
            init((StateStoreContext) context, root);
        } else {
            throw new UnsupportedOperationException(
                    "Use init(StateStoreContext, StateStore) instead."
            );
        }
    }

    @Override
    public void init(final StateStoreContext context,
                     final StateStore root) {
        // open the DB dir
        this.context = context;
        metricsRecorder.init(getMetricsImpl(context), context.taskId());
        openDB(context.appConfigs(), context.stateDir());

        final File positionCheckpointFile = new File(context.stateDir(), name() + ".position");
        final OffsetCheckpoint positionCheckpoint = new OffsetCheckpoint(positionCheckpointFile);
        this.position = StoreQueryUtils.readPositionFromCheckpoint(positionCheckpoint);

        // value getter should always read directly from rocksDB
        // since it is only for values that are already flushed
        context.register(
            root,
            (RecordBatchingStateRestoreCallback) this::restoreBatch,
            () -> StoreQueryUtils.checkpointPosition(positionCheckpoint, position)
        );
        consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
            context.appConfigs(),
            IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
            false);
    }

    @SuppressWarnings("unchecked")
    void openDB(final Map<String, Object> configs, final File stateDir) {
        // initialize the default rocksdb options

        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        userSpecifiedOptions = new RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(dbOptions, columnFamilyOptions);

        final BlockBasedTableConfigWithAccessibleCache tableConfig = new BlockBasedTableConfigWithAccessibleCache();
        cache = new LRUCache(BLOCK_CACHE_SIZE);
        tableConfig.setBlockCache(cache);
        tableConfig.setBlockSize(BLOCK_SIZE);

        filter = new BloomFilter();
        tableConfig.setFilterPolicy(filter);

        userSpecifiedOptions.optimizeFiltersForHits();
        userSpecifiedOptions.setTableFormatConfig(tableConfig);
        userSpecifiedOptions.setWriteBufferSize(WRITE_BUFFER_SIZE);
        userSpecifiedOptions.setCompressionType(COMPRESSION_TYPE);
        userSpecifiedOptions.setCompactionStyle(COMPACTION_STYLE);
        userSpecifiedOptions.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
        userSpecifiedOptions.setCreateIfMissing(true);
        userSpecifiedOptions.setErrorIfExists(false);
        userSpecifiedOptions.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
        // this is the recommended way to increase parallelism in RocksDb
        // note that the current implementation of setIncreaseParallelism affects the number
        // of compaction threads but not flush threads (the latter remains one). Also,
        // the parallelism value needs to be at least two because of the code in
        // https://github.com/facebook/rocksdb/blob/62ad0a9b19f0be4cefa70b6b32876e764b7f3c11/util/options.cc#L580
        // subtracts one from the value passed to determine the number of compaction threads
        // (this could be a bug in the RocksDB code and their devs have been contacted).
        userSpecifiedOptions.setIncreaseParallelism(Math.max(Runtime.getRuntime().availableProcessors(), 2));

        wOptions = new WriteOptions();
        wOptions.setDisableWAL(true);

        fOptions = new FlushOptions();
        fOptions.setWaitForFlush(true);

        rOptions = new ReadOptions();

        final Class<RocksDBConfigSetter> configSetterClass =
                (Class<RocksDBConfigSetter>) configs.get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG);

        if (configSetterClass != null) {
            configSetter = Utils.newInstance(configSetterClass);
            configSetter.setConfig(name, userSpecifiedOptions, configs);
        }

        // always enable atomic flush, to guarantee atomicity of data and offsets
        userSpecifiedOptions.setAtomicFlush(true);

        dbDir = new File(new File(stateDir, parentDir), name);
        try {
            Files.createDirectories(dbDir.getParentFile().toPath());
            Files.createDirectories(dbDir.getAbsoluteFile().toPath());
        } catch (final IOException fatal) {
            throw new ProcessorStateException(fatal);
        }

        // Setup statistics before the database is opened, otherwise the statistics are not updated
        // with the measurements from Rocks DB
        setupStatistics(configs, dbOptions);
        // todo: provide separate offsetsColumnFamilyOptions that's optimized for offset storage
        openRocksDB(dbOptions, columnFamilyOptions, columnFamilyOptions);
        open = true;

        addValueProvidersToMetricsRecorder();
    }

    private void setupStatistics(final Map<String, Object> configs, final DBOptions dbOptions) {
        statistics = userSpecifiedOptions.statistics();
        if (statistics == null) {
            if (RecordingLevel.forName((String) configs.get(METRICS_RECORDING_LEVEL_CONFIG)) == RecordingLevel.DEBUG) {
                statistics = new Statistics();
                dbOptions.setStatistics(statistics);
            }
            userSpecifiedStatistics = false;
        } else {
            userSpecifiedStatistics = true;
        }
    }

    private void addValueProvidersToMetricsRecorder() {
        final TableFormatConfig tableFormatConfig = userSpecifiedOptions.tableFormatConfig();
        if (tableFormatConfig instanceof BlockBasedTableConfigWithAccessibleCache) {
            final Cache cache = ((BlockBasedTableConfigWithAccessibleCache) tableFormatConfig).blockCache();
            metricsRecorder.addValueProviders(name, db, cache, userSpecifiedStatistics ? null : statistics);
        } else if (tableFormatConfig instanceof BlockBasedTableConfig) {
            throw new ProcessorStateException("The used block-based table format configuration does not expose the " +
                    "block cache. Use the BlockBasedTableConfig instance provided by Options#tableFormatConfig() to configure " +
                    "the block-based table format of RocksDB. Do not provide a new instance of BlockBasedTableConfig to " +
                    "the RocksDB options.");
        } else {
            metricsRecorder.addValueProviders(name, db, null, userSpecifiedStatistics ? null : statistics);
        }
    }

    void openRocksDB(final DBOptions dbOptions,
                     final ColumnFamilyOptions columnFamilyOptions,
                     final ColumnFamilyOptions offsetsColumnFamilyOptions) {
        final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
                dbOptions,
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor(OFFSETS_COLUMN_FAMILY_NAME, offsetsColumnFamilyOptions)
        );

        cf = new SingleColumnFamilyAccessor(columnFamilies.get(0));
        if (context.isolationLevel() == IsolationLevel.READ_COMMITTED) {
            accessor = new BatchedDBAccessor(db, columnFamilies.get(1), rOptions, wOptions, false);
        } else {
            accessor = new DirectDBAccessor(db, columnFamilies.get(1), wOptions);
        }
        offsetsCF = columnFamilies.get(1);
    }

    List<ColumnFamilyHandle> openRocksDB(final DBOptions dbOptions,
                                         final ColumnFamilyDescriptor defaultColumnFamilyDescriptor,
                                         final ColumnFamilyDescriptor... columnFamilyDescriptors) {
        final String absolutePath = dbDir.getAbsolutePath();
        final List<ColumnFamilyDescriptor> extraDescriptors = Arrays.asList(columnFamilyDescriptors);
        final List<ColumnFamilyDescriptor> allDescriptors = new ArrayList<>(1 + columnFamilyDescriptors.length);
        allDescriptors.add(defaultColumnFamilyDescriptor);
        allDescriptors.addAll(extraDescriptors);

        try {
            final Options options = new Options(dbOptions, defaultColumnFamilyDescriptor.getOptions());
            final List<byte[]> allExisting = RocksDB.listColumnFamilies(options, absolutePath);

            final List<ColumnFamilyDescriptor> existingDescriptors = allDescriptors.stream()
                    .filter(descriptor -> descriptor == defaultColumnFamilyDescriptor || allExisting.stream().anyMatch(existing -> Arrays.equals(existing, descriptor.getName())))
                    .collect(Collectors.toList());
            final List<ColumnFamilyDescriptor> toCreate = extraDescriptors.stream()
                    .filter(descriptor -> allExisting.stream().noneMatch(existing -> Arrays.equals(existing, descriptor.getName())))
                    .collect(Collectors.toList());
            final List<ColumnFamilyHandle> existingColumnFamilies = new ArrayList<>(existingDescriptors.size());
            db = RocksDB.open(dbOptions, absolutePath, existingDescriptors, existingColumnFamilies);
            final List<ColumnFamilyHandle> createdColumnFamilies = db.createColumnFamilies(toCreate);

            // match up the existing and created ColumnFamilyHandles with the existing/created ColumnFamilyDescriptors
            // so that the order of the resultant List matches the order of the openRocksDB arguments
            final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(allDescriptors.size());
            for (int i = 0, j = 0, k = 0; i < allDescriptors.size(); i++) {
                if (j < existingDescriptors.size() && (existingDescriptors.get(j) == allDescriptors.get(i))) {
                    columnFamilies.add(existingColumnFamilies.get(j));
                    j++;
                } else if (k < toCreate.size() && (toCreate.get(k) == allDescriptors.get(i))) {
                    columnFamilies.add(createdColumnFamilies.get(k));
                    k++;
                }
            }
            return columnFamilies;
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.toString(), e);
        }
    }


    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    private void validateStoreOpen() {
        if (!open) {
            throw new InvalidStateStoreException("Store " + name + " is currently closed");
        }
    }

    @Override
    public synchronized void put(final Bytes key,
                                 final byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        cf.put(accessor, key.get(), value);

        StoreQueryUtils.updatePosition(position, context);
    }

    @Override
    public synchronized byte[] putIfAbsent(final Bytes key,
                                           final byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        final byte[] originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        try (final WriteBatch batch = new WriteBatch()) {
            cf.prepareBatch(entries, batch);
            write(batch);
            StoreQueryUtils.updatePosition(position, context);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while batch writing to store " + name, e);
        }
    }

    @Override
    public <R> QueryResult<R> query(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config) {

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
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix,
                                                                                    final PS prefixKeySerializer) {
        if (!autoManagedIterators) {
            throw new IllegalStateException("Must specify openIterators in call to prefixScan()");
        }
        return doPrefixScan(prefix, prefixKeySerializer, openIterators);
    }

    <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix,
                                                                             final PS prefixKeySerializer,
                                                                             final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        if (autoManagedIterators) {
            throw new IllegalStateException("Cannot specify openIterators when using auto-managed iterators");
        }
        return doPrefixScan(prefix, prefixKeySerializer, openIterators);
    }

    <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> doPrefixScan(final P prefix,
                                                                               final PS prefixKeySerializer,
                                                                               final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        validateStoreOpen();
        Objects.requireNonNull(prefix, "prefix cannot be null");
        Objects.requireNonNull(prefixKeySerializer, "prefixKeySerializer cannot be null");
        final Bytes prefixBytes = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));

        final ManagedKeyValueIterator<Bytes, byte[]> rocksDbPrefixSeekIterator = cf.prefixScan(accessor, prefixBytes);
        openIterators.add(rocksDbPrefixSeekIterator);
        rocksDbPrefixSeekIterator.onClose(() -> openIterators.remove(rocksDbPrefixSeekIterator));
        accessor.maybeRegisterTransactionIterator(rocksDbPrefixSeekIterator);

        return rocksDbPrefixSeekIterator;
    }

    @Override
    public synchronized byte[] get(final Bytes key) {
        validateStoreOpen();
        try {
            return cf.get(accessor, key.get());
        } catch (final RocksDBException e) {
            // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new ProcessorStateException("Error while getting value for key from store " + name, e);
        }
    }

    @Override
    public synchronized byte[] delete(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        final byte[] oldValue;
        try {
            oldValue = cf.getOnly(accessor, key.get());
        } catch (final RocksDBException e) {
            // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new ProcessorStateException("Error while getting value for key from store " + name, e);
        }
        put(key, null);
        return oldValue;
    }

    void deleteRange(final Bytes keyFrom, final Bytes keyTo) {
        Objects.requireNonNull(keyFrom, "keyFrom cannot be null");
        Objects.requireNonNull(keyTo, "keyTo cannot be null");

        validateStoreOpen();

        // End of key is exclusive, so we increment it by 1 byte to make keyTo inclusive.
        // RocksDB's deleteRange() does not support a null upper bound so in the event
        // of overflow from increment(), the operation cannot be performed and an
        // IndexOutOfBoundsException will be thrown.
        cf.deleteRange(accessor, keyFrom.get(), Bytes.increment(keyTo).get());
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                              final Bytes to) {
        if (!autoManagedIterators) {
            throw new IllegalStateException("Must specify openIterators in call to range()");
        }
        return range(from, to, true, openIterators);
    }

    synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                       final Bytes to,
                                                       final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        if (autoManagedIterators) {
            throw new IllegalStateException("Cannot specify openIterators when using auto-managed iterators");
        }
        return range(from, to, true, openIterators);
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from,
                                                                     final Bytes to) {
        if (!autoManagedIterators) {
            throw new IllegalStateException("Must specify openIterators in call to reverseRange()");
        }
        return range(from, to, false, openIterators);
    }

    synchronized KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from,
                                                              final Bytes to,
                                                              final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        if (autoManagedIterators) {
            throw new IllegalStateException("Cannot specify openIterators when using auto-managed iterators");
        }
        return range(from, to, false, openIterators);
    }

    private KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                  final Bytes to,
                                                  final boolean forward,
                                                  final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        if (Objects.nonNull(from) && Objects.nonNull(to) && from.compareTo(to) > 0) {
            log.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                    + "This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();

        final ManagedKeyValueIterator<Bytes, byte[]> rocksDBRangeIterator = cf.range(accessor, from, to, forward);
        openIterators.add(rocksDBRangeIterator);
        rocksDBRangeIterator.onClose(() -> openIterators.remove(rocksDBRangeIterator));
        accessor.maybeRegisterTransactionIterator(rocksDBRangeIterator);

        return rocksDBRangeIterator;
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> all() {
        if (!autoManagedIterators) {
            throw new IllegalStateException("Must specify openIterators in call to all()");
        }
        return all(true, openIterators);
    }

    synchronized KeyValueIterator<Bytes, byte[]> all(final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        if (autoManagedIterators) {
            throw new IllegalStateException("Cannot specify openIterators when using auto-managed iterators");
        }
        return all(true, openIterators);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        if (!autoManagedIterators) {
            throw new IllegalStateException("Must specify openIterators in call to reverseAll()");
        }
        return all(false, openIterators);
    }

    KeyValueIterator<Bytes, byte[]> reverseAll(final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        if (autoManagedIterators) {
            throw new IllegalStateException("Cannot specify openIterators when using auto-managed iterators");
        }
        return all(false, openIterators);
    }

    private KeyValueIterator<Bytes, byte[]> all(final boolean forward,
                                                final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
        validateStoreOpen();
        final ManagedKeyValueIterator<Bytes, byte[]> rocksDbIterator = cf.all(accessor, forward);
        openIterators.add(rocksDbIterator);
        rocksDbIterator.onClose(() -> openIterators.remove(rocksDbIterator));
        accessor.maybeRegisterTransactionIterator(rocksDbIterator);
        return rocksDbIterator;
    }

    /**
     * Return an approximate count of key-value mappings in this store.
     *
     * <code>RocksDB</code> cannot return an exact entry count without doing a
     * full scan, so this method relies on the <code>rocksdb.estimate-num-keys</code>
     * property to get an approximate count. The returned size also includes
     * a count of dirty keys in the store's in-memory cache, which may lead to some
     * double-counting of entries and inflate the estimate.
     *
     * @return an approximate count of key-value mappings in the store.
     */
    @Override
    public long approximateNumEntries() {
        validateStoreOpen();
        final long numEntries;
        try {
            numEntries = cf.approximateNumEntries(accessor);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error fetching property from store " + name, e);
        }
        if (isOverflowing(numEntries)) {
            return Long.MAX_VALUE;
        }
        return numEntries;
    }

    private boolean isOverflowing(final long value) {
        // RocksDB returns an unsigned 8-byte integer, which could overflow long
        // and manifest as a negative value.
        return value < 0;
    }

    @Override
    public boolean managesOffsets() {
        return true;
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        if (db == null) {
            return;
        }
        try {
            accessor.commit(changelogOffsets);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while executing commit from store " + name, e);
        } finally {
            accessor.reset();
        }
    }

    @Override
    public Long getCommittedOffset(final TopicPartition partition) {
        final byte[] serializedTopic = TOPIC_SERIALIZER.serialize(null, partition.topic());
        final ByteBuffer tp = ByteBuffer.allocate(serializedTopic.length + Integer.BYTES);
        tp.putInt(partition.partition()).put(serializedTopic);
        try {
            return OFFSET_DESERIALIZER.deserialize(null, db.get(offsetsCF, tp.array()));
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while getting committed offset for partition " + partition + " from store " + name, e);
        }
    }

    @Override
    public void addToBatch(final KeyValue<byte[], byte[]> record,
                           final WriteBatchInterface batch) throws RocksDBException {
        cf.addToBatch(record.key, record.value, batch);
    }

    @Override
    public void write(final WriteBatchInterface batch) throws RocksDBException {
        if (batch instanceof WriteBatch) {
            db.write(wOptions, (WriteBatch) batch);
        } else if (batch instanceof WriteBatchWithIndex) {
            db.write(wOptions, (WriteBatchWithIndex) batch);
        } else {
            throw new ProcessorStateException("Unknown type of batch " + batch.getClass().getCanonicalName());
        }
    }

    @Override
    public synchronized void close() {
        if (!open) {
            return;
        }

        open = false;
        closeOpenIterators();
        accessor.close();

        if (configSetter != null) {
            configSetter.close(name, userSpecifiedOptions);
            configSetter = null;
        }

        metricsRecorder.removeValueProviders(name);

        // Important: do not rearrange the order in which the below objects are closed!
        // Order of closing must follow: ColumnFamilyHandle > RocksDB > DBOptions > ColumnFamilyOptions
        offsetsCF.close();
        cf.close();
        db.close();
        userSpecifiedOptions.close();
        wOptions.close();
        fOptions.close();
        filter.close();
        cache.close();
        if (statistics != null) {
            statistics.close();
        }

        cf = null;
        userSpecifiedOptions = null;
        wOptions = null;
        fOptions = null;
        db = null;
        filter = null;
        cache = null;
        statistics = null;
    }

    private void closeOpenIterators() {
        final HashSet<KeyValueIterator<Bytes, byte[]>> iterators;
        synchronized (openIterators) {
            iterators = new HashSet<>(openIterators);
        }
        if (iterators.size() != 0) {
            log.warn("Closing {} open iterators for store {}", iterators.size(), name);
            for (final KeyValueIterator<Bytes, byte[]> iterator : iterators) {
                iterator.close();
            }
        }
    }

    interface DBAccessor {
        byte[] get(final ColumnFamilyHandle columnFamily, final byte[] key) throws RocksDBException;
        RocksIterator newIterator(final ColumnFamilyHandle columnFamily);
        void put(final ColumnFamilyHandle columnFamily, final byte[] key, final byte[] value) throws RocksDBException;
        void delete(final ColumnFamilyHandle columnFamily, final byte[] key) throws RocksDBException;
        void deleteRange(final ColumnFamilyHandle columnFamily, final byte[] from, final byte[] to) throws RocksDBException;
        long approximateNumEntries(final ColumnFamilyHandle columnFamily) throws RocksDBException;
        long approximateNumUncommittedBytes();
        void writeOffset(final TopicPartition topicPartition, final Long offset, final WriteBatchInterface batch) throws RocksDBException;
        void commit(final Map<TopicPartition, Long> changelogOffsets) throws RocksDBException;
        void reset();
        void close();

        void maybeRegisterTransactionIterator(final ManagedKeyValueIterator<Bytes, byte[]> iterator);
    }

    static class DirectDBAccessor implements DBAccessor {

        private final RocksDB db;
        private final ColumnFamilyHandle offsetsCF;
        private final WriteOptions wOptions;

        DirectDBAccessor(final RocksDB db, final ColumnFamilyHandle offsetsCF, final WriteOptions wOptions) {
            this.db = db;
            this.offsetsCF = offsetsCF;
            this.wOptions = wOptions;
        }

        @Override
        public byte[] get(final ColumnFamilyHandle columnFamily, final byte[] key) throws RocksDBException {
            return db.get(columnFamily, key);
        }

        @Override
        public RocksIterator newIterator(final ColumnFamilyHandle columnFamily) {
            return db.newIterator(columnFamily);
        }

        @Override
        public void put(final ColumnFamilyHandle columnFamily, final byte[] key, final byte[] value) throws RocksDBException {
            db.put(columnFamily, key, value);
        }

        @Override
        public void delete(final ColumnFamilyHandle columnFamily, final byte[] key) throws RocksDBException {
            db.delete(columnFamily, key);
        }

        @Override
        public void deleteRange(final ColumnFamilyHandle columnFamily, final byte[] from, final byte[] to) throws RocksDBException {
            db.deleteRange(columnFamily, from, to);
        }

        @Override
        public long approximateNumEntries(final ColumnFamilyHandle columnFamily) throws RocksDBException {
            return db.getLongProperty(columnFamily, "rocksdb.estimate-num-keys");
        }

        @Override
        public long approximateNumUncommittedBytes() {
            return 0;
        }

        @Override
        public void commit(final Map<TopicPartition, Long> changelogOffsets) throws RocksDBException {
            try (final WriteBatch batch = new WriteBatch()) {
                for (final Map.Entry<TopicPartition, Long> e : changelogOffsets.entrySet()) {
                    writeOffset(e.getKey(), e.getValue(), batch);
                }
                db.write(wOptions, batch);
            }
        }

        @Override
        public void reset() {
            // no state to reset
        }

        @Override
        public void close() {
            // nothing to close
        }

        @Override
        public void maybeRegisterTransactionIterator(final ManagedKeyValueIterator<Bytes, byte[]> iterator) {
            // never register iterators as transaction iterator, because we have no transaction
        }

        @Override
        public void writeOffset(final TopicPartition topicPartition, final Long offset, final WriteBatchInterface batch) throws RocksDBException {
            final byte[] serializedTopic = TOPIC_SERIALIZER.serialize(null, topicPartition.topic());
            final ByteBuffer tp = ByteBuffer.allocate(serializedTopic.length + Integer.BYTES);
            tp.putInt(topicPartition.partition()).put(serializedTopic);

            if (offset == null) {
                batch.delete(offsetsCF, tp.array());
            } else {
                final byte[] serializedOffset = OFFSET_SERIALIZER.serialize(null, offset);
                batch.put(offsetsCF, tp.array(), serializedOffset);
            }
        }
    }

    static class BatchedDBAccessor implements RocksDBStore.DBAccessor {

        private final WriteBatchWithIndex batch = new WriteBatchWithIndex(true);

        private final RocksDB db;
        private final ColumnFamilyHandle offsetsCF;
        private final ReadOptions rOptions;
        private final WriteOptions wOptions;
        private long uncommittedBytes;

        // used to simulate calls from StreamThreads in tests
        boolean isStreamThreadForTest;

        private Set<KeyValueIterator<Bytes, byte[]>> openTransactionIterators = new HashSet<>();

        BatchedDBAccessor(final RocksDB db,
                          final ColumnFamilyHandle offsetsCF,
                          final ReadOptions rOptions,
                          final WriteOptions wOptions,
                          final boolean isStreamThreadForTest) {
            this.db = db;
            this.offsetsCF = offsetsCF;
            this.rOptions = rOptions;
            this.wOptions = wOptions;
            this.isStreamThreadForTest = isStreamThreadForTest;
        }

        @Override
        public byte[] get(final ColumnFamilyHandle columnFamily, final byte[] key) throws RocksDBException {
            if (Thread.currentThread() instanceof StreamThread || isStreamThreadForTest) {
                return batch.getFromBatchAndDB(db, columnFamily, rOptions, key);
            } else {
                return db.get(columnFamily, rOptions, key);
            }
        }

        @Override
        public RocksIterator newIterator(final ColumnFamilyHandle columnFamily) {
            if (Thread.currentThread() instanceof StreamThread || isStreamThreadForTest) {
                return batch.newIteratorWithBase(columnFamily, db.newIterator(columnFamily));
            } else {
                return db.newIterator(columnFamily, rOptions);
            }
        }

        @Override
        public void maybeRegisterTransactionIterator(final ManagedKeyValueIterator<Bytes, byte[]> iterator) {
            if (Thread.currentThread() instanceof StreamThread || isStreamThreadForTest) {
                openTransactionIterators.add(iterator);
                iterator.onClose(() -> openTransactionIterators.remove(iterator));
            } else {
                // nothing to register, because this iterator doesn't iterate the transaction buffer
            }
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
            if (Thread.currentThread() instanceof StreamThread || isStreamThreadForTest) {
                return batch.count() + db.getLongProperty(columnFamily, "rocksdb.estimate-num-keys");
            } else {
                return db.getLongProperty(columnFamily, "rocksdb.estimate-num-keys");
            }
        }

        @Override
        public long approximateNumUncommittedBytes() {
            if (Thread.currentThread() instanceof StreamThread || isStreamThreadForTest) {
                return uncommittedBytes;
            } else {
                return 0;
            }
        }

        @Override
        public void commit(final Map<TopicPartition, Long> changelogOffsets) throws RocksDBException {
            // add offsets to batch
            for (final Map.Entry<TopicPartition, Long> e : changelogOffsets.entrySet()) {
                writeOffset(e.getKey(), e.getValue(), batch);
            }
            db.write(wOptions, batch);
        }

        @Override
        public void reset() {
            for (final KeyValueIterator<Bytes, byte[]> iterator : openTransactionIterators) {
                iterator.close();
            }
            batch.clear();
            uncommittedBytes = 0;
        }

        @Override
        public void close() {
            for (final KeyValueIterator<Bytes, byte[]> iterator : openTransactionIterators) {
                iterator.close();
            }
            batch.close();
            uncommittedBytes = 0;
        }

        @Override
        public void writeOffset(final TopicPartition partition, final Long offset, final WriteBatchInterface batch) throws RocksDBException {
            final byte[] serializedTopic = TOPIC_SERIALIZER.serialize(null, partition.topic());
            final ByteBuffer tp = ByteBuffer.allocate(serializedTopic.length + Integer.BYTES);
            tp.putInt(partition.partition()).put(serializedTopic);
            if (offset == null) {
                batch.delete(offsetsCF, tp.array());
            } else {
                final byte[] serializedOffset = OFFSET_SERIALIZER.serialize(null, offset);
                batch.put(offsetsCF, tp.array(), serializedOffset);
            }
        }
    }


    interface ColumnFamilyAccessor {

        void put(final DBAccessor accessor, final byte[] key, final byte[] value);

        default void prepareBatch(final List<KeyValue<Bytes, byte[]>> entries,
                                  final WriteBatchInterface batch)  throws RocksDBException {
            for (final KeyValue<Bytes, byte[]> entry : entries) {
                Objects.requireNonNull(entry.key, "key cannot be null");
                addToBatch(entry.key.get(), entry.value, batch);
            }
        };

        byte[] get(final DBAccessor accessor, final byte[] key) throws RocksDBException;

        /**
         * In contrast to get(), we don't migrate the key to new CF.
         * <p>
         * Use for get() within delete() -- no need to migrate, as it's deleted anyway
         */
        byte[] getOnly(final DBAccessor accessor, final byte[] key) throws RocksDBException;

        ManagedKeyValueIterator<Bytes, byte[]> range(final DBAccessor accessor,
                                              final Bytes from,
                                              final Bytes to,
                                              final boolean forward);

        /**
         * Deletes keys entries in the range ['from', 'to'], including 'from' and excluding 'to'.
         */
        void deleteRange(final DBAccessor accessor, final byte[] from, final byte[] to);

        ManagedKeyValueIterator<Bytes, byte[]> all(final DBAccessor accessor, final boolean forward);

        ManagedKeyValueIterator<Bytes, byte[]> prefixScan(final DBAccessor accessor, final Bytes prefix);

        long approximateNumEntries(final DBAccessor accessor) throws RocksDBException;

        void flush() throws RocksDBException;

        void addToBatch(final byte[] key,
                        final byte[] value,
                        final WriteBatchInterface batch) throws RocksDBException;

        void close();
    }

    class SingleColumnFamilyAccessor implements ColumnFamilyAccessor {
        private final ColumnFamilyHandle columnFamily;

        SingleColumnFamilyAccessor(final ColumnFamilyHandle columnFamily) {
            this.columnFamily = columnFamily;
        }

        @Override
        public void put(final DBAccessor accessor,
                        final byte[] key,
                        final byte[] value) {
            if (value == null) {
                try {
                    accessor.delete(columnFamily, key);
                } catch (final RocksDBException e) {
                    // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + name, e);
                }
            } else {
                try {
                    accessor.put(columnFamily, key, value);
                } catch (final RocksDBException e) {
                    // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while putting key/value into store " + name, e);
                }
            }
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            return accessor.get(columnFamily, key);
        }

        @Override
        public byte[] getOnly(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            return get(accessor, key);
        }

        public ManagedKeyValueIterator<Bytes, byte[]> range(final DBAccessor accessor,
                                                            final Bytes from,
                                                            final Bytes to,
                                                            final boolean forward) {
            return new RocksDBRangeIterator(
                    name,
                    accessor.newIterator(columnFamily),
                    from,
                    to,
                    forward,
                    true
            );
        }

        @Override
        public void deleteRange(final DBAccessor accessor, final byte[] from, final byte[] to) {
            try {
                accessor.deleteRange(columnFamily, from, to);
            } catch (final RocksDBException e) {
                // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while removing key from store " + name, e);
            }
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> all(final DBAccessor accessor, final boolean forward) {
            final RocksIterator innerIterWithTimestamp = accessor.newIterator(columnFamily);
            if (forward) {
                innerIterWithTimestamp.seekToFirst();
            } else {
                innerIterWithTimestamp.seekToLast();
            }
            return new RocksDbIterator(name, innerIterWithTimestamp, forward);
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> prefixScan(final DBAccessor accessor, final Bytes prefix) {
            final Bytes to = incrementWithoutOverflow(prefix);
            return new RocksDBRangeIterator(
                    name,
                    accessor.newIterator(columnFamily),
                    prefix,
                    to,
                    true,
                    false
            );
        }

        @Override
        public long approximateNumEntries(final DBAccessor accessor) throws RocksDBException {
            return accessor.approximateNumEntries(columnFamily);
        }

        @Override
        public void flush() throws RocksDBException {
            db.flush(fOptions, columnFamily);
        }

        @Override
        public void addToBatch(final byte[] key,
                               final byte[] value,
                               final WriteBatchInterface batch) throws RocksDBException {
            if (value == null) {
                batch.delete(columnFamily, key);
            } else {
                batch.put(columnFamily, key, value);
            }
        }

        @Override
        public void close() {
            columnFamily.close();
        }
    }

    void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        try (final WriteBatch batch = new WriteBatch()) {
            for (final ConsumerRecord<byte[], byte[]> record : records) {
                ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(
                    record,
                    consistencyEnabled,
                    position
                );
                // If version headers are not present or version is V0
                cf.addToBatch(record.key(), record.value(), batch);
            }
            write(batch);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error restoring batch to store " + name, e);
        }

    }

    // for testing
    public Options getOptions() {
        return userSpecifiedOptions;
    }

    @Override
    public Position getPosition() {
        return position;
    }

    @Override
    public long approximateNumUncommittedBytes() {
        return accessor.approximateNumUncommittedBytes();
    }

    /**
     * Same as {@link Bytes#increment(Bytes)} but {@code null} is returned instead of throwing
     * {@code IndexOutOfBoundsException} in the event of overflow.
     *
     * @param input bytes to increment
     * @return A new copy of the incremented byte array, or {@code null} if incrementing would
     *         result in overflow.
     */
    static Bytes incrementWithoutOverflow(final Bytes input) {
        try {
            return Bytes.increment(input);
        } catch (final IndexOutOfBoundsException e) {
            return null;
        }
    }
}