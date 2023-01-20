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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.StoreToProcessorContextAdapter;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.Transaction;

import java.util.Collections;
import java.util.Map;

/**
 * A storage engine for managing state maintained by a stream processor.
 * <p>
 * If the store is implemented as a persistent store, it <em>must</em> use the store name as directory name and write
 * all data into this store directory.
 * The store directory must be created with the state directory.
 * The state directory can be obtained via {@link ProcessorContext#stateDir() #stateDir()} using the
 * {@link ProcessorContext} provided via {@link #init(StateStoreContext, StateStore) init(...)}.
 * <p>
 * Using nested store directories within the state directory isolates different state stores.
 * If a state store would write into the state directory directly, it might conflict with others state stores and thus,
 * data might get corrupted and/or Streams might fail with an error.
 * Furthermore, Kafka Streams relies on using the store name as store directory name to perform internal cleanup tasks.
 * <p>
 * This interface does not specify any query capabilities, which, of course,
 * would be query engine specific. Instead, it just specifies the minimum
 * functionality required to reload a storage engine from its changelog as well
 * as basic lifecycle management.
 */
public interface StateStore {

    /**
     * The name of this store.
     * @return the storage name
     */
    String name();

    /**
     * Initializes this state store.
     * <p>
     * The implementation of this function must register the root store in the context via the
     * {@link org.apache.kafka.streams.processor.ProcessorContext#register(StateStore, StateRestoreCallback)} function,
     * where the first {@link StateStore} parameter should always be the passed-in {@code root} object, and
     * the second parameter should be an object of user's implementation
     * of the {@link StateRestoreCallback} interface used for restoring the state store from the changelog.
     * <p>
     * Note that if the state store engine itself supports bulk writes, users can implement another
     * interface {@link BatchingStateRestoreCallback} which extends {@link StateRestoreCallback} to
     * let users implement bulk-load restoration logic instead of restoring one record at a time.
     * <p>
     * This method is not called if {@link StateStore#init(StateStoreContext, StateStore)}
     * is implemented.
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     * @deprecated Since 2.7.0. Callers should invoke {@link #init(StateStoreContext, StateStore)} instead.
     *             Implementers may choose to implement this method for backward compatibility or to throw an
     *             informative exception instead.
     */
    @Deprecated
    void init(org.apache.kafka.streams.processor.ProcessorContext context, StateStore root);

    /**
     * Initializes this state store.
     * <p>
     * The implementation of this function must register the root store in the context via the
     * {@link StateStoreContext#register(StateStore, StateRestoreCallback, CommitCallback)} function, where the
     * first {@link StateStore} parameter should always be the passed-in {@code root} object, and
     * the second parameter should be an object of user's implementation
     * of the {@link StateRestoreCallback} interface used for restoring the state store from the changelog.
     * <p>
     * Note that if the state store engine itself supports bulk writes, users can implement another
     * interface {@link BatchingStateRestoreCallback} which extends {@link StateRestoreCallback} to
     * let users implement bulk-load restoration logic instead of restoring one record at a time.
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    default void init(final StateStoreContext context, final StateStore root) {
        init(StoreToProcessorContextAdapter.adapt(context), root);
    }

    /**
     * Creates a new {@link Transaction} for reading/writing to this state store.
     * <p>
     * State stores that do not support transactions will return {@code this} instead, which should be considered a
     * transaction that doesn't provide any isolation or atomicity guarantees.
     * <p>
     * {@link Transaction Transactions} are <em>not thread-safe</em>, and should not be shared among threads. New
     * threads should use this method to create a new transaction, instead of sharing an existing one.
     * <p>
     * Transactions created by this method will have the same {@link IsolationLevel} as the {@link StateStoreContext}
     * that this store was {@link #init(StateStoreContext, StateStore) initialized with}.
     *
     * @return A new {@link Transaction} to control reads/writes to this {@link StateStore}. The Transaction
     *         <em>MUST</em> be {@link Transaction#flush() committed} or {@link Transaction#close() closed} when you
     *         are finished with it, to prevent resource leaks.
     */
    default StateStore newTransaction() {
        return this;
    }

    /**
     * Flush any cached data
     * @deprecated Use {@link #commit(Map)} instead.
     */
    @Deprecated
    default void flush() {
        commit(Collections.emptyMap());
    }

    /**
     * todo: document
     * @param changelogOffsets The input/changelog topic offsets of the records being committed.
     */
    default void commit(final Map<TopicPartition, Long> changelogOffsets) {
        flush();
    }

    /**
     * Returns whether this StateStore manages its own changelog offsets.
     * <p>
     * This can only return {@code true} if {@link #persistent()} also returns {@code true}, as non-persistent stores
     * have no checkpoints to manage.
     *
     * @return Whether this StateStore manages its own changelog offsets.
     */
    default boolean managesCheckpoints() {
        return false;
    }

    /**
     * Returns the committed offset for the given partition.
     * <p>
     * If this store is not {@link #persistent()} or does not {@link #managesCheckpoints() manage its own checkpoints},
     * it will always yield {@code null}.
     * <p>
     * If {@code topicPartition} is a changelog partition or Topology input partition for this StateStore, this method
     * will return the committed offset for that partition.
     * <p>
     * If no committed offset exists for the given partition, or if the partition is not a changelog or input partition
     * for the store, {@code null} will be returned.
     *
     * @param topicPartition The changelog/input partition to get the committed offset for.
     * @return The committed offset for the given partition, or {@code null} if no committed offset exists, or if this
     *         store does not contain committed offsets.
     */
    default Long getCommittedOffset(final TopicPartition topicPartition) {
        return null;
    }

    /**
     * Close the storage engine.
     * Note that this function needs to be idempotent since it may be called
     * several times on the same state store.
     * <p>
     * Users only need to implement this function but should NEVER need to call this api explicitly
     * as it will be called by the library automatically when necessary
     */
    void close();

    /**
     * Return if the storage is persistent or not.
     *
     * @return  {@code true} if the storage is persistent&mdash;{@code false} otherwise
     */
    boolean persistent();

    /**
     * Is this store open for reading and writing
     * @return {@code true} if the store is open
     */
    boolean isOpen();

    /**
     * Execute a query. Returns a QueryResult containing either result data or
     * a failure.
     * <p>
     * If the store doesn't know how to handle the given query, the result
     * shall be a {@link FailureReason#UNKNOWN_QUERY_TYPE}.
     * If the store couldn't satisfy the given position bound, the result
     * shall be a {@link FailureReason#NOT_UP_TO_BOUND}.
     * <p>
     * Note to store implementers: if your store does not support position tracking,
     * you can correctly respond {@link FailureReason#NOT_UP_TO_BOUND} if the argument is
     * anything but {@link PositionBound#unbounded()}. Be sure to explain in the failure message
     * that bounded positions are not supported.
     * <p>
     * @param query The query to execute
     * @param positionBound The position the store must be at or past
     * @param config Per query configuration parameters, such as whether the store should collect detailed execution
     * info for the query
     * @param <R> The result type
     */
    @Evolving
    default <R> QueryResult<R> query(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config) {
        // If a store doesn't implement a query handler, then all queries are unknown.
        return QueryResult.forUnknownQueryType(query, this);
    }

    /**
     * Returns the position the state store is at with respect to the input topic/partitions
     */
    @Evolving
    default Position getPosition() {
        throw new UnsupportedOperationException(
            "getPosition is not implemented by this StateStore (" + getClass() + ")"
        );
    }

    /**
     * Return an approximate count of records not yet committed to this StateStore.
     * <p>
     * This method will return an approximation of the number of records that would be committed by the next call to
     * {@link #flush()}.
     * <p>
     * If this StateStore is unable to approximately count uncommitted records, it will return {@code -1}.
     * If this StateStore does not support atomic transactions, it will return {@code 0}, because records will always
     * be immediately written to a non-transactional store, so there will be none awaiting a {@link #flush()}.
     *
     * @return The approximate number of records awaiting {@link #flush()}, {@code -1} if the number of
     *         uncommitted records can't be counted, or {@code 0} if this StateStore does not support transactions.
     */
    default long approximateNumUncommittedEntries() {
        return 0;
    }

    /**
     * Return an approximate count of memory used by records not yet committed to this StateStore.
     * <p>
     * This method will return an approximation of the memory would be freed by the next call to {@link #flush()}.
     * <p>
     * If this StateStore is unable to approximately count uncommitted memory usage, it will return {@code -1}.
     * If this StateStore does not support atomic transactions, it will return {@code 0}, because records will always
     * be immediately written to a non-transactional store, so there will be none awaiting a {@link #flush()}.
     *
     * @return The approximate size of all records awaiting {@link #flush()}, {@code -1} if the size of uncommitted
     *         records can't be counted, or {@code 0} if this StateStore does not support transactions.
     */
    default long approximateNumUncommittedBytes() {
        return 0;
    }
}
