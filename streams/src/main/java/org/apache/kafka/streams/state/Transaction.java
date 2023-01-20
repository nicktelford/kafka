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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;

import java.util.Map;

/**
 * Represents a read/write transaction on a state store.
 * <p>
 * For compatibility, transactions implement the entire {@link StateStore} interface, however, they only represent a
 * <em>view</em> on an underlying {@link StateStore}; therefore methods that explicitly act on the entire store, such
 * as {@link #init} will do nothing, and others like {@link #flush()} and {@link #close()} will act upon this
 * transaction only.
 * <p>
 * Transactions are <em>NOT</em> thread-safe, and they should not be shared among multiple threads. Threads should
 * create their own {@link StateStore#newTransaction()} new transaction}, which will ensure proper isolation of
 * concurrent changes.
 * <p>
 * For resource-safety, Transactions implement the {@link AutoCloseable} interface, enabling try-with-resources:
 * <pre>
 *     try (final Transaction transaction = store.newTransaction()) {
 *         transaction.put("foo", "bar");
 *         transaction.flush();
 *     }
 * </pre>
 * If you are not using try-with-resources, you <em>must</em> call either {@link #flush()} exactly-once, or {@link
 * #close()} at least once, or risk possible resource leaks.
 * <p>
 * Transactions are isolated from each other, according to their {@link #isolationLevel()}. The semantics of the
 * isolation level corresponds to their <a
 * href="https://en.wikipedia.org/wiki/Isolation_(database_systems)#Read_phenomena">ANSI SQL 92 definitions</a>.
 * <p>
 * All isolation levels guarantee "read-your-own-writes", i.e. that writes in the transaction will be seen by
 * subsequent reads <em>from within the same transaction</em>. Other guarantees vary by isolation level:
 * <p>
 * <table>
 *     <tr>
 *         <th>Isolation Level</th>
 *         <th>Description</th>
 *         <th>Permitted Read Phenomena</th>
 *     </tr>
 *     <tr>
 *         <td>{@link IsolationLevel#READ_UNCOMMITTED}</td> // todo: ALOS
 *         <td>Allows queries to read writes from all ongoing transactions that have not-yet been committed.</td>
 *         <td>dirty reads, non-repeatable reads, phantom reads</td>
 *     </tr>
 *     <tr>
 *         <td>{@link IsolationLevel#READ_COMMITTED}</td> // todo: EOS
 *         <td>Allows queries to only read writes that have been committed to the StateStore. Writes by an ongoing
 *         transaction are not visible <em>until that transaction commits</em>.</td>
 *         <td>non-repeatable reads, phantom reads</td>
 *     </tr>
 * </table>
 * <p>
 * {@link IsolationLevel#READ_UNCOMMITTED} makes no guarantee as to exactly when records from other transactions become
 * visible. Therefore, implementations <em>may</em> not make uncommitted records from other transactions visible until
 * they're committed.
 */
public interface Transaction extends StateStore, AutoCloseable {

    /**
     * The {@link IsolationLevel} that reads and writes in this transaction are subject to.
     */
    IsolationLevel isolationLevel();

    /**
     * Initializes this Transaction.
     * <p>
     * Most transactions require no explicit initialization, therefore the default implementation of this method does
     * nothing.
     *
     * @deprecated Since 2.7.0, the parent method has been deprecated in favor of {@link
     *             #init(StateStoreContext, StateStore)}. Use that method instead.
     */
    @Override @Deprecated
    default void init(final ProcessorContext context, final StateStore root) {
        // do nothing
    }

    /**
     * Initializes this Transaction.
     * <p>
     * Most transactions require no explicit initialization, therefore the default implementation of this method does
     * nothing.
     */
    @Override
    default void init(final StateStoreContext context, final StateStore root) {
        // do nothing
    }

    /**
     * Creates a new {@link Transaction} from an existing transaction.
     * <p>
     * This enables potentially re-using resources from an existing, no longer in-use transaction, instead of creating
     * a new one.
     * <p>
     * This method should only be called if the current transaction is guaranteed to no longer be in-use.
     * Implementations may return either a new Transaction instance, or a reference to themselves, only if they are able
     * to reset to being available for use.
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
    @Override
    StateStore newTransaction();

    /**
     * Is this transaction is open for reading and writing.
     * @return {@code true} if this Transaction can be read from/written to, or {@code false} if it is no longer
     *         accessible.
     */
    @Override
    boolean isOpen();

    /**
     * Commit this Transaction.
     * <p>
     * Any records help by this Transaction should be written to the underlying state store.
     * <p>
     * Once this method returns successfully, this Transaction will no longer be available to use for reads/writes.
     *
     * @see #close() to close the Transaction without writing data to the underlying state store.
     */
    @Override
    void commit(final Map<TopicPartition, Long> changelogOffsets);

    /**
     * Closes this Transaction, without committing records.
     * <p>
     * Any uncommitted records should <em>not</em> be written to the underlying state store, and should instead be
     * discarded.
     * <p>
     * This method should be used to "rollback" a Transaction that should not be {@link #flush() committed}.
     * <p>
     * The underlying {@link StateStore} will <em>not</em> be closed by this method.
     * <p>
     * This method is idempotent: repeatedly calling {@code close()} will not produce an error.
     *
     * @see #flush() to close this Transaction by writing its records to the underlying state store.
     */
    @Override
    void close();
}
