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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Base class for transactional stores, that tracks open transactions and can close them all.
 * <p>
 * Transactions created using {@link #newTransaction()} will be automatically tracked and closed when this store is
 * closed.
 *
 * @see Transaction
 */
public abstract class AbstractTransactionalStore implements StateStore {

    protected StateStoreContext context;

    private final Set<Transaction> openTransactions = Collections.synchronizedSet(new HashSet<>());

    /**
     * @return A new {@link Transaction}.
     */
    public abstract AbstractTransaction<? extends AbstractTransactionalStore> createTransaction();

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

    public void init(final StateStoreContext context,
                     final StateStore root) {
        this.context = context;
    }

    @Override
    public void close() {
        closeOpenTransactions();
    }

    /**
     * Creates a new Transaction and registers it to be automatically closed (if still open) when this state store is
     * closed.
     *
     * @return A new {@link Transaction}; or {@code this}, if no new transaction is necessary to satisfy the store's
     *         {@link org.apache.kafka.common.IsolationLevel}, or if this store does not support transactions.
     */
    @Override
    public final StateStore newTransaction() {
        if (context.isolationLevel() == IsolationLevel.READ_COMMITTED) {
            final AbstractTransaction<? extends AbstractTransactionalStore> transaction = createTransaction();
            transaction.init(context, this);
            return register(transaction);
        } else {
            return this;
        }
    }

    @Override
    public long approximateNumUncommittedBytes() {
        return openTransactions.stream()
                .map(Transaction::approximateNumUncommittedBytes)
                .reduce(0L, Long::sum);
    }

    int openTransactions() {
        return openTransactions.size();
    }

    protected StateStore register(final AbstractTransaction<? extends AbstractTransactionalStore> transaction) {
        transaction.addOnCloseListener(() -> openTransactions.remove(transaction));
        openTransactions.add(transaction);
        return transaction;
    }

    protected void closeOpenTransactions() {
        final HashSet<Transaction> transactions;
        synchronized (openTransactions) {
            transactions = new HashSet<>(openTransactions);
        }
        if (!transactions.isEmpty()) {
            for (final Transaction transaction : transactions) {
                transaction.close();
            }
        }
    }
}
