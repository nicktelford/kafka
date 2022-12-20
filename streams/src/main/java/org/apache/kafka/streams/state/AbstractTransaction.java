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
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Base implementation for {@link Transaction transactions} managed by an implementation of {@link
 * AbstractTransactionalStore}.
 * <p>
 * This base implementation facilitates the automatic tracking and resource management of transactions created by {@link
 * AbstractTransactionalStore}.
 * <p>
 * Implementations of this class should refrain from overriding any of these base implementations, and instead provide
 * implementations for {@link #commitTransaction()} and {@link #closeTransaction()}, and optionally {@link
 * #init(StateStoreContext, StateStore)}.
 *
 * @param <S> The type of the {@link StateStore} that spawned this transaction.
 */
public abstract class AbstractTransaction<S extends StateStore> implements Transaction {

    protected final List<Runnable> onCloseListeners = new ArrayList<>();

    protected StateStoreContext context;
    protected S store;
    protected boolean isOpen = true;

    public abstract void commitTransaction();

    public abstract void closeTransaction();

    /**
     * Add a function that is run after this {@link Transaction} is closed.
     * <p>
     * These listeners will be run even if the {@link #commitTransaction()} or {@link #closeTransaction()} methods throw
     * an exception.
     *
     * @param listener A function to run after this Transaction has been closed.
     */
    void addOnCloseListener(final Runnable listener) {
        onCloseListeners.add(listener);
    }

    protected void validateIsOpen() {
        if (!isOpen) {
            throw new InvalidStateStoreException("Transaction for store " + store.name() + " has been closed.");
        }
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

    @Override @SuppressWarnings("unchecked")
    public void init(final StateStoreContext context, final StateStore root) {
        this.context = context;

        try {
            this.store = (S) root;
        } catch (final ClassCastException e) {
            throw new StreamsException(getClass().getSimpleName() +
                    "#init was called with a StateStore that is not of the type supported by this Transaction. " +
                    "The caller should provide the StateStore that constructed this Transaction.");
        }
    }

    @Override
    public IsolationLevel isolationLevel() {
        return context.isolationLevel();
    }

    @Override
    public void flush() {
        if (!isOpen) return;
        isOpen = false;
        try {
            commitTransaction();
        } finally {
            for (final Runnable listener : onCloseListeners) {
                listener.run();
            }
        }
    }

    @Override
    public void close() {
        if (!isOpen) return;
        isOpen = false;
        try {
            closeTransaction();
        } finally {
            for (final Runnable listener : onCloseListeners) {
                listener.run();
            }
        }
    }

    @Override
    public String name() {
        return store.name();
    }

    @Override
    public boolean persistent() {
        return store.persistent();
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public StateStore newTransaction() {
        return store.newTransaction();
    }
}
