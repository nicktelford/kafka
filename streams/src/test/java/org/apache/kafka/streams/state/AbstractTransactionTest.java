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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.Is.isA;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

public abstract class AbstractTransactionTest {
    protected abstract AbstractTransactionalStore createTransactionalStore(final StateStoreContext context);

    protected InternalMockProcessorContext<Integer, String> context;
    protected AbstractTransactionalStore store;
    protected AbstractTransaction<? extends AbstractTransactionalStore> transaction;

    @Before @SuppressWarnings("unchecked")
    public void before() {
        final Properties streamProps = StreamsTestUtils.getStreamsConfig();
        streamProps.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        context = new InternalMockProcessorContext<>(TestUtils.tempDirectory(), new StreamsConfig(streamProps));
        context.setTime(10);
        store = createTransactionalStore(context);
        store.init((StateStoreContext) context, store);
        transaction = (AbstractTransaction<AbstractTransactionalStore>) store.newTransaction();
    }

    @After
    public void after() {
        store.close();
    }

    @Test
    public void shouldInitWithSameContextAsStore() {
        assertThat(transaction.context, is(sameInstance(store.context)));
    }

    @Test
    public void shouldInitWithStoreReference() {
        assertThat(transaction.store, is(sameInstance(store)));
    }

    @Test
    public void shouldHaveSameIsolationLevelAsContext() {
        assertThat(transaction.isolationLevel(), is(equalTo(context.isolationLevel())));
    }

    @Test
    public void shouldHaveSameNameAsStore() {
        assertThat(transaction.name(), is(equalTo(store.name())));
    }

    @Test
    public void shouldHaveSamePersistenceAsStore() {
        assertThat(transaction.persistent(), is(equalTo(store.persistent())));
    }

    @Test
    public void shouldCreateTransactions() {
        assertThat(transaction.newTransaction(), isA(store.newTransaction().getClass()));
    }

    @Test
    public void shouldHaveReadCommittedIsolationLevel() {
        assertThat(context.isolationLevel(), is(IsolationLevel.READ_COMMITTED));
    }

    @Test
    public void shouldProduceNewTransactions() {
        assertThat(transaction, isA(AbstractTransaction.class));
    }

    @Test
    public void flushIsIdempotent() {
        transaction.commit(Collections.emptyMap());
        transaction.commit(Collections.emptyMap());
    }

    @Test
    public void shouldBeClosedAfterFlush() {
        assertThat(transaction.isOpen(), is(true));
        transaction.commit(Collections.emptyMap());
        assertThat(transaction.isOpen(), is(false));
    }

    @Test
    public void shouldStopTrackingFlushedTransactions() {
        assertThat(store.openTransactions(), is(1));
        transaction.commit(Collections.emptyMap());
        assertThat(store.openTransactions(), is(0));
    }

    @Test
    public void shouldStopTrackingWhenExceptionDuringFlush() {
        // create, initialize and register a "broken" transaction with our store
        final MockBrokenTransaction transaction = new MockBrokenTransaction();
        transaction.init(store.context, store);
        store.register(transaction);

        assertThat(store.openTransactions(), is(2));
        assertThrows(RuntimeException.class, () -> transaction.commit(Collections.emptyMap()));
        assertThat(store.openTransactions(), is(1));
    }

    @Test
    public void closeIsIdempotent() {
        transaction.close();
        transaction.close();
    }

    @Test
    public void shouldBeClosedAfterClose() {
        assertThat(transaction.isOpen(), is(true));
        transaction.close();
        assertThat(transaction.isOpen(), is(false));
    }

    @Test
    public void shouldCloseOpenTransactionsOnClose() {
        assertThat(transaction.isOpen(), is(true));
        store.close();
        assertThat(transaction.isOpen(), is(false));
    }

    @Test
    public void shouldStopTrackingClosedTransactions() {
        assertThat(store.openTransactions(), is(1));
        transaction.close();
        assertThat(store.openTransactions(), is(0));
    }

    @Test
    public void shouldStopTrackingWhenExceptionDuringClose() {
        // create, initialize and register a "broken" transaction with our store
        final MockBrokenTransaction transaction = new MockBrokenTransaction();
        transaction.init(store.context, store);
        store.register(transaction);

        assertThat(store.openTransactions(), is(2));
        assertThrows(RuntimeException.class, transaction::close);
        assertThat(store.openTransactions(), is(1));
    }

    private static class MockBrokenTransaction extends AbstractTransaction<AbstractTransactionalStore> {

        @Override
        public void commitTransaction(final Map<TopicPartition, Long> offsets) {
            throw new RuntimeException("commit failed!");
        }

        @Override
        public void closeTransaction() {
            throw new RuntimeException("close failed!");
        }
    }
}
