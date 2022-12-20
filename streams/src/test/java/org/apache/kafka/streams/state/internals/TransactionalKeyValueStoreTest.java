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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

@RunWith(Parameterized.class)
public class TransactionalKeyValueStoreTest {

    private static final String STORE_NAME = "test-store";
    private static final StreamsConfig CONFIG;

    private static final Bytes KEY_1_BYTES = Bytes.wrap(new byte[] {0x1});

    private static final byte[] VALUE_ABC_BYTES = "abc".getBytes();

    static {
        final Properties streamProps = StreamsTestUtils.getStreamsConfig();
        streamProps.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        CONFIG = new StreamsConfig(streamProps);
    }

    @Parameterized.Parameters(name = "timestamped = {0}")
    public static Object[] params() {
        return new Object[] {false, true};
    }

    @Parameterized.Parameter
    public boolean timestamped;

    private InternalMockProcessorContext<Bytes, byte[]> context;
    private TransactionalKeyValueStore<RocksDBStore, Bytes, byte[]> store;

    @Before @SuppressWarnings("unchecked")
    public void setUp() {
        final RocksDbKeyValueBytesStoreSupplier supplier = new RocksDbKeyValueBytesStoreSupplier(STORE_NAME, timestamped);
        store = (TransactionalKeyValueStore<RocksDBStore, Bytes, byte[]>) supplier.get();
        context = new InternalMockProcessorContext<>(TestUtils.tempDirectory(), CONFIG);
        store.init((StateStoreContext) context, store);
    }

    @After
    public void tearDown() {
        store.close();
    }

    @Test
    public void shouldHaveTransactionAfterInit() {
        // init is called in setUp, therefore a transaction should already be active
        assertThat(store.currentTransaction, is(not(equalTo(null))));
        assertThat(store.currentTransaction.isOpen(), is(true));
    }

    // todo: equivalent tests for all other KeyValueStore methods
    @Test
    public void shouldPutViaTransaction() {
        store.put(KEY_1_BYTES, VALUE_ABC_BYTES);
        assertThat(store.currentTransaction.get(KEY_1_BYTES), is(equalTo(VALUE_ABC_BYTES)));
        assertThat(store.wrapped().get(KEY_1_BYTES), is(equalTo(null)));
    }

    @Test
    public void shouldFlushTransactionOnStoreFlush() {
        store.put(KEY_1_BYTES, VALUE_ABC_BYTES);
        assertThat(store.wrapped().get(KEY_1_BYTES), is(equalTo(null)));
        store.flush();
        assertThat(store.wrapped().get(KEY_1_BYTES), is(equalTo(VALUE_ABC_BYTES)));
    }

    @Test
    public void shouldCycleTransactionOnStoreFlush() {
        final KeyValueStore<Bytes, byte[]> initialTransaction = store.currentTransaction;
        store.flush();
        assertThat(store.currentTransaction, is(not(sameInstance(initialTransaction))));
    }

    @Test
    public void shouldCloseTransactionOnClose() {
        store.close();
        assertThat(store.currentTransaction.isOpen(), is(false));
    }

    @Test
    public void shouldCloseOpenIteratorsOnFlush() {
        final KeyValueIterator<Bytes, byte[]> it = store.all();
        assertThat(it.hasNext(), is(false));
        store.flush();
        assertThrows(InvalidStateStoreException.class, it::hasNext);
    }
}
