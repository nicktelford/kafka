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
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.Is.is;
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
        final StreamThread mockStreamThread = EasyMock.mock(StreamThread.class);
        store = timestamped ?
                new TransactionalTimestampedKeyValueStore<>(new RocksDBTimestampedStore(STORE_NAME, "rocksdb"), () -> mockStreamThread) :
                new TransactionalKeyValueStore<>(new RocksDBStore(STORE_NAME, "rocksdb"), () -> mockStreamThread);

        context = new InternalMockProcessorContext<>(TestUtils.tempDirectory(), CONFIG);
        store.init((StateStoreContext) context, store);
    }

    @After
    public void tearDown() {
        store.close();
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
    public void shouldResetTransactionOnFlush() {
        store.put(KEY_1_BYTES, VALUE_ABC_BYTES);
        assertThat(store.approximateNumEntries(), is(1L));
        assertThat(store.approximateNumUncommittedBytes(), is((long) KEY_1_BYTES.get().length + VALUE_ABC_BYTES.length));
        store.flush();
        assertThat(store.currentTransaction.isOpen(), is(true));
        assertThat(store.approximateNumUncommittedBytes(), is(0L));
        assertThat(store.approximateNumEntries(), is(1L));
    }

    @Test
    public void shouldCloseTransactionOnClose() {
        store.put(KEY_1_BYTES, VALUE_ABC_BYTES);
        assertThat(store.currentTransaction.isOpen(), is(true));
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

    @Test
    public void shouldCloseOpenIteratorsOnClose() {
        final KeyValueIterator<Bytes, byte[]> it = store.all();
        assertThat(it.hasNext(), is(false));
        store.close();
        assertThrows(InvalidStateStoreException.class, it::hasNext);
    }

    @Test
    public void shouldQueryStoreDirectlyForInteractiveQueries() {
        final TransactionalKeyValueStore<RocksDBStore, Bytes, byte[]> interactiveStore = new TransactionalKeyValueStore<>(store.wrapped());
        assertThat(interactiveStore.currentTransaction, is(equalTo(null)));
        interactiveStore.wrapped().put(KEY_1_BYTES, VALUE_ABC_BYTES);
        assertThat(interactiveStore.get(KEY_1_BYTES), is(equalTo(VALUE_ABC_BYTES)));
        assertThat(interactiveStore.currentTransaction, is(equalTo(null)));
    }
}
