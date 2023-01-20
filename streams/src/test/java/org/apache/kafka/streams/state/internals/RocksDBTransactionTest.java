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
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.AbstractTransactionTest;
import org.apache.kafka.streams.state.AbstractTransactionalStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@RunWith(Parameterized.class)
public class RocksDBTransactionTest extends AbstractTransactionTest {
    
    protected static final String DB_NAME = "test-db";
    protected static final String METRICS_SCOPE = "metrics-scope";

    private static final Bytes KEY_1_BYTES = Bytes.wrap(new byte[] {0x1});
    private static final Bytes KEY_2_BYTES = Bytes.wrap(new byte[] {0x2});

    private static final byte[] VALUE_ABC_BYTES = "abc".getBytes();
    private static final byte[] VALUE_DEF_BYTES = "def".getBytes();

    @Parameterized.Parameters(name = "{0}")
    public static Object[] params() {
        return new Object[] {RocksDBStore.class, RocksDBTimestampedStore.class};
    }

    @Parameterized.Parameter
    public Class<AbstractTransactionalStore> storeClass;

    @Override
    protected AbstractTransactionalStore createTransactionalStore(final StateStoreContext context) {
        try {
            return storeClass.getDeclaredConstructor(String.class, String.class).newInstance(DB_NAME, METRICS_SCOPE);
        } catch (final ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private RocksDBStore store() {
        return (RocksDBStore) store;
    }

    @SuppressWarnings("unchecked")
    private RocksDBTransaction<RocksDBStore> transaction() {
        return (RocksDBTransaction<RocksDBStore>) transaction;
    }

    @SuppressWarnings("unchecked")
    private RocksDBTransaction<RocksDBStore> newTransaction() {
        return (RocksDBTransaction<RocksDBStore>) store().newTransaction();
    }

    @Test
    public void shouldReadOwnWritesBeforeFlush() {
        try (RocksDBTransaction<RocksDBStore> transaction = transaction()) {
            transaction.put(KEY_1_BYTES, VALUE_ABC_BYTES);
            assertThat(transaction.get(KEY_1_BYTES), is(equalTo(VALUE_ABC_BYTES)));
        }
    }

    @Test
    public void shouldIsolateTransactions() {
        try (RocksDBTransaction<RocksDBStore> first = transaction();
             RocksDBTransaction<RocksDBStore> second = newTransaction()) {

            first.put(KEY_1_BYTES, VALUE_ABC_BYTES);
            second.put(KEY_2_BYTES, VALUE_DEF_BYTES);

            assertThat(first.get(KEY_2_BYTES), is(equalTo(null)));
            assertThat(second.get(KEY_1_BYTES), is(equalTo(null)));
        }
    }

    @Test
    public void shouldUpdateStoreOnTransactionFlush() {
        try (final RocksDBTransaction<RocksDBStore> transaction = transaction()) {
            transaction.put(KEY_1_BYTES, VALUE_ABC_BYTES);
            assertThat(store().get(KEY_1_BYTES), is(equalTo(null)));
            transaction.commit(Collections.emptyMap());
        }
        assertThat(store().get(KEY_1_BYTES), is(VALUE_ABC_BYTES));
    }

    @Test
    public void shouldNotUpdateStoreOnTransactionClose() {
        try (final RocksDBTransaction<RocksDBStore> transaction = transaction()) {
            transaction.put(KEY_1_BYTES, VALUE_ABC_BYTES);
            assertThat(store().get(KEY_1_BYTES), is(equalTo(null)));
        }
        assertThat(store().get(KEY_1_BYTES), is(equalTo(null)));
    }

    @Test
    public void shouldCloseOpenIteratorsOnTransactionFlush() {
        try (final RocksDBTransaction<RocksDBStore> transaction = transaction()) {
            final KeyValueIterator<Bytes, byte[]> it = transaction.all();
            assertThat(it.hasNext(), is(false));
            transaction.commit(Collections.emptyMap());
            assertThrows(InvalidStateStoreException.class, it::hasNext);
        }
    }

    @Test
    public void shouldCloseOpenIteratorsOnTransactionClose() {
        try (final RocksDBTransaction<RocksDBStore> transaction = transaction()) {
            final KeyValueIterator<Bytes, byte[]> it = transaction.all();
            assertThat(it.hasNext(), is(false));
            transaction.close();
            assertThrows(InvalidStateStoreException.class, it::hasNext);
        }
    }
}
