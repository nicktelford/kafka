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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatchInterface;

import java.io.IOException;

public class TransactionalSegment<S extends Segment>
        extends TransactionalKeyValueStore<S, Bytes, byte[]> implements Segment {

    private Segment currentTransaction() {
        return (Segment) currentTransaction;
    }

    TransactionalSegment(final S wrapped) {
        super(wrapped);
    }

    @Override
    public Segment newTransaction() {
        final StateStore transaction = wrapped().newTransaction();
        if (transaction instanceof Segment) {
            return (Segment) transaction;
        } else {
            throw new IllegalArgumentException("New Transactions created by " +
                    wrapped().getClass().getSimpleName() + " must implement Segment");
        }
    }

    @Override
    public void addToBatch(final KeyValue<byte[], byte[]> record,
                           final WriteBatchInterface batch) throws RocksDBException {
        currentTransaction().addToBatch(record, batch);
    }

    @Override
    public void write(final WriteBatchInterface batch) throws RocksDBException {
        currentTransaction().write(batch);
    }

    @Override
    public void destroy() throws IOException {
        currentTransaction().destroy();
    }

    @Override
    public void deleteRange(final Bytes keyFrom, final Bytes keyTo) {
        currentTransaction().deleteRange(keyFrom, keyTo);
    }

    @Override
    public String toString() {
        return wrapped().toString();
    }
}
