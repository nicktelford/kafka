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
package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.LogAndSuspendExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.internals.Task;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SuspendDeserializationExceptionHandlerTest {

    final StreamsBuilder builder = new StreamsBuilder();

    private final static Serde<Long> LONG_SERDE = Serdes.Long();
    private final static Serde<byte[]> BYTES_SERDE = Serdes.ByteArray();
    private final static Serde<String> STR_SERDE = Serdes.String();

    private final static Properties CONFIG = new Properties() {{
                put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndSuspendExceptionHandler.class.getName());
            }};

    @Test
    public void shouldNotSuspendTaskWithoutError() {
        try (final TopologyTestDriver driver = new TopologyTestDriver(testTopology(builder), CONFIG)) {

            final TestInputTopic<Long, Long> in = driver.createInputTopic("in", LONG_SERDE.serializer(), LONG_SERDE.serializer());
            final TestOutputTopic<Long, Long> out = driver.createOutputTopic("out", LONG_SERDE.deserializer(), LONG_SERDE.deserializer());
            in.pipeInput(1L, 123L);
            in.pipeInput(2L, 456L);

            assertEquals(new KeyValue<>(1L, 123L), out.readKeyValue());
            assertEquals(new KeyValue<>(2L, 456L), out.readKeyValue());
            assertTrue(out.isEmpty());
        }
    }

    @Test
    public void shouldSuspendTaskOnDeserializationError() {
        try (final TopologyTestDriver driver = new TopologyTestDriver(testTopology(builder), CONFIG)) {

            final Serializer<Long> longSerializer = LONG_SERDE.serializer();
            final Serializer<String> stringSerializer = STR_SERDE.serializer();

            final TestInputTopic<byte[], byte[]> in = driver.createInputTopic("in", BYTES_SERDE.serializer(), BYTES_SERDE.serializer());
            final TestOutputTopic<Long, Long> out = driver.createOutputTopic("out", LONG_SERDE.deserializer(), LONG_SERDE.deserializer());
            in.pipeInput(longSerializer.serialize("in", 1L), longSerializer.serialize("in", 123L));
            assertEquals(new KeyValue<>(1L, 123L), out.readKeyValue());

            assertEquals(Task.State.RUNNING, driver.task.state());
            in.pipeInput(longSerializer.serialize("in", 2L), stringSerializer.serialize("in", "456"));
            assertEquals(Task.State.SUSPENDED, driver.task.state());

            assertTrue(out.isEmpty());
        }
    }

    private Topology testTopology(final StreamsBuilder builder) {
        builder.stream("in", Consumed.with(Serdes.Long(), Serdes.Long())).to("out");
        return builder.build();
    }
}

