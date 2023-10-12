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
package org.apache.kafka.streams.integration;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndSuspendExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class SuspendDeserializationExceptionHandlerIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(SuspendDeserializationExceptionHandlerIntegrationTest.class);
    
    private static final Serializer<Long> LONG_SER = Serdes.Long().serializer();
    private static final Serializer<String> STR_SER = Serdes.String().serializer();
    private static final Deserializer<Long> LONG_DESER = Serdes.Long().deserializer();

    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC_NAME = "in";
    private static final String OUTPUT_TOPIC_NAME = "out";
    private static final String GROUP_ID = "test-group";

    public final static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        NUM_BROKERS,
        Utils.mkProperties(new HashMap<String, String>() {{
                put(KafkaConfig.AutoCreateTopicsEnableProp(), "true");
                put(KafkaConfig.TransactionsTopicMinISRProp(), "1");
                put(KafkaConfig.TransactionsTopicReplicationFactorProp(), "1");
            }})
    );
    private final MockTime mockTime = CLUSTER.time;
    
    public final static Properties CONFIG = new Properties();
    public static KafkaStreams app;

    private static Field threadsField;

    @BeforeAll
    public static void setUp() throws IOException, InterruptedException, NoSuchFieldException {
        // setup reflection
        threadsField = KafkaStreams.class.getDeclaredField("threads");
        threadsField.setAccessible(true);
        
        CLUSTER.start();
        CLUSTER.createTopic(INPUT_TOPIC_NAME, 2, 1);
        CLUSTER.createTopic(OUTPUT_TOPIC_NAME, 2, 1);

        CONFIG.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndSuspendExceptionHandler.class.getName());
        CONFIG.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, GROUP_ID);
        CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1L);
        CONFIG.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        CONFIG.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTIONAL_ID_CONFIG), "test");

        app = new KafkaStreams(testTopology(new StreamsBuilder()), CONFIG);
        app.start();
    }

    @AfterAll
    public static void tearDown() {
        app.close();
        CLUSTER.stop();
    }

    @Test
    public void shouldCommitOffsetOfLastSuccessfulRecordOnSuspend() throws InterruptedException, ExecutionException {
        final List<KeyValue<Long, byte[]>> kvs = Arrays.asList(
            new KeyValue<>(1L, LONG_SER.serialize(INPUT_TOPIC_NAME, 123L)),
            new KeyValue<>(2L, STR_SER.serialize(INPUT_TOPIC_NAME, "456"))
        );
        TestUtils.waitForCondition(
                () -> app.state() == KafkaStreams.State.RUNNING,
                10 * 1000,
                "App is not running."
        );
        TestUtils.waitForCondition(
                () -> streamThreads().stream().allMatch(thread -> thread.readyOnlyAllTasks().size() == 2),
                10 * 1000,
                "Never suspended all tasks."
        );
        assertTrue(streamThreads().get(0).readOnlyActiveTasks().stream().allMatch(task -> task.state() == Task.State.RUNNING));
        produceValues(Optional.of(0), kvs);
        assertTrue(app.state().isRunningOrRebalancing());
        // Wait for 0_0 to be suspended
        TestUtils.waitForCondition(
            () -> streamThreads().stream().flatMap(thread -> thread.readOnlyActiveTasks().stream())
                    .filter(task -> task.id().equals(new TaskId(0, 0)))
                    .findFirst()
                    .map(task -> task.state() == Task.State.SUSPENDED)
                    .orElse(false),
            10 * 1000,
            () -> "Task 0_0 was never SUSPENDED: " + streamThreads().stream().flatMap(thread ->
                thread.readOnlyActiveTasks().stream().map(task -> task.id().toString() + " (" + task.state().toString() + ")")
            ).collect(Collectors.joining(", "))
        );

//        TestUtils.waitForCondition(
//                () -> !getInputTopicOffsets().isEmpty(),
//                10 * 1000,
//                "Failed to commit offsets."
//        );

        // verify first (valid) record was output
        final List<KeyValue<Long, Long>> expected = kvs.stream().limit(1)
                .map(kv -> new KeyValue<>(kv.key, LONG_DESER.deserialize(OUTPUT_TOPIC_NAME, kv.value)))
                .collect(Collectors.toList());
        final List<KeyValue<Long, Long>> actual = consumeValues(Optional.of(0)).stream()
                .map(kv -> new KeyValue<>(kv.key, LONG_DESER.deserialize(OUTPUT_TOPIC_NAME, kv.value)))
                .collect(Collectors.toList());
        assertEquals(expected, actual);

        // verify offsets committed for first (valid) record
        final Map<Integer, Long> expectedOffsets = new HashMap<Integer, Long>() {{
                put(0, 1L);
            }};
        assertEquals(expectedOffsets, getInputTopicOffsets());
    }

    private static Topology testTopology(final StreamsBuilder builder) {
        builder.stream("in", Consumed.with(Serdes.Long(), Serdes.Long()))
                .to("out", Produced.with(Serdes.Long(), Serdes.Long()));
        return builder.build();
    }

    private void produceValues(final Optional<Integer> partition, final Collection<KeyValue<Long, byte[]>> keyValues) {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        IntegrationTestUtils.produceSynchronously(
            producerProps,
            false,
            INPUT_TOPIC_NAME,
            partition,
            keyValues.stream()
                .map(kv -> new KeyValueTimestamp<>(kv.key, kv.value, mockTime.milliseconds()))
                .collect(Collectors.toList())
        );
    }

    private Map<Integer, Long> getInputTopicOffsets() throws ExecutionException, InterruptedException {
        final Properties adminProps = new Properties() {{
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
            }};
        try (final AdminClient admin = AdminClient.create(adminProps)) {
            LOG.info("Found consumer group(s): {}",
                    admin.listConsumerGroups().all().get().stream().map(ConsumerGroupListing::groupId).collect(Collectors.joining(", ")));
            return admin.listConsumerGroupOffsets(GROUP_ID)
                    .partitionsToOffsetAndMetadata(GROUP_ID)
                    .get().entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().partition(), e -> e.getValue().offset()));
        }
    }
    
    private List<KeyValue<Long, byte[]>> consumeValues(final Optional<Integer> partition) {
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-out");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (final KafkaConsumer<Long, byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singleton(OUTPUT_TOPIC_NAME));
            final ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            final Iterable<ConsumerRecord<Long, byte[]>> iter;
            if (partition.isPresent()) {
                iter = records.records(new TopicPartition(OUTPUT_TOPIC_NAME, partition.get()));
            } else {
                iter = records.records(OUTPUT_TOPIC_NAME);
            }
            return StreamSupport
                    .stream(iter.spliterator(), false)
                    .map(rec -> new KeyValue<>(rec.key(), rec.value()))
                    .collect(Collectors.toList());
        }
    }

    @SuppressWarnings("unchecked")
    private List<StreamThread> streamThreads() {
        try {
            return (List<StreamThread>) threadsField.get(app);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
