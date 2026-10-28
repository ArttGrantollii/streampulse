package io.github.arttgrantollii.streampulse.job;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.arttgrantollii.streampulse.contract.EditEvent;
import io.github.arttgrantollii.streampulse.contract.Fixtures;
import io.github.arttgrantollii.streampulse.contract.RawRecord;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The real ingestion pipeline on a Flink mini-cluster against the real Kafka broker of the
 * compose stack (STREAMPULSE_TEST_KAFKA, default localhost:9094). Uses its own topics, so it
 * can run while live ingestion is going on.
 */
@Tag("integration")
class IngestionPipelineIntegrationTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder().setNumberTaskManagers(1).setNumberSlotsPerTaskManager(2).build());

    private static final String BOOTSTRAP = System.getenv().getOrDefault("STREAMPULSE_TEST_KAFKA", "localhost:9094");
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String suffix = UUID.randomUUID().toString().substring(0, 8);
    private final String rawTopic = "streampulse.test." + suffix + ".raw.v1";
    private final String dlqTopic = "streampulse.test." + suffix + ".dlq.v1";
    private Admin admin;

    @BeforeEach
    void createTopics() throws Exception {
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP));
        admin.createTopics(List.of(new NewTopic(rawTopic, 3, (short) 1), new NewTopic(dlqTopic, 1, (short) 1)))
                .all().get(30, SECONDS);
    }

    @AfterEach
    void deleteTopics() throws Exception {
        admin.deleteTopics(List.of(rawTopic, dlqTopic)).all().get(30, SECONDS);
        admin.close();
    }

    @Test
    void validEventsFlowOnAndEveryRejectLandsInTheDlqWithItsReason() throws Exception {
        List<RawRecord> fixture = Fixtures.recording();
        long futureS = (Fixtures.INGEST_MS + 10 * 60_000) / 1000;
        try (KafkaProducer<byte[], byte[]> producer = producer()) {
            for (RawRecord record : fixture) {
                send(producer, record.value(), record.headers());
            }
            Map<String, byte[]> ingest = Map.of("sp-ingest-ts",
                    Long.toString(Fixtures.INGEST_MS).getBytes(StandardCharsets.UTF_8));
            send(producer, utf8("{\"meta\":{\"id\":\"truncated"), ingest);
            send(producer, utf8(Fixtures.edit().replace("\"id\":\"f473e767-1f26-4d08-88d4-48001c7585f9\",", "")), ingest);
            send(producer, utf8(Fixtures.edit().replace("1793188797", Long.toString(futureS))), ingest);
            send(producer, utf8(Fixtures.edit().replace("\"domain\":\"en.wikipedia.org\"", "\"domain\":\"canary\"")),
                    ingest);
            producer.flush();
        }

        JobConfig config = new JobConfig(BOOTSTRAP, rawTopic, dlqTopic, "it-" + suffix, "earliest", "it");
        StreamExecutionEnvironment env = StreamPulseJob.environment();
        env.setParallelism(2);
        IngestionPipeline.Streams streams = IngestionPipeline.build(env.fromSource(
                // Bounded: stop at the end offsets seen at start, so the job finishes by itself.
                IngestionPipeline.sourceBuilder(config).setBounded(OffsetsInitializer.latest()).build(),
                WatermarkStrategy.noWatermarks(), "raw-topic-source"));
        streams.dlq().sinkTo(IngestionPipeline.dlqSink(config));

        List<EditEvent> events = new ArrayList<>();
        try (CloseableIterator<EditEvent> results = streams.events().executeAndCollect()) {
            results.forEachRemaining(events::add);
        }

        Set<String> fixtureIds = fixture.stream()
                .map(r -> readTree(new String(r.value(), StandardCharsets.UTF_8)).at("/meta/id").asText())
                .collect(Collectors.toSet());
        assertThat(events).hasSize(120); // canary dropped, the 3 rejects diverted
        assertThat(events).extracting(EditEvent::eventId).containsExactlyInAnyOrderElementsOf(fixtureIds);

        List<JsonNode> dlq = readDlq(3);
        assertThat(dlq).extracting(n -> n.get("reason").asText())
                .containsExactlyInAnyOrder("invalid_json", "missing_identity", "impossible_time");
        assertThat(dlq).allSatisfy(n -> {
            assertThat(n.at("/source/topic").asText()).isEqualTo(rawTopic);
            assertThat(n.get("job_version").asText()).isEqualTo("it");
        });
    }

    private KafkaProducer<byte[], byte[]> producer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    private void send(KafkaProducer<byte[], byte[]> producer, byte[] value, Map<String, byte[]> headers) {
        List<Header> kafkaHeaders = new ArrayList<>();
        headers.forEach((k, v) -> kafkaHeaders.add(new RecordHeader(k, v)));
        producer.send(new ProducerRecord<>(rawTopic, null, null, null, value, kafkaHeaders));
    }

    private List<JsonNode> readDlq(int expected) {
        List<JsonNode> out = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class))) {
            TopicPartition partition = new TopicPartition(dlqTopic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.currentTimeMillis() + 30_000;
            while (out.size() < expected && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(500))) {
                    out.add(readTree(new String(record.value(), StandardCharsets.UTF_8)));
                }
            }
        }
        return out;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static JsonNode readTree(String json) {
        try {
            return JSON.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
