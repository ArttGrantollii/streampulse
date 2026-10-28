package io.github.arttgrantollii.streampulse.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.arttgrantollii.streampulse.contract.EditEventParser;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class RawRecordDeserializerTest {

    private static final long INGEST_MS = 1_793_188_800_000L;
    private final RawRecordDeserializer deserializer = new RawRecordDeserializer(new EditEventParser("t", () -> 1L));

    private static ConsumerRecord<byte[], byte[]> record(byte[] value) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("sp-ingest-ts", Long.toString(INGEST_MS).getBytes(StandardCharsets.UTF_8));
        headers.add("sp-mode", "live".getBytes(StandardCharsets.UTF_8));
        return new ConsumerRecord<>("wikimedia.recentchange.raw.v1", 2, 77L, INGEST_MS + 3, TimestampType.CREATE_TIME,
                0, value == null ? 0 : value.length, null, value, headers, Optional.empty());
    }

    private SourceRecord deserialize(ConsumerRecord<byte[], byte[]> record) {
        List<SourceRecord> out = new ArrayList<>();
        deserializer.deserialize(record, new Collector<>() {
            @Override
            public void collect(SourceRecord value) {
                out.add(value);
            }

            @Override
            public void close() {
            }
        });
        assertThat(out).hasSize(1); // exactly one output per input, valid or not
        return out.get(0);
    }

    @Test
    void validPayloadBecomesAnEventWithItsKafkaCoordinates() {
        String edit = """
                {"meta":{"id":"abc","dt":"2026-10-28T11:59:59Z"},"type":"edit","wiki":"enwiki",
                 "title":"Kafka","timestamp":1793188798}""";
        SourceRecord result = deserialize(record(edit.getBytes(StandardCharsets.UTF_8)));
        assertThat(result.isValid()).isTrue();
        assertThat(result.event().ingestTimeMs()).isEqualTo(INGEST_MS);
        assertThat(result.event().sourcePartition()).isEqualTo(2);
        assertThat(result.event().sourceOffset()).isEqualTo(77L);
        assertThat(result.dlqJson()).isNull();
    }

    @Test
    void malformedPayloadBecomesADlqRecordInsteadOfAnException() throws Exception {
        SourceRecord result = deserialize(record("{oops".getBytes(StandardCharsets.UTF_8)));
        assertThat(result.isValid()).isFalse();
        assertThat(result.dlqReason()).isEqualTo("invalid_json");
        JsonNode dlq = new ObjectMapper().readTree(result.dlqJson());
        assertThat(dlq.at("/source/topic").asText()).isEqualTo("wikimedia.recentchange.raw.v1");
        assertThat(dlq.at("/source/partition").asInt()).isEqualTo(2);
        assertThat(dlq.at("/source/offset").asLong()).isEqualTo(77L);
        assertThat(dlq.at("/source/headers/sp-mode").asText()).isEqualTo("live");
    }

    @Test
    void tombstoneWithNullValueIsRejectedNotACrash() {
        assertThat(deserialize(record(null)).dlqReason()).isEqualTo("invalid_json");
    }

    @Test
    void producedTypeIsAFlinkPojoSoTheJobRunsWithKryoDisabled() {
        assertThat(deserializer.getProducedType()).isInstanceOf(PojoTypeInfo.class);
    }
}
