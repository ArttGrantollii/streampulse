package io.github.arttgrantollii.streampulse.job;

import io.github.arttgrantollii.streampulse.contract.EditEvent;
import io.github.arttgrantollii.streampulse.contract.EditEventParser;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * The ingestion and data-quality stage (Phase 3b): raw Kafka records in, valid
 * {@link EditEvent}s and DLQ records out. Sources and sinks are built here but wired by the
 * caller, so tests run exactly this pipeline with test sinks.
 */
public final class IngestionPipeline {

    /** The two outputs of the stage. */
    public record Streams(DataStream<EditEvent> events, DataStream<String> dlq) {}

    private IngestionPipeline() {
    }

    public static KafkaSourceBuilder<SourceRecord> sourceBuilder(JobConfig config) {
        OffsetsInitializer start = switch (config.startingOffsets()) {
            case "earliest" -> OffsetsInitializer.earliest();
            case "latest" -> OffsetsInitializer.latest();
            default -> OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
        };
        return KafkaSource.<SourceRecord>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.rawTopic())
                .setGroupId(config.groupId())
                .setStartingOffsets(start)
                .setDeserializer(new RawRecordDeserializer(new EditEventParser(config.jobVersion())));
    }

    /** DLQ records, at-least-once: a duplicate DLQ entry is harmless, a lost one is not (ADR-0008). */
    public static KafkaSink<String> dlqSink(JobConfig config) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(config.dlqTopic())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    public static Streams build(DataStream<SourceRecord> parsed) {
        SingleOutputStreamOperator<EditEvent> events = parsed
                .process(new RouteParsedRecords())
                .uid("route-parsed-records")
                .name("route: valid / dlq / canary");
        return new Streams(events, events.getSideOutput(RouteParsedRecords.DLQ));
    }
}
