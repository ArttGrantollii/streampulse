package io.github.arttgrantollii.streampulse.smoke;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.ParameterTool;

/**
 * Phase 1 infrastructure smoke test.
 *
 * <p>Proves that the pinned versions work together end to end: the Kafka connector
 * (5.0.0-2.2) reads from the broker (4.2.2) inside a Flink 2.2.1 cluster, keyed state
 * lives in the RocksDB backend, and checkpoints are written to the shared volume.
 * scripts/smoke_test.sh produces N records and expects the job to count exactly N.
 */
public final class SmokeJob {

    private SmokeJob() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrapServers = params.get("bootstrap-servers", "kafka:9092");
        String topic = params.get("topic", "streampulse.smoke.v1");
        String groupId = params.get("group-id", "streampulse-smoke");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                // The smoke topic is recreated for every run, so reading from the start is exact.
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Explicit uids keep state mapped to operators if the job graph changes later.
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-smoke-source")
                .uid("kafka-smoke-source")
                // A single constant key routes every record to one counter.
                .keyBy(value -> CountRecords.SINGLE_KEY)
                .process(new CountRecords())
                .uid("count-records")
                .name("count-records")
                .sinkTo(new DiscardingSink<>())
                .name("discard");

        env.execute("streampulse-smoke");
    }
}
