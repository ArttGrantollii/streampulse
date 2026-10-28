package io.github.arttgrantollii.streampulse.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

/**
 * The StreamPulse analytics job. Phase 3b: raw topic -> parse/validate -> DLQ, canaries
 * dropped. Event time (3c), dedup and lateness (3d) and the PostgreSQL sink (3e) are
 * added in the next subphases; until then valid events end in a discarding sink.
 *
 * <pre>
 * flink run -d -c io.github.arttgrantollii.streampulse.job.StreamPulseJob streampulse-flink-jobs.jar \
 *     [--bootstrap-servers kafka:9092] [--starting-offsets committed|earliest|latest]
 * </pre>
 */
public final class StreamPulseJob {

    private StreamPulseJob() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        StreamExecutionEnvironment env = environment();

        IngestionPipeline.Streams streams = IngestionPipeline.build(
                env.fromSource(IngestionPipeline.sourceBuilder(config).build(), WatermarkStrategy.noWatermarks(),
                                "raw-topic-source")
                        .uid("raw-topic-source"));

        streams.dlq().sinkTo(IngestionPipeline.dlqSink(config)).uid("dlq-sink").name("dlq-sink");
        streams.events().sinkTo(new DiscardingSink<>()).uid("edit-events-placeholder")
                .name("edit events (placeholder until 3c)");

        env.execute("streampulse-analytics " + config.jobVersion());
    }

    /**
     * The execution environment with Kryo forbidden: if any type would fall back to Kryo
     * (slow, and its state cannot evolve), the job fails at submission instead of silently.
     */
    public static StreamExecutionEnvironment environment() {
        Configuration conf = new Configuration();
        conf.set(PipelineOptions.GENERIC_TYPES, false);
        return StreamExecutionEnvironment.getExecutionEnvironment(conf);
    }
}
