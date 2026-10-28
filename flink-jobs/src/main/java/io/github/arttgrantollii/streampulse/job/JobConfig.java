package io.github.arttgrantollii.streampulse.job;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;
import org.apache.flink.util.ParameterTool;

/**
 * Job settings, from {@code flink run ... --key value} arguments with safe defaults.
 *
 * @param startingOffsets where to start when no checkpoint/savepoint is restored:
 *     {@code committed} (the group's committed offsets, else earliest), {@code earliest}
 *     or {@code latest}. On restore, Flink always resumes from the checkpointed offsets.
 */
public record JobConfig(
        String bootstrapServers,
        String rawTopic,
        String dlqTopic,
        String groupId,
        String startingOffsets,
        String jobVersion) {

    static final String DEFAULT_RAW_TOPIC = "wikimedia.recentchange.raw.v1";
    static final String DEFAULT_DLQ_TOPIC = "wikimedia.recentchange.dlq.v1";

    public static JobConfig fromArgs(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        String startingOffsets = params.get("starting-offsets", "committed");
        if (!startingOffsets.matches("committed|earliest|latest")) {
            throw new IllegalArgumentException("--starting-offsets must be committed, earliest or latest");
        }
        return new JobConfig(
                params.get("bootstrap-servers", "kafka:9092"),
                params.get("raw-topic", DEFAULT_RAW_TOPIC),
                params.get("dlq-topic", DEFAULT_DLQ_TOPIC),
                params.get("group-id", "streampulse-analytics"),
                startingOffsets,
                buildVersion());
    }

    /** The Maven project version, written into the jar by resource filtering. */
    static String buildVersion() {
        try (InputStream in = JobConfig.class.getResourceAsStream("/streampulse-job.properties")) {
            if (in == null) {
                return "unknown";
            }
            Properties properties = new Properties();
            properties.load(in);
            return properties.getProperty("version", "unknown");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
