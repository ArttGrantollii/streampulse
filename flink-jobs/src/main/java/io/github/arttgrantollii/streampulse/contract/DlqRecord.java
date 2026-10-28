package io.github.arttgrantollii.streampulse.contract;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

/**
 * A rejected raw record with everything needed to debug it, written to
 * {@code wikimedia.recentchange.dlq.v1}. Contract: schemas/streampulse/dlq-record.v1.json
 * (checked by DlqRecordTest). Changes within v1 are additive only.
 */
public record DlqRecord(
        RejectReason reason,
        String detail,
        String rawValue,
        int rawValueBytes,
        boolean rawValueTruncated,
        String topic,
        int partition,
        long offset,
        long kafkaTimestampMs,
        Map<String, String> headers,
        String jobVersion,
        long detectedAtMs) {

    public static final int SCHEMA_VERSION = 1;
    /** Keeps DLQ records well under Kafka's default 1 MB message limit. */
    public static final int MAX_RAW_VALUE_CHARS = 65_536;

    private static final ObjectMapper JSON = new ObjectMapper();

    static DlqRecord of(RawRecord raw, RejectReason reason, String detail, String jobVersion, long detectedAtMs) {
        // Decoding with replacement never fails, so even non-UTF-8 payloads are preserved readably.
        String value = new String(raw.value(), StandardCharsets.UTF_8);
        boolean truncated = value.length() > MAX_RAW_VALUE_CHARS;
        Map<String, String> headers = new TreeMap<>();
        raw.headers().forEach((name, bytes) -> headers.put(name, new String(bytes, StandardCharsets.UTF_8)));
        return new DlqRecord(
                reason,
                detail,
                truncated ? value.substring(0, MAX_RAW_VALUE_CHARS) : value,
                raw.value().length,
                truncated,
                raw.topic(),
                raw.partition(),
                raw.offset(),
                raw.kafkaTimestampMs(),
                headers,
                jobVersion,
                detectedAtMs);
    }

    public String toJson() {
        ObjectNode node = JSON.createObjectNode();
        node.put("schema_version", SCHEMA_VERSION);
        node.put("reason", reason.code());
        node.put("detail", detail);
        node.put("raw_value", rawValue);
        node.put("raw_value_bytes", rawValueBytes);
        node.put("raw_value_truncated", rawValueTruncated);
        ObjectNode source = node.putObject("source");
        source.put("topic", topic);
        source.put("partition", partition);
        source.put("offset", offset);
        source.put("kafka_timestamp_ms", kafkaTimestampMs);
        ObjectNode headerNode = source.putObject("headers");
        headers.forEach(headerNode::put);
        node.put("job_version", jobVersion);
        node.put("detected_at_ms", detectedAtMs);
        try {
            return JSON.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("a tree of strings and numbers always serializes", e);
        }
    }
}
