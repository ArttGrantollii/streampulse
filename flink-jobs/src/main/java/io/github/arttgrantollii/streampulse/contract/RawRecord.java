package io.github.arttgrantollii.streampulse.contract;

import java.util.Map;

/**
 * One record of the raw topic exactly as Kafka delivered it: the upstream payload
 * (untouched, ADR-0005), the producer's {@code sp-*} headers and its coordinates.
 */
public record RawRecord(
        byte[] value,
        Map<String, byte[]> headers,
        String topic,
        int partition,
        long offset,
        long kafkaTimestampMs) {

    public RawRecord {
        headers = Map.copyOf(headers);
    }
}
