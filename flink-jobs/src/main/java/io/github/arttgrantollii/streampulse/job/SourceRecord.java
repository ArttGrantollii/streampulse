package io.github.arttgrantollii.streampulse.job;

import io.github.arttgrantollii.streampulse.contract.EditEvent;
import io.github.arttgrantollii.streampulse.contract.ParseResult;

/**
 * What the Kafka source emits: a parsed raw record, either a valid event or a ready
 * DLQ record (as JSON). Exactly one of {@code event} and {@code dlqJson} is set.
 *
 * <p>Flat and Flink-POJO friendly on purpose: the job forbids Kryo
 * ({@code pipeline.generic-types: false}), so maps, byte arrays and interfaces must not
 * travel between operators. Parsing at the source (not in a later operator) also lets
 * 3c assign event-time watermarks in the source, per Kafka partition.
 *
 * @param event     the parsed event, or null if rejected
 * @param dlqReason the rejection reason code (metric label), or null
 * @param dlqJson   the DLQ record as JSON (schemas/streampulse/dlq-record.v1.json), or null
 */
public record SourceRecord(EditEvent event, String dlqReason, String dlqJson) {

    static SourceRecord from(ParseResult result) {
        // instanceof patterns, not a pattern switch: the job targets Java 17 (Flink's recommended JVM).
        if (result instanceof ParseResult.Valid valid) {
            return new SourceRecord(valid.event(), null, null);
        }
        ParseResult.Invalid invalid = (ParseResult.Invalid) result;
        return new SourceRecord(null, invalid.dlq().reason().code(), invalid.dlq().toJson());
    }

    boolean isValid() {
        return event != null;
    }
}
