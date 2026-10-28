package io.github.arttgrantollii.streampulse.contract;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.function.LongSupplier;

/**
 * Turns one raw {@code recentchange} payload into an {@link EditEvent} or a {@link DlqRecord}.
 *
 * <p>The single place that touches upstream JSON (ADR-0005). Rules:
 * <ul>
 *   <li>Unknown fields are ignored; upstream already sends fields its schema does not list.</li>
 *   <li>Missing optional fields become null; only identity and time are required.</li>
 *   <li>A field we use with the wrong JSON type is rejected, never coerced.</li>
 *   <li>Sanity bounds on event time, relative to ingest time (the producer's receive time,
 *       preserved by replay, so the result is the same on every run):
 *       <ul>
 *         <li>more than {@value #FUTURE_TOLERANCE_MS} ms in the future: rejected for every type,
 *             because one future timestamp would push watermarks ahead and make normal
 *             events look late;</li>
 *         <li>more than 31 days in the past: rejected for edits only. Old timestamps are
 *             legitimate on other types (a Commons upload log event from 2005 was observed,
 *             Research 0002 §1.5) and cannot move a max-based watermark.</li>
 *       </ul>
 *   </li>
 * </ul>
 * Thread-safe; serializable so Flink can ship it to task managers.
 */
public final class EditEventParser implements Serializable {

    private static final long serialVersionUID = 1L;

    static final long FUTURE_TOLERANCE_MS = 5 * 60 * 1000L;
    static final long MAX_EDIT_AGE_MS = 31L * 24 * 60 * 60 * 1000;
    static final String INGEST_HEADER = "sp-ingest-ts";

    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private final String jobVersion;
    private final SerializableClock clock;

    /** A clock that survives Java serialization (Flink serializes operators and their fields). */
    @FunctionalInterface
    public interface SerializableClock extends LongSupplier, Serializable {}

    public EditEventParser(String jobVersion, SerializableClock clock) {
        this.jobVersion = jobVersion;
        this.clock = clock;
    }

    public EditEventParser(String jobVersion) {
        this(jobVersion, System::currentTimeMillis);
    }

    public ParseResult parse(RawRecord raw) {
        try {
            return new ParseResult.Valid(toEvent(raw));
        } catch (Rejected r) {
            return new ParseResult.Invalid(DlqRecord.of(raw, r.reason, r.getMessage(), jobVersion, clock.getAsLong()));
        }
    }

    private EditEvent toEvent(RawRecord raw) throws Rejected {
        JsonNode root = readJson(raw.value());

        JsonNode meta = root.path("meta");
        if (meta.isMissingNode() || meta.isNull()) {
            throw new Rejected(RejectReason.MISSING_IDENTITY, "meta is missing");
        }
        if (!meta.isObject()) {
            throw new Rejected(RejectReason.INCOMPATIBLE_TYPE, "meta must be an object");
        }
        String eventId = optionalString(meta, "id", "meta.id");
        if (eventId == null || eventId.isBlank()) {
            throw new Rejected(RejectReason.MISSING_IDENTITY, "meta.id is missing or blank");
        }

        ChangeType type;
        String rawType;
        JsonNode typeNode = root.path("type");
        if (typeNode.isTextual()) {
            rawType = typeNode.asText();
            type = ChangeType.fromWire(rawType);
        } else if (typeNode.isNumber()) {
            rawType = typeNode.asText(); // the schema allows "or a number"
            type = ChangeType.UNKNOWN;
        } else if (typeNode.isMissingNode() || typeNode.isNull()) {
            rawType = null;
            type = ChangeType.UNKNOWN;
        } else {
            throw new Rejected(RejectReason.INCOMPATIBLE_TYPE, "type must be a string or a number");
        }

        String wiki = optionalString(root, "wiki", "wiki");
        String title = optionalString(root, "title", "title");
        if (type.isEdit() && (wiki == null || title == null)) {
            throw new Rejected(RejectReason.MISSING_IDENTITY, "an edit needs wiki and title");
        }

        Long timestampS = optionalLong(root, "timestamp", "timestamp");
        Long emittedAtMs = optionalInstantMs(meta, "dt", "meta.dt");
        long eventTimeMs;
        EventTimeSource source;
        if (timestampS != null) {
            try {
                eventTimeMs = Math.multiplyExact(timestampS, 1000L);
            } catch (ArithmeticException e) {
                throw new Rejected(RejectReason.INCOMPATIBLE_TYPE, "timestamp is out of range: " + timestampS);
            }
            source = EventTimeSource.TIMESTAMP;
        } else if (emittedAtMs != null) {
            eventTimeMs = emittedAtMs;
            source = EventTimeSource.META_DT;
        } else {
            throw new Rejected(RejectReason.MISSING_TIME, "neither timestamp nor meta.dt");
        }

        long ingestTimeMs = ingestTime(raw);
        if (eventTimeMs > ingestTimeMs + FUTURE_TOLERANCE_MS) {
            throw new Rejected(RejectReason.IMPOSSIBLE_TIME,
                    "event time " + Instant.ofEpochMilli(eventTimeMs) + " is more than 5 min after ingest "
                            + Instant.ofEpochMilli(ingestTimeMs));
        }
        if (type.isEdit() && eventTimeMs < ingestTimeMs - MAX_EDIT_AGE_MS) {
            throw new Rejected(RejectReason.IMPOSSIBLE_TIME,
                    "edit event time " + Instant.ofEpochMilli(eventTimeMs) + " is more than 31 days before ingest "
                            + Instant.ofEpochMilli(ingestTimeMs));
        }

        JsonNode revision = optionalObject(root, "revision");
        JsonNode length = optionalObject(root, "length");
        Long namespace = optionalLong(root, "namespace", "namespace");
        Long lengthNew = length == null ? null : optionalLong(length, "new", "length.new");
        Long lengthOld = length == null ? null : optionalLong(length, "old", "length.old");

        return new EditEvent(
                eventId,
                eventTimeMs,
                source,
                emittedAtMs,
                ingestTimeMs,
                type,
                rawType,
                wiki,
                optionalString(root, "server_name", "server_name"),
                toInt(namespace, "namespace"),
                title,
                optionalBoolean(root, "bot", "bot"),
                optionalString(root, "user", "user"),
                revision == null ? null : optionalLong(revision, "new", "revision.new"),
                revision == null ? null : optionalLong(revision, "old", "revision.old"),
                toInt(lengthNew, "length.new"),
                toInt(lengthOld, "length.old"),
                optionalLong(root, "id", "id"),
                "canary".equals(meta.path("domain").asText(null)),
                optionalString(root, "$schema", "$schema"),
                raw.partition(),
                raw.offset());
    }

    private static JsonNode readJson(byte[] value) throws Rejected {
        JsonNode root;
        try {
            root = JSON.readTree(value);
        } catch (JsonProcessingException e) {
            throw new Rejected(RejectReason.INVALID_JSON, "not valid JSON: " + e.getOriginalMessage());
        } catch (IOException e) {
            throw new Rejected(RejectReason.INVALID_JSON, "unreadable payload: " + e.getMessage());
        }
        if (root == null || !root.isObject()) {
            throw new Rejected(RejectReason.INVALID_JSON, "payload is not a JSON object");
        }
        return root;
    }

    private static long ingestTime(RawRecord raw) {
        byte[] header = raw.headers().get(INGEST_HEADER);
        if (header != null) {
            try {
                return Long.parseLong(new String(header, StandardCharsets.UTF_8));
            } catch (NumberFormatException e) {
                // fall through: an unusable header is not the event's fault
            }
        }
        return raw.kafkaTimestampMs();
    }

    // --- typed, null-tolerant field access: absent or JSON null -> null, wrong type -> rejected ---

    private static boolean absent(JsonNode node) {
        return node.isMissingNode() || node.isNull();
    }

    private static String optionalString(JsonNode parent, String field, String path) throws Rejected {
        JsonNode node = parent.path(field);
        if (absent(node)) {
            return null;
        }
        if (!node.isTextual()) {
            throw new Rejected(RejectReason.INCOMPATIBLE_TYPE, path + " must be a string");
        }
        return node.asText();
    }

    private static Long optionalLong(JsonNode parent, String field, String path) throws Rejected {
        JsonNode node = parent.path(field);
        if (absent(node)) {
            return null;
        }
        if (!node.isIntegralNumber() || !node.canConvertToLong()) {
            throw new Rejected(RejectReason.INCOMPATIBLE_TYPE, path + " must be an integer");
        }
        return node.asLong();
    }

    private static Boolean optionalBoolean(JsonNode parent, String field, String path) throws Rejected {
        JsonNode node = parent.path(field);
        if (absent(node)) {
            return null;
        }
        if (!node.isBoolean()) {
            throw new Rejected(RejectReason.INCOMPATIBLE_TYPE, path + " must be a boolean");
        }
        return node.asBoolean();
    }

    private static JsonNode optionalObject(JsonNode parent, String field) throws Rejected {
        JsonNode node = parent.path(field);
        if (absent(node)) {
            return null;
        }
        if (!node.isObject()) {
            throw new Rejected(RejectReason.INCOMPATIBLE_TYPE, field + " must be an object");
        }
        return node;
    }

    private static Long optionalInstantMs(JsonNode parent, String field, String path) throws Rejected {
        String text = optionalString(parent, field, path);
        if (text == null) {
            return null;
        }
        try {
            return OffsetDateTime.parse(text).toInstant().toEpochMilli(); // 'Z' or an explicit offset
        } catch (DateTimeParseException e) {
            throw new Rejected(RejectReason.INCOMPATIBLE_TYPE, path + " is not an ISO-8601 instant: " + text);
        }
    }

    private static Integer toInt(Long value, String path) throws Rejected {
        if (value == null) {
            return null;
        }
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new Rejected(RejectReason.INCOMPATIBLE_TYPE, path + " is out of range: " + value);
        }
        return value.intValue();
    }

    /** Internal control flow: carries the reject reason up to {@link #parse}. */
    private static final class Rejected extends Exception {
        private static final long serialVersionUID = 1L;
        private final RejectReason reason;

        Rejected(RejectReason reason, String detail) {
            super(detail, null, false, false); // no stack trace: rejection is an expected outcome
            this.reason = reason;
        }
    }
}
