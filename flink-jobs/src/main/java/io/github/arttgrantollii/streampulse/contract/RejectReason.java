package io.github.arttgrantollii.streampulse.contract;

/** Why a raw record went to the DLQ (docs/ARCHITECTURE.md §13). The codes are part of the DLQ contract. */
public enum RejectReason {
    /** Not parseable as a JSON object (malformed, truncated, not UTF-8, trailing garbage, not an object). */
    INVALID_JSON("invalid_json"),
    /** No usable {@code meta.id}, or an edit without {@code wiki} or {@code title}. */
    MISSING_IDENTITY("missing_identity"),
    /** Neither {@code timestamp} nor {@code meta.dt}. */
    MISSING_TIME("missing_time"),
    /** Event time outside the sanity bounds relative to ingest time (see EditEventParser). */
    IMPOSSIBLE_TIME("impossible_time"),
    /** A field we use has the wrong JSON type, e.g. {@code timestamp} as a string. */
    INCOMPATIBLE_TYPE("incompatible_type");

    private final String code;

    RejectReason(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }
}
