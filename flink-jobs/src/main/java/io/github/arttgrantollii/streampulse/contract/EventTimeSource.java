package io.github.arttgrantollii.streampulse.contract;

/** Which upstream field the event time came from (ADR-0006). */
public enum EventTimeSource {
    /** {@code timestamp}: when the change was saved (rc_timestamp), whole seconds. The normal case. */
    TIMESTAMP,
    /** {@code meta.dt}: when Wikimedia emitted the event; used only if {@code timestamp} is absent. */
    META_DT
}
