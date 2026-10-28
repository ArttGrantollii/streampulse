package io.github.arttgrantollii.streampulse.contract;

/** Outcome of parsing one raw record: a valid event, or a DLQ record explaining why not. */
public sealed interface ParseResult {

    record Valid(EditEvent event) implements ParseResult {}

    record Invalid(DlqRecord dlq) implements ParseResult {}
}
