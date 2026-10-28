package io.github.arttgrantollii.streampulse.contract;

/** MediaWiki recent-change type ({@code type} field). Only EDIT and NEW count as edits (ADR-0001). */
public enum ChangeType {
    EDIT,
    NEW,
    LOG,
    CATEGORIZE,
    EXTERNAL,
    /** Missing, numeric (the schema allows "or a number") or a value we do not know yet. */
    UNKNOWN;

    static ChangeType fromWire(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        return switch (value) {
            case "edit" -> EDIT;
            case "new" -> NEW;
            case "log" -> LOG;
            case "categorize" -> CATEGORIZE;
            case "external" -> EXTERNAL;
            default -> UNKNOWN;
        };
    }

    public boolean isEdit() {
        return this == EDIT || this == NEW;
    }
}
