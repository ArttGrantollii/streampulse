package io.github.arttgrantollii.streampulse.contract;

import static io.github.arttgrantollii.streampulse.contract.Fixtures.INGEST_MS;
import static io.github.arttgrantollii.streampulse.contract.Fixtures.edit;
import static io.github.arttgrantollii.streampulse.contract.Fixtures.raw;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class EditEventParserTest {

    private final EditEventParser parser = new EditEventParser("test", () -> 999L);

    private EditEvent valid(RawRecord raw) {
        ParseResult result = parser.parse(raw);
        assertThat(result).as("expected a valid event").isInstanceOf(ParseResult.Valid.class);
        return ((ParseResult.Valid) result).event();
    }

    private DlqRecord rejected(RawRecord raw, RejectReason reason) {
        ParseResult result = parser.parse(raw);
        assertThat(result).as("expected a rejection").isInstanceOf(ParseResult.Invalid.class);
        DlqRecord dlq = ((ParseResult.Invalid) result).dlq();
        assertThat(dlq.reason()).as(dlq.detail()).isEqualTo(reason);
        return dlq;
    }

    // --- real data ---------------------------------------------------------------------------

    @Test
    void everyEventOfTheRealFixtureParses() {
        List<RawRecord> recording = Fixtures.recording();
        assertThat(recording).hasSize(120);
        for (RawRecord raw : recording) {
            ParseResult result = parser.parse(raw);
            assertThat(result)
                    .as(() -> result instanceof ParseResult.Invalid i ? i.dlq().detail() : "")
                    .isInstanceOf(ParseResult.Valid.class);
        }
    }

    @Test
    void fixtureEventsKeepTheirMeaning() {
        List<EditEvent> events = Fixtures.recording().stream()
                .map(parser::parse).map(r -> ((ParseResult.Valid) r).event()).toList();
        assertThat(events).filteredOn(e -> e.type().isEdit()).hasSize(58); // 51 edit + 7 new
        assertThat(events).allSatisfy(e -> {
            assertThat(e.eventTimeSource()).isEqualTo(EventTimeSource.TIMESTAMP);
            assertThat(e.eventTimeMs() % 1000).isZero(); // `timestamp` has whole-second resolution
            assertThat(e.emittedAtMs()).isGreaterThanOrEqualTo(e.eventTimeMs()); // meta.dt is emission time
        });
        assertThat(events).filteredOn(e -> e.type() == ChangeType.NEW)
                .allSatisfy(e -> assertThat(e.revisionOld()).isNull());
    }

    // --- field mapping -------------------------------------------------------------------------

    @Test
    void mapsEveryFieldOfAnEdit() {
        EditEvent e = valid(raw(edit()));
        assertThat(e.eventId()).isEqualTo("f473e767-1f26-4d08-88d4-48001c7585f9");
        assertThat(e.eventTimeMs()).isEqualTo(1_793_188_797_000L);
        assertThat(e.emittedAtMs()).isEqualTo(1_793_188_798_500L);
        assertThat(e.ingestTimeMs()).isEqualTo(INGEST_MS);
        assertThat(e.type()).isEqualTo(ChangeType.EDIT);
        assertThat(e.wiki()).isEqualTo("enwiki");
        assertThat(e.serverName()).isEqualTo("en.wikipedia.org");
        assertThat(e.namespace()).isZero();
        assertThat(e.title()).isEqualTo("Apache Kafka");
        assertThat(e.bot()).isFalse();
        assertThat(e.user()).isEqualTo("Example");
        assertThat(e.revisionNew()).isEqualTo(101L);
        assertThat(e.revisionOld()).isEqualTo(100L);
        assertThat(e.bytesDelta()).isEqualTo(120);
        assertThat(e.rcId()).isEqualTo(123_456_789L);
        assertThat(e.canary()).isFalse();
        assertThat(e.sourceSchema()).isEqualTo("/mediawiki/recentchange/1.0.0");
        assertThat(e.sourcePartition()).isEqualTo(1);
        assertThat(e.sourceOffset()).isEqualTo(42L);
    }

    @Test
    void unknownFieldsAreIgnored() {
        // Upstream already sends fields its schema does not list (title_url, notify_url, meta.offset).
        String withExtras = edit().replace("\"minor\":false", "\"minor\":false,\"brand_new_field\":{\"x\":[1,2]}");
        assertThat(valid(raw(withExtras)).title()).isEqualTo("Apache Kafka");
    }

    @Test
    void missingOptionalFieldsBecomeNull() {
        String minimal = """
                {"meta":{"id":"abc","dt":"2026-10-28T11:59:59Z"},"type":"log","timestamp":1793188798}""";
        EditEvent e = valid(raw(minimal));
        assertThat(e.wiki()).isNull();
        assertThat(e.bot()).isNull();
        assertThat(e.revisionNew()).isNull();
        assertThat(e.bytesDelta()).isNull();
        assertThat(e.namespace()).isNull();
    }

    @Test
    void jsonNullIsTreatedAsAbsent() {
        // rc `id` is null on some log events (Research 0001).
        String logEvent = """
                {"meta":{"id":"abc","dt":"2026-10-28T11:59:59Z"},"type":"log","timestamp":1793188798,"id":null}""";
        assertThat(valid(raw(logEvent)).rcId()).isNull();
    }

    @Test
    void numericOrUnknownTypeIsKeptAsUnknown() {
        assertThat(valid(raw(edit().replace("\"type\":\"edit\"", "\"type\":3"))).type()).isEqualTo(ChangeType.UNKNOWN);
        EditEvent future = valid(raw(edit().replace("\"type\":\"edit\"", "\"type\":\"suggested-edit\"")));
        assertThat(future.type()).isEqualTo(ChangeType.UNKNOWN);
        assertThat(future.rawType()).isEqualTo("suggested-edit");
    }

    @Test
    void canaryEventsAreFlaggedNotRejected() {
        assertThat(valid(raw(edit().replace("\"domain\":\"en.wikipedia.org\"", "\"domain\":\"canary\""))).canary())
                .isTrue();
    }

    // --- event time ----------------------------------------------------------------------------

    @Test
    void fallsBackToMetaDtWhenTimestampIsAbsent() {
        EditEvent e = valid(raw(edit().replace("\"timestamp\":1793188797,", "")));
        assertThat(e.eventTimeSource()).isEqualTo(EventTimeSource.META_DT);
        assertThat(e.eventTimeMs()).isEqualTo(1_793_188_798_500L);
    }

    @Test
    void metaDtWithAnExplicitOffsetIsAccepted() {
        EditEvent e = valid(raw(edit().replace("2026-10-28T11:59:58.500Z", "2026-10-28T13:59:58.500+02:00")));
        assertThat(e.emittedAtMs()).isEqualTo(1_793_188_798_500L);
    }

    @Test
    void ingestTimeFallsBackToTheKafkaTimestampWithoutTheHeader() {
        RawRecord noHeader = new RawRecord(edit().getBytes(StandardCharsets.UTF_8), Map.of(), "t", 0, 0, 1_793_188_800_123L);
        assertThat(valid(noHeader).ingestTimeMs()).isEqualTo(1_793_188_800_123L);
    }

    @Test
    void oldTimestampIsValidOnLogEvents() {
        // Observed 2026-10-28: a Commons upload log event emitted today with timestamp 2005-01-23.
        String upload = """
                {"meta":{"id":"abc","dt":"2026-10-28T11:59:59Z"},"type":"log","log_type":"upload",
                 "wiki":"commonswiki","title":"File:Takt2trans.png","timestamp":1106456492}""";
        assertThat(valid(raw(upload)).eventTimeMs()).isEqualTo(1_106_456_492_000L);
    }

    @Test
    void editMoreThan31DaysOldIsImpossible() {
        long thirtyTwoDaysAgoS = (INGEST_MS - 32L * 86_400_000) / 1000;
        rejected(raw(edit().replace("1793188797", Long.toString(thirtyTwoDaysAgoS))), RejectReason.IMPOSSIBLE_TIME);
    }

    @Test
    void editThirtyDaysOldIsStillValid() {
        long thirtyDaysAgoS = (INGEST_MS - 30L * 86_400_000) / 1000;
        valid(raw(edit().replace("1793188797", Long.toString(thirtyDaysAgoS))));
    }

    @Test
    void futureTimestampIsImpossibleForEveryType() {
        // One future timestamp would push watermarks ahead and make normal events look late.
        long sixMinutesAheadS = (INGEST_MS + 6 * 60_000) / 1000;
        String futureLog = """
                {"meta":{"id":"abc","dt":"2026-10-28T11:59:59Z"},"type":"log","timestamp":%d}"""
                .formatted(sixMinutesAheadS);
        rejected(raw(futureLog), RejectReason.IMPOSSIBLE_TIME);
    }

    @Test
    void slightlyAheadOfIngestIsTolerated() {
        // Clock offsets of seconds are normal (Research 0001/0002).
        long fourMinutesAheadS = (INGEST_MS + 4 * 60_000) / 1000;
        valid(raw(edit().replace("1793188797", Long.toString(fourMinutesAheadS))));
    }

    // --- rejections ----------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
        "truncated JSON              | {\"meta\":{\"id\":\"a\"",
        "trailing garbage            | {\"meta\":{\"id\":\"a\"}} trailing",
        "array instead of object     | [1,2,3]",
        "plain string                | \"edit\"",
        "empty payload               | ''",
    })
    void notAJsonObjectIsInvalidJson(String name, String payload) {
        rejected(raw(payload), RejectReason.INVALID_JSON);
    }

    @Test
    void nonUtf8BytesAreInvalidJsonAndStillReadableInTheDlq() {
        DlqRecord dlq = rejected(raw(new byte[] {'{', (byte) 0xC3, (byte) 0x28, '}'}), RejectReason.INVALID_JSON);
        assertThat(dlq.rawValueBytes()).isEqualTo(4);
        assertThat(dlq.rawValue()).startsWith("{");
    }

    @Test
    void missingMetaIdIsMissingIdentity() {
        rejected(raw(edit().replace("\"id\":\"f473e767-1f26-4d08-88d4-48001c7585f9\",", "")),
                RejectReason.MISSING_IDENTITY);
        rejected(raw(edit().replace("f473e767-1f26-4d08-88d4-48001c7585f9", "  ")), RejectReason.MISSING_IDENTITY);
    }

    @Test
    void missingMetaIsMissingIdentity() {
        rejected(raw("{\"type\":\"edit\",\"timestamp\":1793188797}"), RejectReason.MISSING_IDENTITY);
    }

    @Test
    void editWithoutWikiOrTitleIsMissingIdentity() {
        rejected(raw(edit().replace("\"wiki\":\"enwiki\",", "")), RejectReason.MISSING_IDENTITY);
        rejected(raw(edit().replace("\"title\":\"Apache Kafka\",", "")), RejectReason.MISSING_IDENTITY);
    }

    @Test
    void neitherTimestampNorMetaDtIsMissingTime() {
        String noTime = edit().replace("\"timestamp\":1793188797,", "").replace("\"dt\":\"2026-10-28T11:59:58.500Z\",", "");
        rejected(raw(noTime), RejectReason.MISSING_TIME);
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
        "timestamp as string   | \"timestamp\":1793188797        | \"timestamp\":\"1793188797\"",
        "timestamp as float    | \"timestamp\":1793188797        | \"timestamp\":1793188797.5",
        "bot as string         | \"bot\":false                   | \"bot\":\"false\"",
        "title as number       | \"title\":\"Apache Kafka\"      | \"title\":42",
        "namespace as string   | \"namespace\":0                 | \"namespace\":\"0\"",
        "revision as array     | \"revision\":{\"old\":100,\"new\":101} | \"revision\":[100,101]",
        "meta.dt unparseable   | \"dt\":\"2026-10-28T11:59:58.500Z\" | \"dt\":\"yesterday\"",
        "meta as string        | \"meta\":{                      | \"meta\":\"x\",\"ignored\":{",
        "timestamp overflows   | \"timestamp\":1793188797        | \"timestamp\":9223372036854775807",
    })
    void wrongTypeOnAFieldWeUseIsIncompatibleType(String name, String from, String to) {
        String payload = edit().replace(from, to);
        assertThat(payload).as("test setup must change the payload").isNotEqualTo(edit());
        DlqRecord dlq = rejected(raw(payload), RejectReason.INCOMPATIBLE_TYPE);
        assertThat(dlq.detail()).isNotBlank();
    }

    @Test
    void rejectionKeepsFullDebuggingContext() {
        DlqRecord dlq = rejected(raw("{broken"), RejectReason.INVALID_JSON);
        assertThat(dlq.rawValue()).isEqualTo("{broken");
        assertThat(dlq.topic()).isEqualTo("wikimedia.recentchange.raw.v1");
        assertThat(dlq.partition()).isEqualTo(1);
        assertThat(dlq.offset()).isEqualTo(42L);
        assertThat(dlq.kafkaTimestampMs()).isEqualTo(INGEST_MS + 7);
        assertThat(dlq.headers()).containsEntry("sp-ingest-ts", Long.toString(INGEST_MS));
        assertThat(dlq.jobVersion()).isEqualTo("test");
        assertThat(dlq.detectedAtMs()).isEqualTo(999L);
    }
}
