package io.github.arttgrantollii.streampulse.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** Test helpers: raw records from the shared fixture, and hand-made payloads. */
public final class Fixtures {

    /** Tests run with flink-jobs/ as the working directory (Maven basedir). */
    public static final Path REPO_ROOT = Path.of("..").toAbsolutePath().normalize();
    public static final Path RECENTCHANGE_120 = REPO_ROOT.resolve("sample-data/fixtures/recentchange-120.ndjson");

    /** 2026-10-28T12:00:00Z: the ingest time used by hand-made records. */
    public static final long INGEST_MS = 1_793_188_800_000L;

    private static final ObjectMapper JSON = new ObjectMapper();

    private Fixtures() {
    }

    /** The committed recording as raw records, with the headers the producer would have set. */
    public static List<RawRecord> recording() {
        try {
            List<String> lines = Files.readAllLines(RECENTCHANGE_120, StandardCharsets.UTF_8);
            return java.util.stream.IntStream.range(0, lines.size()).mapToObj(i -> {
                JsonNode row = readTree(lines.get(i));
                long recvMs = row.get("recv_ms").asLong();
                return new RawRecord(
                        row.get("data").asText().getBytes(StandardCharsets.UTF_8),
                        Map.of("sp-ingest-ts", Long.toString(recvMs).getBytes(StandardCharsets.UTF_8),
                                "sp-mode", "replay".getBytes(StandardCharsets.UTF_8)),
                        "wikimedia.recentchange.raw.v1", i % 3, i, recvMs);
            }).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static RawRecord raw(String payload) {
        return raw(payload.getBytes(StandardCharsets.UTF_8));
    }

    static RawRecord raw(byte[] payload) {
        return new RawRecord(payload,
                Map.of("sp-ingest-ts", Long.toString(INGEST_MS).getBytes(StandardCharsets.UTF_8)),
                "wikimedia.recentchange.raw.v1", 1, 42, INGEST_MS + 7);
    }

    /** A valid edit payload; tests replace or remove parts of it as JSON text. */
    public static String edit() {
        return """
                {"$schema":"/mediawiki/recentchange/1.0.0",
                 "meta":{"id":"f473e767-1f26-4d08-88d4-48001c7585f9","dt":"2026-10-28T11:59:58.500Z",
                         "domain":"en.wikipedia.org","stream":"mediawiki.recentchange"},
                 "type":"edit","namespace":0,"title":"Apache Kafka","wiki":"enwiki",
                 "server_name":"en.wikipedia.org","timestamp":1793188797,"user":"Example","bot":false,
                 "id":123456789,"revision":{"old":100,"new":101},"length":{"old":5000,"new":5120},
                 "minor":false,"title_url":"https://en.wikipedia.org/wiki/Apache_Kafka"}
                """;
    }

    static JsonNode readTree(String json) {
        try {
            return JSON.readTree(json);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
