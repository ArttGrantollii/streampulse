package io.github.arttgrantollii.streampulse.contract;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.InputFormat;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Contract test: what the job writes to the DLQ must match schemas/streampulse/dlq-record.v1.json. */
class DlqRecordTest {

    private static JsonSchema schema;

    @BeforeAll
    static void loadSchema() throws IOException {
        String json = Files.readString(Fixtures.REPO_ROOT.resolve("schemas/streampulse/dlq-record.v1.json"));
        schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(json);
    }

    private static Set<ValidationMessage> violations(String json) {
        return schema.validate(json, InputFormat.JSON);
    }

    private static DlqRecord dlq(RejectReason reason, byte[] payload) {
        return DlqRecord.of(Fixtures.raw(payload), reason, "because", "0.1.0", 1_793_188_801_000L);
    }

    @ParameterizedTest
    @EnumSource(RejectReason.class)
    void everyReasonProducesASchemaValidRecord(RejectReason reason) {
        String json = dlq(reason, "{oops".getBytes(StandardCharsets.UTF_8)).toJson();
        assertThat(violations(json)).isEmpty();
    }

    @Test
    void schemaRejectsAnUnknownReason() {
        // Guards the contract test itself: a validator that accepts everything would prove nothing.
        String json = dlq(RejectReason.INVALID_JSON, new byte[] {'x'}).toJson().replace("invalid_json", "made_up");
        assertThat(violations(json)).isNotEmpty();
    }

    @Test
    void jsonCarriesTheDocumentedFieldNames() {
        JsonNode node = Fixtures.readTree(dlq(RejectReason.MISSING_TIME, "{}".getBytes(StandardCharsets.UTF_8)).toJson());
        assertThat(node.get("schema_version").asInt()).isEqualTo(1);
        assertThat(node.get("reason").asText()).isEqualTo("missing_time");
        assertThat(node.at("/source/partition").asInt()).isEqualTo(1);
        assertThat(node.at("/source/offset").asLong()).isEqualTo(42L);
        assertThat(node.at("/source/headers/sp-ingest-ts").asText()).isEqualTo(Long.toString(Fixtures.INGEST_MS));
    }

    @Test
    void oversizedPayloadIsTruncatedButItsSizeIsKept() {
        byte[] huge = "x".repeat(DlqRecord.MAX_RAW_VALUE_CHARS + 10).getBytes(StandardCharsets.UTF_8);
        DlqRecord record = dlq(RejectReason.INVALID_JSON, huge);
        assertThat(record.rawValueTruncated()).isTrue();
        assertThat(record.rawValue()).hasSize(DlqRecord.MAX_RAW_VALUE_CHARS);
        assertThat(record.rawValueBytes()).isEqualTo(huge.length);
        assertThat(violations(record.toJson())).isEmpty();
    }
}
