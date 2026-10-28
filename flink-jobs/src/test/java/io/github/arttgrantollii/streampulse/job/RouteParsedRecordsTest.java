package io.github.arttgrantollii.streampulse.job;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.arttgrantollii.streampulse.contract.ChangeType;
import io.github.arttgrantollii.streampulse.contract.EditEvent;
import io.github.arttgrantollii.streampulse.contract.EventTimeSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

class RouteParsedRecordsTest {

    private static EditEvent event(String id, boolean canary) {
        return new EditEvent(id, 1_000L, EventTimeSource.TIMESTAMP, 1_500L, 2_000L, ChangeType.EDIT, "edit",
                "enwiki", "en.wikipedia.org", 0, "Kafka", false, "u", 2L, 1L, 10, 5, 9L, canary, null, 0, 0L);
    }

    @Test
    void validEventsContinueDlqRecordsGoToTheSideOutputAndCanariesAreDropped() throws Exception {
        try (OneInputStreamOperatorTestHarness<SourceRecord, EditEvent> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(new RouteParsedRecords())) {
            harness.processElement(new SourceRecord(event("a", false), null, null), 0L);
            harness.processElement(new SourceRecord(null, "invalid_json", "{\"reason\":\"invalid_json\"}"), 0L);
            harness.processElement(new SourceRecord(event("canary", true), null, null), 0L);
            harness.processElement(new SourceRecord(event("b", false), null, null), 0L);

            assertThat(harness.extractOutputValues()).extracting(EditEvent::eventId).containsExactly("a", "b");
            assertThat(harness.getSideOutput(RouteParsedRecords.DLQ))
                    .extracting(StreamRecord::getValue)
                    .containsExactly("{\"reason\":\"invalid_json\"}");
        }
    }
}
