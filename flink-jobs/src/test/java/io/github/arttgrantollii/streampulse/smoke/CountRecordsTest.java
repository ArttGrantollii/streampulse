package io.github.arttgrantollii.streampulse.smoke;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class CountRecordsTest {

    private static KeyedOneInputStreamOperatorTestHarness<String, String, Long> newHarness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new CountRecords()), value -> value, Types.STRING);
    }

    @Test
    void countsRecordsPerKey() throws Exception {
        try (var harness = newHarness()) {
            harness.open();

            harness.processElement("a", 0L);
            harness.processElement("a", 0L);
            harness.processElement("b", 0L);

            assertThat(harness.extractOutputValues()).containsExactly(1L, 2L, 1L);
        }
    }

    @Test
    void continuesFromRestoredStateAfterSnapshot() throws Exception {
        OperatorSubtaskState snapshot;
        try (var harness = newHarness()) {
            harness.open();
            harness.processElement("a", 0L);
            harness.processElement("a", 0L);
            // What a checkpoint captures for this operator.
            snapshot = harness.snapshot(1L, 1L);
        }

        // A fresh operator instance, as after a TaskManager failure, restored from the snapshot.
        try (var restored = newHarness()) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processElement("a", 0L);

            assertThat(restored.extractOutputValues()).containsExactly(3L);
        }
    }
}
