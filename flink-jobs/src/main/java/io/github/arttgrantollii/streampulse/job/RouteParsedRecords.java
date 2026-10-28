package io.github.arttgrantollii.streampulse.job;

import io.github.arttgrantollii.streampulse.contract.EditEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Splits parsed records three ways: valid events continue (main output), rejected records
 * go to the DLQ side output, and Wikimedia canary events are dropped (they are synthetic
 * health checks, Research 0001). Each path is counted, so DLQ spikes are visible in
 * Flink's metrics (exported to Prometheus in Phase 5) with the reason as a label.
 */
public class RouteParsedRecords extends ProcessFunction<SourceRecord, EditEvent> {

    public static final OutputTag<String> DLQ = new OutputTag<>("dlq", Types.STRING);

    private transient MetricGroup metrics;
    private transient Counter validEvents;
    private transient Counter canaryDropped;
    private transient Map<String, Counter> dlqByReason;

    @Override
    public void open(OpenContext openContext) {
        metrics = getRuntimeContext().getMetricGroup().addGroup("streampulse");
        validEvents = metrics.counter("validEvents");
        canaryDropped = metrics.counter("canaryDropped");
        dlqByReason = new HashMap<>();
    }

    @Override
    public void processElement(SourceRecord record, Context ctx, Collector<EditEvent> out) {
        if (!record.isValid()) {
            dlqByReason
                    .computeIfAbsent(record.dlqReason(), reason -> metrics.addGroup("reason", reason).counter("dlqRecords"))
                    .inc();
            ctx.output(DLQ, record.dlqJson());
            return;
        }
        if (record.event().canary()) {
            canaryDropped.inc();
            return;
        }
        validEvents.inc();
        out.collect(record.event());
    }
}
