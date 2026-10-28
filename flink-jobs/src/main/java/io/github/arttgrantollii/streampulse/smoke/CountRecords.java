package io.github.arttgrantollii.streampulse.smoke;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Counts records per key in keyed state and emits the running count.
 *
 * <p>Because the count is Flink-managed state, it is included in checkpoints and
 * restored after a failure, so a restarted job continues from the checkpointed
 * value instead of starting again at zero.
 */
public class CountRecords extends KeyedProcessFunction<String, String, Long> {

    static final String SINGLE_KEY = "all";

    private static final Logger LOG = LoggerFactory.getLogger(CountRecords.class);

    private transient ValueState<Long> count;

    @Override
    public void open(OpenContext openContext) {
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
    }

    @Override
    public void processElement(String value, Context ctx, Collector<Long> out) throws Exception {
        Long current = count.value();
        long next = (current == null ? 0L : current) + 1;
        count.update(next);
        // scripts/smoke_test.sh reads this line from the TaskManager log.
        LOG.info("SMOKE_COUNT={}", next);
        out.collect(next);
    }
}
