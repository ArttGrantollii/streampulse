package io.github.arttgrantollii.streampulse.job;

import io.github.arttgrantollii.streampulse.contract.EditEventParser;
import io.github.arttgrantollii.streampulse.contract.RawRecord;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

/**
 * Turns each raw-topic Kafka record into a {@link SourceRecord}, inside the Kafka source.
 * Never throws for bad data: a payload that cannot be parsed becomes a DLQ record, so one
 * malformed event cannot fail the job (a deserializer exception would restart it in a loop).
 */
public final class RawRecordDeserializer implements KafkaRecordDeserializationSchema<SourceRecord> {

    private static final long serialVersionUID = 1L;
    private static final byte[] EMPTY = new byte[0];

    private final EditEventParser parser;

    public RawRecordDeserializer(EditEventParser parser) {
        this.parser = parser;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<SourceRecord> out) {
        Map<String, byte[]> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), header.value() == null ? EMPTY : header.value()); // last one wins
        }
        RawRecord raw = new RawRecord(
                record.value() == null ? EMPTY : record.value(), // a tombstone is rejected as invalid_json
                headers,
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp());
        out.collect(SourceRecord.from(parser.parse(raw)));
    }

    @Override
    public TypeInformation<SourceRecord> getProducedType() {
        return TypeInformation.of(SourceRecord.class);
    }
}
