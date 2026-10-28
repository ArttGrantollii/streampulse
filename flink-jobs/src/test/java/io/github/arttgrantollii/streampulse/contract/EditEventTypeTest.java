package io.github.arttgrantollii.streampulse.contract;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

/**
 * EditEvent will live in Flink state (dedup, windows). If Flink cannot analyse it as a POJO
 * it silently falls back to Kryo: slower, and its state cannot evolve across job upgrades.
 */
class EditEventTypeTest {

    private final TypeInformation<EditEvent> type = TypeInformation.of(EditEvent.class);

    @Test
    void flinkTreatsTheRecordAsAPojoNotAGenericKryoType() {
        assertThat(type).isInstanceOf(PojoTypeInfo.class).isNotInstanceOf(GenericTypeInfo.class);
        assertThat(type.getArity()).isEqualTo(22);
    }

    @Test
    void serializerRoundTripsAllFieldsIncludingNulls() throws Exception {
        EditEvent original = ((ParseResult.Valid) new EditEventParser("t").parse(Fixtures.raw(Fixtures.edit()))).event();
        EditEvent sparse = new EditEvent("id", 1L, EventTimeSource.META_DT, null, 2L, ChangeType.UNKNOWN, null,
                null, null, null, null, null, null, null, null, null, null, null, true, null, 0, 0L);

        TypeSerializer<EditEvent> serializer = type.createSerializer(new SerializerConfigImpl());
        for (EditEvent event : new EditEvent[] {original, sparse}) {
            DataOutputSerializer out = new DataOutputSerializer(256);
            serializer.serialize(event, out);
            EditEvent copy = serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
            assertThat(copy).isEqualTo(event);
        }
    }
}
