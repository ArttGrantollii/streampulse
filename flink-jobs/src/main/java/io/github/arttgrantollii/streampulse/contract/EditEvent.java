package io.github.arttgrantollii.streampulse.contract;

/**
 * The internal event contract, version 1 (docs/ARCHITECTURE.md §7).
 *
 * <p>The only type that analytics code sees. Produced by {@link EditEventParser} from a
 * raw Wikimedia {@code recentchange} payload. Boxed fields are nullable: the upstream
 * schema makes them optional. A Java record, so Flink serializes it with its POJO
 * serializer rather than falling back to Kryo (checked by EditEventTypeTest); this
 * matters for state stored across job upgrades.
 *
 * @param eventId          {@code meta.id}, the unique event id; the dedup key (ADR-0007)
 * @param eventTimeMs      event time in epoch ms ({@code timestamp * 1000}, else {@code meta.dt})
 * @param eventTimeSource  which field {@code eventTimeMs} came from
 * @param emittedAtMs      {@code meta.dt}: when Wikimedia emitted it (null if absent)
 * @param ingestTimeMs     when our producer received it ({@code sp-ingest-ts} header)
 * @param type             change type; only EDIT and NEW are edits
 * @param rawType          {@code type} as sent, kept for UNKNOWN values (may be a number)
 * @param wiki             wiki database name, e.g. {@code enwiki}
 * @param serverName       e.g. {@code en.wikipedia.org}
 * @param namespace        page namespace id
 * @param title            full page title including namespace prefix
 * @param bot              rc_bot: the change was flagged as a bot change
 * @param user             user name, IP or temporary account
 * @param revisionNew      new revision id
 * @param revisionOld      previous revision id (null for page creations)
 * @param lengthNew        new page length in bytes
 * @param lengthOld        previous page length in bytes
 * @param rcId             rcid, unique per wiki (null for some log events)
 * @param canary           Wikimedia synthetic health-check event ({@code meta.domain == "canary"})
 * @param sourceSchema     {@code $schema} the event declared
 * @param sourcePartition  raw-topic partition (debugging, late-event records)
 * @param sourceOffset     raw-topic offset
 */
public record EditEvent(
        String eventId,
        long eventTimeMs,
        EventTimeSource eventTimeSource,
        Long emittedAtMs,
        long ingestTimeMs,
        ChangeType type,
        String rawType,
        String wiki,
        String serverName,
        Integer namespace,
        String title,
        Boolean bot,
        String user,
        Long revisionNew,
        Long revisionOld,
        Integer lengthNew,
        Integer lengthOld,
        Long rcId,
        boolean canary,
        String sourceSchema,
        int sourcePartition,
        long sourceOffset) {

    /** Size change in bytes, if both lengths are known. */
    public Integer bytesDelta() {
        return lengthNew == null || lengthOld == null ? null : lengthNew - lengthOld;
    }
}
