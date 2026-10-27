# ADR-0001 — Use `mediawiki.recentchange` as the V1 source

- **Status:** Accepted
- **Date:** 2026-10-26
- **Evidence:** [Research 0001](../research/0001-eventstreams-source-validation.md)

## Context
The specification assumed a "Wikipedia edit stream" with page identifiers. Research showed:
- `recentchange` covers all Wikimedia projects and has no `page_id`.
- `mediawiki.page_change.v1` has `page_id`, but it reports a different bot signal (`performer.is_bot` instead of rc_bot), contains legitimately ancient `dt` values (3.9% of the sample), and is about 2.4× larger per event.

## Decision
Ingest **`mediawiki.recentchange`** only, through `https://stream.wikimedia.org/v2/stream/recentchange`, in V1.
- An **edit** is defined as `type ∈ {edit, new}`. Other types are kept in raw and counted by type, but they are not "edits".
- **Page identity** is `(wiki, namespace, title)`. Analytics key on `(wiki, title)`, because the title already contains the namespace prefix.
- **Bot** means the `bot` field (the edit was flagged as a bot edit).
- All projects are ingested. A `project_family` dimension (wikipedia, wiktionary, commons, wikidata, …) lets dashboards focus on Wikipedia without discarding data.

## Alternatives
| Option | Why not (for V1) |
|---|---|
| `mediawiki.page_change.v1` as the primary source | Stable page IDs are attractive. However, it has a less established bot semantic, a stricter schema, ancient `dt` values that complicate event-time validation, and is not the canonical "recent changes" view that edit-count analytics expect |
| Both streams, joined on `rev_id` | A good stream-join exercise, but a second source and a join belong after the core is correct. Kept as a stretch goal |
| Filter to Wikipedia only at ingestion | Violates "raw is immutable and complete". Filtering is a query-time concern |

## Consequences
- A page move changes the title, so a moved page splits into two identities. That is acceptable for minute-to-hour analytics, and documented as a limitation.
- Top-page analytics need explicit filters (namespace 0, human-only, project family), because bot activity on Wikidata dominates raw counts.
- Moving to `page_change` later is a source change, not a redesign: the internal `EditEvent` contract ([ADR-0005](0005-serialization-and-schema-contracts.md)) isolates it.
