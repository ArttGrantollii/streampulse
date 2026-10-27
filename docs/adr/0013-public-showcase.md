# ADR-0013 — Public showcase: a static site, not a hosted pipeline

- **Status:** Accepted
- **Date:** 2026-10-26

## Context
StreamPulse will be published on a public portfolio alongside SparkShift. Visitors (recruiters, interviewers, engineers) click in, spend a few minutes, and must have a **smooth, zero-install experience**. They will not run `docker compose`.

Hosting the full stack (Kafka, Flink, PostgreSQL, Grafana) around the clock would need a VM with roughly 8 GB of RAM, a monthly bill and ongoing operations. A public Grafana or Flink UI is also a security and usability liability.

Verified facts:
- Wikimedia EventStreams answers browser requests with `access-control-allow-origin: *` (checked 2026-10-26), so a browser can consume the live stream directly.
- SparkShift already ships a static GitHub Pages playground (`sparkshift/.github/workflows/pages.yml`). The portfolio should feel consistent.

## Decision
Two audiences, two deliverables:

1. **The engineering system** (this repository), for engineers who want to run it: `make up` plus `make demo`, which replays a bundled recording so it works offline in minutes without waiting for live traffic.
2. **The public showcase**: a static site on GitHub Pages, with no backend and no running costs:
   - **Live**: the visitor's browser connects to Wikimedia directly and shows the live edit stream. An interactive visualisation explains the concepts the real pipeline implements: event time vs arrival time, watermarks, windows closing, late events, duplicates being dropped. The visitor can adjust the watermark delay or inject a duplicate. It is **clearly labelled as an in-browser illustration**, not the Flink pipeline.
   - **Results**: real outputs exported as static JSON from a multi-day run of the actual pipeline. The headline is detected "breaking news" spikes, with their validation (precision and lead time).
   - **Engineering**: architecture diagram, the crash-test result (PASS, recovery time), measured throughput, and links to ADRs and code.

## Alternatives
| Option | Why not |
|---|---|
| Host the full stack on a VM | Ongoing cost and operations. A dashboard frozen during an outage looks worse than a static page. Visitors can't interact with Flink anyway |
| Public read-only Grafana | Not interactive in a meaningful way, a security surface, and slow to load |
| Screenshots and video only | Not "play around", and loses the live element that makes streaming tangible |

## Consequences
- Results shown publicly come from **recorded real runs**. The page states the run dates, so they are never presented as live.
- The browser visualisation must never be confused with the real pipeline, so labels say exactly what runs where.
- Export tooling (PostgreSQL → static JSON, without raw data) becomes part of the final phase.
- The site follows Wikimedia etiquette: one stream connection per open tab, closed when the tab is hidden.
