# ADR-0010 — Spike detection

- **Status:** Proposed. Parameters are fitted on recorded data in Phase 4.
- **Date:** 2026-10-26

## Context
"Breaking-event" detection asks whether a page is being edited much more than *its own* normal rate.
- Per-page counts are **sparse, small integers**: most pages see 0–1 edits per hour.
- With sparse counts, z-scores misbehave. The variance is ~0, so one extra edit gives an infinite z.
- Fixed thresholds ignore that some pages (e.g. Wikidata items edited by bots) are always busy.

## Proposed algorithm
Per `(wiki, title)` key, considering **human edits only** (bot mass edits are a different phenomenon, tracked separately), in event time:

1. **Current count** `k`: edits in a 5-minute window sliding every minute.
2. **Baseline rate** `λ`: an EWMA of per-minute counts over the past, with half-life ≈ 30 min, kept in keyed state, plus a small prior `λ₀` (the wiki-level median per-page rate) so new pages are not infinitely surprising.
3. **Expected count** `μ = max(λ, λ₀) × 5`.
4. **Score:** the Poisson upper-tail probability `p = P(X ≥ k | μ)`, reported as `spike_score = −log10(p)`.
5. **Spike** when all of the following hold:
   - `k ≥ k_min`: a minimum-volume guard, initially 5 edits;
   - `distinct_editors ≥ 2`: excludes one person saving repeatedly;
   - `spike_score ≥ s_min`: initially 6, i.e. p ≤ 10⁻⁶. That is deliberately strict because we test about 1 M pages a day, a multiple-testing problem.
6. **Cooldown:** at most one spike event per page per 30 minutes, so one news event doesn't produce a spike every minute.
7. **State bounding:** per-page state is cleared by an event-time timer after 6 h with no edits.

Every quantity is deterministic in event time, explainable in one sentence ("12 edits in 5 minutes when we'd expect 0.3"), and unit-testable with synthetic sequences.

## Alternatives
| Option | Why not |
|---|---|
| `count > N` | Ignores per-page baselines. Busy pages always fire and quiet pages never do |
| z-score on mean/variance | Breaks down on sparse counts |
| EWMA ratio `k / μ` alone | Unstable for small `μ`, and it says nothing about statistical significance |
| ML or seasonal models | Not explainable within the scope; the specification explicitly excludes them |

## Validation plan (Phase 4)
- Replay a 24 h recording, list the detected spikes, and sample them manually against real news or Wikipedia talk pages.
- Report precision on that sample and the spikes/hour rate, and tune `k_min` and `s_min`.
- Document false-positive classes: edit wars, bot-like humans, cleanup campaigns.
