# Provider Selection And Modeling

## Source Choice

Choose Sina when:

- You need a working MVP quickly.
- You need historical backfill by calendar date.
- You want one payload that currently includes all five play types.
- You want lower request-environment sensitivity.
- You want to combine low-frequency batch polling with on-demand match-detail enrichment from the same provider family.

Choose Sporttery when:

- You need a source closer to the original sporttery ecosystem.
- You are willing to validate headers, runtime, and fallback behavior.
- You want a secondary source for cross-checking payloads.

Use both when:

- You want Sina as the operational source and Sporttery as a probe or validation source.
- You need confidence checks before settling field mappings.

## Minimal Storage Model

Persist at least:

- `source_provider`
- `request_date`
- `match_id`
- `ticai_id`
- `match_no`
- `league`
- `team1`
- `team2`
- `kickoff_time`
- `score_full`
- `score_half`
- `show_sell_status`
- Per-play sell-status fields
- Raw odds strings for `spf`, `rqspf`, `bf`, `jq`, `bqc`
- Raw source payload

## Normalization Rules

- Keep provider-native IDs and normalized IDs side by side.
- Parse odds strings into structured arrays or objects, but never discard the raw string.
- Treat sale-status values as opaque enums until enough samples support a stable mapping.
- Keep `matchNo` as a display identifier, not as the primary date key.
- Store list-level odds and detail-level bookmaker odds separately. They answer different questions and are not interchangeable.

## Modeling Notes

- Default to the regular parlay context. Do not build single-match logic until an explicit single-match field is confirmed.
- Start prediction work with `spf` as the main target and `rqspf` as an important auxiliary feature.
- Delay `bf`, `jq`, and `bqc` prediction targets until the simpler targets are stable.
- When detail odds are available, prefer `footballMatchOddsEuro` for `spf` market context and `footballMatchOddsAsia` for `rqspf` market context.
- Use `FootballMatchIntelligence` only as a low-weight supplement after structured data such as odds, standings, form, and injuries.

## Validation Checklist

- Re-fetch a live sample before coding against an assumption.
- Verify whether the task needs current-sale data or settled historical data.
- Record the exact request URL, headers, and fetch date when analyzing a provider.
- Distinguish verified facts from inferred semantics in code comments and docs.
- Split the job into two layers:
  - Batch layer: discover current sale batches and settlement state
  - Detail layer: fetch per-match detail only when generating AI context or a detail page
