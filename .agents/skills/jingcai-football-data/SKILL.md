---
name: jingcai-football-data
description: Chinese football lottery / 竞彩足球 / 足球彩票 data-source workflow for this repository. Use when Codex needs to inspect Sina or Sporttery payloads, compare provider stability, adjust fetch and sync logic, design parsing or storage, or maintain prediction data flow around 竞彩足球 matches in this project.
---

# Jingcai Football Data

## Quick Start

- Read `services/jingcai_football_service.py` before changing provider behavior, request strategy, or detail enrichment.
- Read `utils/jingcai_football.py` before changing field parsing, normalization, or result derivation.
- For live endpoint validation, run `scripts/probe_jingcai_source.py` from this skill directory before coding against assumptions.
- Default to Sina mobile JSON for MVP, daily sync, and historical backfill.
- Treat Sporttery web APIs as a secondary or validation source because real requests are more sensitive to request environment.
- Persist raw payloads before normalization. Keep the source name, request date, and probe notes.
- Ignore 单关 detection unless an explicit field is found. Do not infer it from one anomalous status value alone.
- Use list endpoints for batch discovery and settlement polling.
- Use match-detail endpoints only when you are actually about to build AI context or a match-detail page.

## Workflow

1. Pick the provider path.
   - For fastest integration or historical backfill, read [references/sina-mobile.md](references/sina-mobile.md).
   - For official-ish endpoints or source comparison, read [references/sporttery-official.md](references/sporttery-official.md).
   - For storage, field mapping, and selection rules, read [references/provider-selection.md](references/provider-selection.md).
2. Validate live behavior with a real request before coding against assumptions. Prefer `scripts/probe_jingcai_source.py` for that first pass. These endpoints can change or block unattended clients.
3. Normalize odds strings into structured fields, but store the raw strings too.
4. Fetch detail endpoints on demand for the specific matches being predicted.
5. Cache detail payloads by `event_key` and `detail_type`, then summarize them before sending them to the model.
6. Keep sale-status enums raw unless enough evidence exists to map them safely.

## Current Observations

- Sale-status mapping must still be treated as observational, not absolute. Re-verify on new dates before hard-coding business rules.
- In the Sina sample for `2026-04-06`, `spfSellStatus=2` matched the same-day matches that appeared to open `胜平负` 单关, including `周一002` and `周一012`.
- In the same sample, `spfSellStatus=0` matched matches that did not open `胜平负`, including `周一016`.
- `rqspfSellStatus` still has weaker evidence. Do not expand the above observation to `rqspf` unless a fresh sample confirms it.
- Prefer a conservative implementation pattern in product code:
  - Treat `0` and explicit settled states as unavailable.
  - Allow recommendation/simulation only when odds are present and the event is not already settled.
  - If later you need to explicitly label “单关”, surface it as a tentative UI/ops hint first, not as a hard-coded invariant.

## Project Touchpoints

- Batch fetch and detail fetch live in `services/jingcai_football_service.py`.
- Domain parsing and result normalization live in `utils/jingcai_football.py`.
- Persistence flows through `database.py` lottery event and lottery event detail tables.
- Prompt assembly and target constraints also depend on `utils/prompt_assistant.py` and `ai_trader.py`.

## Execution Rules

- Use mobile-style headers for Sina requests.
- Prefer a single Sina `gameTypes=spf` feed first. Current observations show it already carries `spf`, `rqspf`, `bf`, `jq`, and `bqc` in the same match object.
- For match-level enrichment, prefer these Sina `mix.lottery.sina.com.cn` categories:
  - `footballMatchDetail`
  - `footballMatchTeamBattleHistory`
  - `footballMatchTeamTable`
  - `footballMatchTeamRecentZhanJi`
  - `footballMatchTeamInjury`
  - `footballMatchTeamRecentMatches` as a lower-priority schedule supplement
- Treat these as high-value market/context enrichments when available:
  - `footballMatchOddsEuro`
  - `footballMatchOddsAsia`
  - `footballMatchOddsTotals`
  - `FootballMatchIntelligence`
- Use absolute dates like `2026-04-05` and exact `matchNo` values when discussing a sample because `周日024` is not a calendar date.
- When a task depends on current behavior, re-fetch a sample response instead of trusting old notes.
- Do not high-frequency poll detail endpoints all day. Low-frequency poll the batch list; fetch detail data only when a new batch is being predicted or a detail view is being rendered.
- Prefer structured signals over prewritten prose: euro/asia/totals and standings should outweigh `FootballMatchIntelligence` text when they conflict.

## Output Expectations

- State which provider you chose and why.
- Separate verified facts from inference.
- Call out anti-bot, empty-response, or status-code uncertainty explicitly.
