# Sina Mobile JSON

## Endpoint Template

Primary endpoint template:

```text
https://alpha.lottery.sina.com.cn/gateway/index/entry?format=json&__caller__=wap&__version__=1.0.0&__verno__=10000&cat1=jczqMatches&gameTypes=spf&date=&isPrized=&isAll=1&dpc=1
```

Recommended headers:

```text
User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1
Referer: https://alpha.lottery.sina.com.cn/lottery/jczq/
```

Detail endpoint host:

```text
https://mix.lottery.sina.com.cn/gateway/index/entry
```

## Verified Behavior

Observed on `2026-04-05`:

- The endpoint returns JSON with `result.status.code = 0`.
- `result.data` is the match array.
- `date=YYYY-MM-DD` plus `isPrized=1` returns settled historical matches.
- `date=YYYY-MM-DD` plus `isPrized=0` returns current sale-day matches.
- `dpc=0` versus `dpc=1` showed no visible payload difference in the tested sample.

Historical example:

```text
https://alpha.lottery.sina.com.cn/gateway/index/entry?format=json&__caller__=wap&__version__=1.0.0&__verno__=10000&cat1=jczqMatches&gameTypes=spf&date=2026-04-04&isPrized=1&isAll=1&dpc=1
```

## Current Payload Shape

Each match object currently includes:

- Identity: `matchId`, `tiCaiId`, `matchNo`, `matchNoValue`
- Match info: `league`, `leagueOfficial`, `team1`, `team2`, `matchTime`, `matchTimeFormat`
- Result info: `score1`, `score2`, `halfScore1`, `halfScore2`
- Sale statuses: `showSellStatus`, `showSellStatusCn`, `spfSellStatus`, `rqspfSellStatus`, `bfSellStatus`, `jqSellStatus`, `bqcSellStatus`
- Odds and prizes: `spf`, `spfPrize`, `rqspf`, `rqspfPrize`, `bf`, `bfPrize`, `jq`, `jqPrize`, `bqc`, `bqcPrize`

## Match Detail Endpoints

Verified on `2026-04-06` for `matchId=3572486`:

- `footballMatchDetail`
  - Match status, score, round, stage, weather, environment, team positions
- `footballMatchTeamBattleHistory`
  - Historical head-to-head matches with score, euro odds, asia handicap, totals line
- `footballMatchTeamTable`
  - Team standings and split records such as all/home/away
- `footballMatchTeamRecentZhanJi`
  - A team's recent completed matches
- `footballMatchTeamInjury`
  - Injury and suspension list
- `footballMatchTeamRecentMatches`
  - Upcoming fixtures for both teams
- `footballMatchOddsEuro`
  - Multi-bookmaker European odds with initial/current values and timestamps
- `footballMatchOddsAsia`
  - Multi-bookmaker Asian handicap odds with initial/current handicap lines and timestamps
- `footballMatchOddsTotals`
  - Multi-bookmaker totals odds with initial/current goal lines and timestamps
- `FootballMatchIntelligence`
  - Prewritten positive/negative/neutral text insights for each team

Representative URLs:

```text
https://mix.lottery.sina.com.cn/gateway/index/entry?format=json&__caller__=wap&__version__=1.0.0&__verno__=10000&cat1=footballMatchDetail&matchId=3572486
https://mix.lottery.sina.com.cn/gateway/index/entry?format=json&__caller__=wap&__version__=1.0.0&__verno__=10000&cat1=footballMatchTeamBattleHistory&matchId=3572486&limit=10&isSameHostAway=0&isSameLeague=0&dpc=1
https://mix.lottery.sina.com.cn/gateway/index/entry?format=json&__caller__=wap&__version__=1.0.0&__verno__=10000&cat1=footballMatchTeamTable&matchId=3572486&dpc=1
```

Recommended request mode:

- Use `jczqMatches` for batch detection and settlement polling
- Use match detail endpoints only when generating AI context or rendering a match detail view
- Cache detail payloads by `event_key` for several hours instead of polling them continuously
- Suggested enrichment priority:
  1. `footballMatchDetail`
  2. `footballMatchOddsEuro`
  3. `footballMatchOddsAsia`
  4. `footballMatchTeamTable`
  5. `footballMatchTeamBattleHistory`
  6. `footballMatchTeamRecentZhanJi`
  7. `footballMatchTeamInjury`
  8. `FootballMatchIntelligence` as low-weight prose context
  9. `footballMatchTeamRecentMatches` as lower-priority schedule context

## Important Observations

- `gameTypes=spf`, `rqspf`, `bf`, `jq`, and `bqc` currently return the same full match object shape.
- For MVP ingestion, one feed is enough. Prefer `gameTypes=spf` unless a live check shows a regression.
- `showSellStatus` looks like overall availability. `*SellStatus` is per play type.
- No explicit `single`, `dan`, `isSingle`, or `dg` field was found in the tested payloads.
- A single anomalous sell-status value is not enough to conclude a match is 单关.

## Parsing Notes

- `spf`: 3 comma-separated odds for win, draw, loss.
- `rqspf`: 4 comma-separated segments. The first segment is the handicap, the next 3 are odds.
- `jq`: 8 comma-separated odds.
- `bqc`: 9 comma-separated odds.
- `bf`: 31 comma-separated odds.
- `*Prize` fields are empty before settlement and contain result plus prize after settlement.

Keep both:

- Raw strings such as `spf`, `rqspf`, `bf`, `jq`, `bqc`
- Normalized structured fields derived from those strings

## Recommended Use

- Use Sina as the default source for quick project integration.
- Use it for historical backfill because `date` and `isPrized` are directly usable.
- Snapshot raw payloads if you later want line movement or pre-close comparisons.
- For AI prediction quality, enrich each candidate match with detail, table, battle-history, recent-form, and injury data right before prompting the model.
- Add euro/asia/totals odds when you want stronger market-based features than the simplified list-level odds.
- Treat `FootballMatchIntelligence` as a supplemental text source, not as the primary truth. It is already opinionated and can bias the model.
- Do not replace batch polling with detail polling. The list endpoint is still the right place to discover new sale batches and completed results.
