# Sporttery Web API

## Reference Implementation

Observed implementation reference:

- Repository: `https://github.com/excalibur-sa/football-lottery`
- File: `api/sporttery_provider.py`

That project calls Sporttery domain endpoints instead of scraping HTML pages.

## Known Endpoint Family

Base host:

```text
https://webapi.sporttery.cn/gateway
```

Common football endpoints seen in the reference implementation:

- `/uniform/football/getMatchListV1.qry`
- `/uniform/football/getUniformMatchResultV1.qry`
- `/uniform/football/getFixedBonusV1.qry`

Typical example URLs:

```text
https://webapi.sporttery.cn/gateway/uniform/football/getMatchListV1.qry?clientCode=3001&showHistory=1&sort=0
https://webapi.sporttery.cn/gateway/uniform/football/getFixedBonusV1.qry?clientCode=3001&matchIssueNo=250405
```

## Verified Behavior

Observed on `2026-04-05`:

- Direct `curl` requests returned an HTML error page instead of JSON.
- Adding common browser headers such as `User-Agent`, `Referer`, and `Accept` did not fix the response in this environment.

Treat this as an environment-sensitive source. It may work in one runtime and fail in another.

## Practical Implications

- Sporttery is closer to the original ecosystem, but it is harder to rely on for unattended sync.
- Do not assume the interface is publicly stable just because a third-party repository uses it.
- Re-verify response format from the live environment before implementing production logic around it.

## Recommended Use

- Use Sporttery as a secondary source or a validation source.
- Prefer it when you need an official-ish comparison point and you can afford probe logic or fallback handling.
- Do not make it the only source for an MVP unless you have already validated reliable access from the deployment environment.
