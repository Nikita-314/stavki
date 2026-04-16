## Winline Mapping Runbook

### 1. What to find in DevTools

- Event list request: prematch line and, if available, live line.
- Event card request: one concrete event with its market groups and outcomes.
- Result request: status, settlement, winner, score, or cancelled/void state.

### 2. What to capture for each request

For every useful request, write down:

- `page url`
- `request url`
- `method`
- `headers` - only important technical headers, no secrets, no cookies, no auth dumps
- `query params`
- `response content-type`
- `example response JSON`
- `important response fields`

Minimum capture rule:
- one raw response example for line list
- one raw response example for event markets
- one raw response example for results
- enough context to see how `event_external_id` is formed and whether it is stable

### 3. Mapping table for line payload

| Normalized field | Required | Expected meaning | Real JSON path | Notes |
|------------------|----------|------------------|----------------|-------|
| `event_external_id` | Yes | Stable event identifier used in both line and result payloads |  | Must match result payload id |
| `sport` | Yes | Sport slug or title that can be mapped to `SportType` |  | Example: `football`, `soccer`, `cs2`, `dota2` |
| `tournament_name` | Yes | Competition / league / tournament title |  | Prefer provider field, not synthetic fallback |
| `match_name` | Yes | Human-readable match title |  | Can be built if provider only gives teams |
| `home_team` | Yes | First team / player / side |  | Required for signal creation |
| `away_team` | Yes | Second team / player / side |  | Required for signal creation |
| `event_start_at` | No | Scheduled start timestamp if present |  | Record timezone/format |
| `is_live` | Yes | Live flag or status-derived boolean |  | Can be derived from status field |
| `market_type` | Yes | Stable market code or normalized market kind |  | Prefer raw provider code over display label |
| `market_label` | Yes | Human-readable market label |  | Example: `Match Winner`, `1x2` |
| `selection` | Yes | Outcome title / side / pick name |  | Must match settlement winner semantics when possible |
| `odds_value` | Yes | Decimal coefficient / price |  | Record exact numeric field |
| `section_name` | No | High-level market section |  | Example: `Main`, `Totals` |
| `subsection_name` | No | Nested group inside a section |  | Example: `Match Result` |
| `search_hint` | No | Searchable helper string for operator UI |  | Can be normalized later from teams + selection |

### 4. Mapping table for result payload

| Normalized field | Required | Expected meaning | Real JSON path | Notes |
|------------------|----------|------------------|----------------|-------|
| `event_external_id` | Yes | Stable event identifier matching the line payload |  | Critical join key |
| `sport` | Yes | Sport slug/title mappable to `SportType` |  | May be nested inside event metadata |
| `winner_selection` | No | Winning side / selection title if event is settled normally |  | Must be comparable with signal selection |
| `is_void` | Yes | Whether event/market is void, cancelled, refunded, or not settled |  | Can be derived from status codes |
| `settled_at` | No | Settlement timestamp |  | Keep raw provider timezone |
| `raw_json` | Yes | Original provider result item | n/a | Preserve full item for future debugging |

### 5. What counts as a good enough payload

- Event id is stable inside the provider and reusable for settlement lookup.
- Team names or sides are present and readable.
- Market selection is present.
- Odds coefficient is present as a numeric field or numeric string.
- There is a clear way to determine final winner or void/cancelled outcome.

### 6. Implementation readiness checklist

- [ ] Line payload found
- [ ] Result payload found
- [ ] Event id matches between line and result payloads
- [ ] Sport can be determined and mapped
- [ ] Market selection and odds are available
- [ ] Settlement winner or void can be determined
