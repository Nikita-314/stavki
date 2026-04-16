## Winline Source Plan

### Why production source moves to Winline

- Production signal source must match the bookmaker where the bet is actually placed.
- Winline gives the real event ids, real market labels, real selections, and real odds that the operator sees.
- This keeps `event_external_id`, search hints, and settlement flow aligned with the same source.

### Why The Odds API is not a production signal source here

- It is an aggregator, not the execution bookmaker.
- Its ids, market taxonomy, and outcome naming are not guaranteed to match Winline.
- Fake bookmaker mapping would produce signals that look compatible in code but do not match the real Winline line.
- It can stay as an auxiliary integration, but not as the production source of executable signals.

### Required line data from Winline

- `event_external_id`
- `sport`
- `tournament_name`
- `match_name`
- `home_team`
- `away_team`
- `is_live`
- `event_start_at`
- `market_type`
- `market_label`
- `selection`
- `odds_value`
- `section_name`
- `subsection_name`
- `search_hint`

### Required settlement data from Winline

- `event_external_id`
- `final result / winner / void`
- `settled_at`
- `raw result payload`

### Manual next step in browser devtools/network

1. Open Winline in the browser and keep `Network` filtered to `Fetch/XHR`.
2. Find the JSON that returns the event list / prematch or live line.
3. Open one event and find the JSON that returns the event card and its markets.
4. Find the JSON that returns final status, winner, void, or settlement/result state.
5. Save raw request URLs, methods, headers, and example JSON bodies for all three payloads.
6. Map the real JSON paths to the normalized fields above before writing any HTTP client.
