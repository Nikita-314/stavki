## Winline JSON Path Insertion Checklist

### A. Line payload insertion checklist

#### 1. `event_external_id`

- Step: Locate the stable event identifier in the line list or event object.
- What to locate: Provider event id that also appears in the result payload.
- Expected normalized field: `event_external_id`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` event extraction loop
- Notes: If not found, skip item. Do not synthesize from display title.

#### 2. `sport`

- Step: Locate sport slug, code, or title on the event object.
- What to locate: Raw provider sport value that can later map to `SportType`.
- Expected normalized field: `sport`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` event extraction loop
- Notes: Fallback from another event-level sport field is allowed. If still missing, skip item.

#### 3. `tournament_name`

- Step: Locate competition, league, or tournament title.
- What to locate: Tournament container or event metadata field.
- Expected normalized field: `tournament_name`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` event extraction loop
- Notes: Fallback from parent category/league field is allowed. If missing, skip item.

#### 4. `match_name`

- Step: Locate explicit match title or decide whether to build it.
- What to locate: Event title field such as `Team A vs Team B`.
- Expected normalized field: `match_name`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` event extraction loop
- Notes: If not present, build synthetic value from `home_team` + `away_team`. Skip only if teams are also missing.

#### 5. `home_team`

- Step: Locate first side or home team name.
- What to locate: Team 1 / competitor 1 / home side field.
- Expected normalized field: `home_team`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` event extraction loop
- Notes: Fallback from participants array is allowed. If missing, skip item.

#### 6. `away_team`

- Step: Locate second side or away team name.
- What to locate: Team 2 / competitor 2 / away side field.
- Expected normalized field: `away_team`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` event extraction loop
- Notes: Fallback from participants array is allowed. If missing, skip item.

#### 7. `event_start_at`

- Step: Locate scheduled start timestamp if the provider exposes it.
- What to locate: Event start time / kickoff / begin time field.
- Expected normalized field: `event_start_at`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` event extraction loop
- Notes: Leave `None` if absent. Record raw timezone format in notes.

#### 8. `is_live`

- Step: Locate explicit live flag or derive it from status.
- What to locate: `is_live`, `live`, status code, or live section membership.
- Expected normalized field: `is_live`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` event extraction loop
- Notes: Fallback from status-derived boolean is allowed. Default `False` only if provider meaning is clear.

#### 9. `market_type`

- Step: Locate the stable market code or raw market kind.
- What to locate: Provider market id/code, not only UI label.
- Expected normalized field: `market_type`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` market extraction loop
- Notes: If missing, fallback from market label only as temporary workaround. Skip market if neither exists.

#### 10. `market_label`

- Step: Locate human-readable market name.
- What to locate: Market title shown in event card or market group.
- Expected normalized field: `market_label`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` market extraction loop
- Notes: Fallback from `market_type` is allowed. Do not skip if `market_type` is already present.

#### 11. `selection`

- Step: Locate outcome title or pick side.
- What to locate: Outcome name inside market outcomes array.
- Expected normalized field: `selection`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` market extraction loop
- Notes: If missing, skip market item.

#### 12. `odds_value`

- Step: Locate coefficient / price / decimal odds field.
- What to locate: Numeric or numeric-string outcome price.
- Expected normalized field: `odds_value`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` market extraction loop
- Notes: If missing or not parseable as decimal, skip market item.

#### 13. `section_name`

- Step: Locate market section or tab name.
- What to locate: High-level group such as `Main`, `Totals`, `Handicaps`.
- Expected normalized field: `section_name`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` market extraction loop
- Notes: Leave `None` if not present. Can also be normalized from parent market group.

#### 14. `subsection_name`

- Step: Locate nested subgroup name if the provider splits markets further.
- What to locate: Child market group or subcategory title.
- Expected normalized field: `subsection_name`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` market extraction loop
- Notes: Leave `None` if not present.

#### 15. `search_hint`

- Step: Decide whether provider gives a useful search string or whether it should be built.
- What to locate: Searchable title or combined market text.
- Expected normalized field: `search_hint`
- JSON path from DevTools:
- Example value:
- Where to insert in code: inside `parse_payload()` market extraction loop, or inside `to_candidates()` only if normalization is deferred
- Notes: Build synthetic value from teams + market label + selection if no provider field exists.

### B. Result payload insertion checklist

#### 1. `event_external_id`

- Step: Locate the stable event identifier in the result payload.
- What to locate: Provider result event id matching the line payload.
- Expected normalized field: `event_external_id`
- JSON path from DevTools:
- Example value:
- Where to insert in code: `parse_result_payload()`
- Notes: No fallback if id cannot be matched. Skip result item.

#### 2. `sport`

- Step: Locate sport metadata on the result item or its parent event object.
- What to locate: Sport slug, code, or title.
- Expected normalized field: `sport`
- JSON path from DevTools:
- Example value:
- Where to insert in code: `_extract_sport()`
- Notes: Fallback across multiple raw sport keys is allowed. Skip result item if sport still cannot be mapped.

#### 3. `winner_selection`

- Step: Locate winner side, winning participant, or final selection equivalent.
- What to locate: Winner field, score-derived winner, or provider result code.
- Expected normalized field: `winner_selection`
- JSON path from DevTools:
- Example value:
- Where to insert in code: `parse_result_payload()` or future helper for winner extraction
- Notes: Fallback is allowed if winner can be derived from score/result code. Leave `None` only when event is not void and winner truly cannot be determined.

#### 4. `is_void`

- Step: Locate cancelled/void/refund status or derive it from event status.
- What to locate: Void flag, cancelled status, refund code, or equivalent.
- Expected normalized field: `is_void`
- JSON path from DevTools:
- Example value:
- Where to insert in code: `parse_result_payload()` or future helper for void detection
- Notes: Fallback from status code is allowed. If neither void nor winner can be determined, skip result item.

#### 5. `settled_at`

- Step: Locate settlement timestamp or final timestamp.
- What to locate: Settled, finished, completed, or updated-at field.
- Expected normalized field: `settled_at`
- JSON path from DevTools:
- Example value:
- Where to insert in code: `parse_result_payload()`
- Notes: Leave `None` if absent.

#### 6. `raw_json`

- Step: Preserve the full provider result item.
- What to locate: Entire raw result object after extraction.
- Expected normalized field: `raw_json`
- JSON path from DevTools: full item
- Example value:
- Where to insert in code: `parse_result_payload()`
- Notes: Always preserve raw item for debugging and later mapping refinement. Do not skip because of missing optional fields if `raw_json` is available.
