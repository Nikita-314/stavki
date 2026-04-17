# Football runtime diagnostics

Collected on 2026-04-17.

## Runtime/config

- paused: false
- football enabled: true
- cs2 enabled: false
- dota enabled: false
- BOT_TOKEN configured: yes
- SIGNAL_CHAT_ID configured: yes
- AUTO_SIGNAL_PREVIEW_ONLY: true
- FOOTBALL_DEBUG_DISABLE_FILTER: false
- live provider configured: yes

## Current football run

- source: fallback (`the_odds_api` -> `manual_winline_json`)
- raw events fetched: 1
- normalized markets: 9
- candidates total: 9
- candidates after runtime/sport filter: 9
- candidates after football candidate filter + dedup: 7
- football send filter: not reached in current config (`preview_only=true`)
- final signals to send: 0
- messages actually sent: 0

## Diagnostic run with preview disabled

- candidates before send filter: 7
- after whitelist: 7
- after ranking: 7
- after family dedup: 3
- after per-match cap: 1
- final signals to send: 1
- created signals: 0
- messages actually sent: 0

Selected candidate:

- `Zenit vs Spartak -> result [Full Time Result (Zenit)] @2.15`

Drop reasons:

- blocked_family: 0
- low_score: 0
- dedup_family: 4
- cap_per_match: 2

## Delivery path

- notification_service reached in current config: no
- notification_service reached with preview disabled: no
- send_message called by football auto cycle: no
- exception in football auto send: none

Reason with preview disabled:

- selected football signal already exists in DB (`existing_signal_id=1`), so ingestion returns `created_signals=0`

## Telegram delivery separate check

- demo/manual send path: works
- sent messages: 4

## Polling check

- running bot processes: 1
- fresh `Conflict: terminated by other getUpdates request`: not found

## Main conclusion

User currently receives no football auto-signals because `AUTO_SIGNAL_PREVIEW_ONLY=true`.

Even after temporarily disabling preview mode for diagnostics, the chosen fallback football signal is already present in DB (`event_external_id=101001`, market `1x2`, selection `Zenit`), so no new signal is created and notification send is not called.
