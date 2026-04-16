from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from app.core.enums import SportType
from app.schemas.event_result import EventResultInput
from app.schemas.winline_raw import WinlineRawResultItem, WinlineRawResultPayload


class WinlineResultAdapter:
    """Normalize future Winline result payloads into settlement-ready event results.

    The real Winline result mapper will stay read-only and payload-driven. It should later be
    able to parse:
    - event status fields that show whether the event is final, cancelled, or still running
    - final score and/or winner-side fields from result payloads
    - void/cancelled/refund states represented by provider status codes or flags
    - settlement timestamp fields when Winline exposes them
    """

    def parse_result_payload(self, payload: dict[str, Any]) -> WinlineRawResultPayload:
        """Parse a future Winline settlement payload into normalized raw result items.

        Expected normalized result fields:
        - `event_external_id`
        - `winner_selection`
        - `is_void`
        - `settled_at`

        Practical mapper plan once DevTools payloads are captured:
        - find where event status lives in the result payload
        - find whether the payload exposes final score, winner side, or both
        - determine how cancelled/void states are represented
        - preserve provider timestamps if there is an explicit settled/finalized field

        The normalized raw result schema stays intentionally small. The full provider item is
        preserved in `raw_json`, so sport, status, and winner logic can be refined later
        without changing the settlement pipeline contract.
        """
        if not isinstance(payload, dict):
            return WinlineRawResultPayload(source_name='winline', results=[])

        results_raw = payload.get('results')
        if not isinstance(results_raw, list):
            return WinlineRawResultPayload(source_name='winline', results=[])

        results: list[WinlineRawResultItem] = []
        for item in results_raw:
            if not isinstance(item, dict):
                continue
            # TODO: event_external_id -> real result event id path; skip result if missing.
            # TODO: winner_selection -> real winner side path or derive from result code/score.
            # TODO: is_void -> real void/cancelled flag path or derive from status.
            # TODO: settled_at -> real settled/finished timestamp path; leave None if absent.
            # TODO: keep full raw item in raw_json for later winner/sport debugging.
            try:
                results.append(WinlineRawResultItem.model_validate({**item, 'raw_json': item}))
            except (ValidationError, TypeError, ValueError):
                continue

        return WinlineRawResultPayload(source_name='winline', results=results)

    def to_event_results(self, raw: WinlineRawResultPayload) -> list[EventResultInput]:
        event_results: list[EventResultInput] = []

        for item in raw.results:
            sport = self._extract_sport(item)
            if sport is None:
                continue

            try:
                event_results.append(
                    EventResultInput(
                        event_external_id=item.event_external_id,
                        sport=sport,
                        winner_selection=item.winner_selection,
                        is_void=bool(item.is_void),
                        settled_at=item.settled_at,
                        result_payload_json=item.raw_json,
                    )
                )
            except (ValidationError, TypeError, ValueError):
                continue

        return event_results

    def _extract_sport(self, item: WinlineRawResultItem) -> SportType | None:
        raw = item.raw_json or {}
        # TODO: sport -> replace fallback keys below with the real result sport path from DevTools.
        # TODO: if sport is nested under event metadata, read that path here before mapping.
        value = raw.get('sport') or raw.get('sport_name') or raw.get('sport_slug') or raw.get('sport_key')
        if not isinstance(value, str):
            return None
        return self._map_sport(value)

    def _map_sport(self, value: str) -> SportType | None:
        normalized = ' '.join((value or '').strip().lower().replace('-', ' ').replace('_', ' ').split())
        if normalized in {'football', 'soccer'}:
            return SportType.FOOTBALL
        if normalized in {'cs2', 'cs 2', 'counter strike', 'counter strike 2'}:
            return SportType.CS2
        if normalized in {'dota2', 'dota 2'}:
            return SportType.DOTA2
        return None
