"""Centralized rule tables for Winline raw line/result normalization.

Keep these tables easy to extend by hand. Bridges use them as the first layer,
then fall back to older heuristics.
"""

from __future__ import annotations

WINLINE_LINE_MARKET_TYPE_RULES = [
    {
        "id_tip_market": {"1", "17", "20"},
        "market_type": "1x2",
        "source": "rule:idTipMarket",
    },
    {
        "id_tip_market": {"2", "3", "18", "28"},
        "market_type": "total_goals",
        "source": "rule:idTipMarket",
    },
    {
        "id_tip_market": {"4", "5", "19", "29"},
        "market_type": "handicap",
        "source": "rule:idTipMarket",
    },
    {
        "id_tip_market": {"6", "26"},
        "market_type": "both_teams_to_score",
        "source": "rule:idTipMarket",
    },
    {
        "id_tip_market": {"7", "8", "30"},
        "market_type": "match_winner",
        "source": "rule:idTipMarket",
    },
    {
        "text_contains": ("обе забьют", "both teams", "btts"),
        "market_type": "both_teams_to_score",
        "source": "rule:text",
    },
    {
        "text_contains": ("фора", "handicap"),
        "market_type": "handicap",
        "source": "rule:text",
    },
    {
        "text_contains": ("тотал", "total", "over", "under"),
        "market_type": "total_goals",
        "source": "rule:text",
    },
    {
        "text_contains": ("1x2", "full time result", "исход"),
        "market_type": "1x2",
        "source": "rule:text",
    },
]

WINLINE_SECTION_RULES = [
    {
        "market_type": "1x2",
        "section_name": "Main",
        "subsection_name": "Full Time Result",
        "source": "rule:market_type",
    },
    {
        "market_type": "match_winner",
        "section_name": "Main",
        "subsection_name": "Match Winner",
        "source": "rule:market_type",
    },
    {
        "market_type": "total_goals",
        "section_name": "Totals",
        "subsection_name": "Goals",
        "source": "rule:market_type",
    },
    {
        "market_type": "handicap",
        "section_name": "Handicap",
        "subsection_name": "Handicap",
        "source": "rule:market_type",
    },
    {
        "market_type": "both_teams_to_score",
        "section_name": "Goals",
        "subsection_name": "BTTS",
        "source": "rule:market_type",
    },
    {
        "text_contains": ("обе забьют", "both teams", "btts"),
        "section_name": "Goals",
        "subsection_name": "BTTS",
        "source": "rule:text",
    },
]

WINLINE_SELECTION_RULES = [
    {"aliases": {"1", "home", "п1"}, "normalized": "HOME", "source": "rule:selection"},
    {"aliases": {"2", "away", "п2"}, "normalized": "AWAY", "source": "rule:selection"},
    {"aliases": {"x", "draw", "ничья"}, "normalized": "DRAW", "source": "rule:selection"},
    {
        "aliases": {"yes", "да", "оба забьют: да", "обе забьют: да"},
        "normalized": "YES",
        "source": "rule:selection",
    },
    {
        "aliases": {"no", "нет", "оба забьют: нет", "обе забьют: нет"},
        "normalized": "NO",
        "source": "rule:selection",
    },
    {"aliases": {"over", "больше"}, "normalized": "OVER", "source": "rule:selection"},
    {"aliases": {"under", "меньше"}, "normalized": "UNDER", "source": "rule:selection"},
]

WINLINE_RESULT_VOID_STATUS_RULES = [
    {"aliases": {"void"}, "is_void": True, "source": "rule:status"},
    {"aliases": {"cancelled", "canceled"}, "is_void": True, "source": "rule:status"},
    {"aliases": {"refund", "returned", "return"}, "is_void": True, "source": "rule:status"},
    {"aliases": {"annulled"}, "is_void": True, "source": "rule:status"},
]

WINLINE_RESULT_WINNER_RULES = [
    {"aliases": {"1", "home", "п1", "home_win"}, "normalized": "HOME", "source": "rule:winner"},
    {"aliases": {"2", "away", "п2", "away_win"}, "normalized": "AWAY", "source": "rule:winner"},
    {"aliases": {"x", "draw", "ничья"}, "normalized": "DRAW", "source": "rule:winner"},
    {"aliases": {"yes", "да"}, "normalized": "YES", "source": "rule:winner"},
    {"aliases": {"no", "нет"}, "normalized": "NO", "source": "rule:winner"},
    {"aliases": {"over", "больше"}, "normalized": "OVER", "source": "rule:winner"},
    {"aliases": {"under", "меньше"}, "normalized": "UNDER", "source": "rule:winner"},
]
