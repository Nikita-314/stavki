from __future__ import annotations

from enum import Enum


class SportType(str, Enum):
    CS2 = "CS2"
    DOTA2 = "DOTA2"
    FOOTBALL = "FOOTBALL"


class BookmakerType(str, Enum):
    FONBET = "FONBET"
    WINLINE = "WINLINE"
    BETBOOM = "BETBOOM"


class SignalStatus(str, Enum):
    NEW = "NEW"
    SENT = "SENT"
    ENTERED = "ENTERED"
    MISSED = "MISSED"
    SETTLED = "SETTLED"
    CANCELED = "CANCELED"


class EntryStatus(str, Enum):
    PENDING = "PENDING"
    ENTERED = "ENTERED"
    SKIPPED = "SKIPPED"
    REJECTED = "REJECTED"


class BetResult(str, Enum):
    WIN = "WIN"
    LOSE = "LOSE"
    VOID = "VOID"
    UNKNOWN = "UNKNOWN"


class FailureCategory(str, Enum):
    MODEL_ERROR = "MODEL_ERROR"
    EXECUTION_ERROR = "EXECUTION_ERROR"
    MARKET_UNAVAILABLE = "MARKET_UNAVAILABLE"
    LINE_MOVEMENT = "LINE_MOVEMENT"
    VARIANCE = "VARIANCE"
    DATA_ISSUE = "DATA_ISSUE"
    UNKNOWN = "UNKNOWN"

