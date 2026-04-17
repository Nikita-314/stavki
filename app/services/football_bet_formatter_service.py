from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class FootballBetPresentation:
    main_label: str
    detail_label: str | None = None


class FootballBetFormatterService:
    _TEAM_TRANSLIT = {
        "Zenit": "Зенит",
        "Spartak": "Спартак",
        "Liverpool": "Ливерпуль",
        "Everton": "Эвертон",
    }
    _HOME_TOKENS = {"HOME", "1", "P1", "П1", "HOME TEAM"}
    _AWAY_TOKENS = {"AWAY", "2", "P2", "П2", "AWAY TEAM"}
    _DRAW_TOKENS = {"DRAW", "X", "Х", "Н", "НИЧЬЯ"}
    _YES_TOKENS = {"YES", "ДА", "Y"}
    _NO_TOKENS = {"NO", "НЕТ", "N"}
    _OVER_TOKENS = {"OVER", "БОЛЬШЕ", "TB", "ТБ", "O"}
    _UNDER_TOKENS = {"UNDER", "МЕНЬШЕ", "TM", "ТМ", "U"}
    _ODD_TOKENS = {"ODD", "НЕЧЕТ", "НЕЧЁТ", "НЕЧЕТНЫЙ", "НЕЧЁТНЫЙ"}
    _EVEN_TOKENS = {"EVEN", "ЧЕТ", "ЧЁТ", "ЧЕТНЫЙ", "ЧЁТНЫЙ"}
    _SHORT_TOKEN_ALIASES = {"Y", "N", "O", "U"}

    def format_bet(
        self,
        *,
        market_type: str | None,
        market_label: str | None,
        selection: str | None,
        home_team: str | None = None,
        away_team: str | None = None,
        section_name: str | None = None,
        subsection_name: str | None = None,
    ) -> FootballBetPresentation:
        market_type_l = (market_type or "").strip().lower()
        market_label_s = (market_label or "").strip()
        selection_s = (selection or "").strip()
        section_s = (section_name or "").strip()
        subsection_s = (subsection_name or "").strip()
        combined_text = " | ".join(
            x for x in [market_type_l, market_label_s.lower(), selection_s.lower(), section_s.lower(), subsection_s.lower()] if x
        )

        if self._is_time_match_total(market_type_l, market_label_s, selection_s):
            combo = self._format_time_match_total(selection_s or market_label_s)
            if combo:
                return FootballBetPresentation(main_label="Тайм/матч + тотал", detail_label=combo)

        if self._is_time_match(market_type_l, market_label_s, selection_s):
            combo = self._normalize_result_combo(selection_s or market_label_s)
            if combo:
                return FootballBetPresentation(main_label=f"Тайм/матч: {combo}")

        period = self._detect_period(section_s, subsection_s, market_label_s)

        if self._is_double_chance(market_type_l, market_label_s, selection_s):
            dc = self._normalize_double_chance(selection_s or market_label_s)
            if dc:
                return FootballBetPresentation(main_label=self._with_period(f"Двойной шанс: {dc}", period))

        if self._is_btts(market_type_l, market_label_s):
            yes_no = self._normalize_yes_no(selection_s)
            label = f"Обе забьют: {yes_no}" if yes_no else "Обе забьют"
            return FootballBetPresentation(main_label=self._with_period(label, period))

        if self._is_total(market_type_l, market_label_s, selection_s):
            total_label = self._format_total(
                market_label=market_label_s,
                selection=selection_s,
                home_team=home_team,
                away_team=away_team,
            )
            if total_label:
                return FootballBetPresentation(main_label=self._with_period(total_label, period))

        if self._is_handicap(market_type_l, market_label_s):
            handicap_label = self._format_handicap(
                market_label=market_label_s,
                selection=selection_s,
                home_team=home_team,
                away_team=away_team,
            )
            if handicap_label:
                return FootballBetPresentation(main_label=self._with_period(handicap_label, period))

        if self._is_correct_score(market_type_l, market_label_s, selection_s):
            score = self._extract_score(selection_s or market_label_s)
            if score:
                return FootballBetPresentation(main_label=f"Точный счёт: {score}")
            if "other" in combined_text or "друг" in combined_text:
                return FootballBetPresentation(main_label="Точный счёт: другой")

        if self._is_win_margin(market_type_l, market_label_s):
            margin = self._format_win_margin(
                market_label=market_label_s,
                selection=selection_s,
                home_team=home_team,
                away_team=away_team,
            )
            if margin:
                return FootballBetPresentation(main_label=f"Победа с разницей: {margin}")

        if self._is_odd_even(market_type_l, market_label_s):
            odd_even = self._normalize_odd_even(selection_s)
            if odd_even:
                return FootballBetPresentation(main_label=f"Чёт/нечёт: {odd_even}")

        if self._is_clean_sheet(market_type_l, market_label_s):
            clean_sheet = self._format_clean_sheet(
                market_label=market_label_s,
                selection=selection_s,
                home_team=home_team,
                away_team=away_team,
            )
            if clean_sheet:
                return FootballBetPresentation(main_label=clean_sheet)

        if self._is_any_team_to_win(market_type_l, market_label_s):
            yes_no = self._normalize_yes_no(selection_s)
            if yes_no:
                return FootballBetPresentation(main_label=f"Любая команда победит: {yes_no}")

        if self._is_outcome(market_type_l, market_label_s):
            outcome = self._normalize_outcome_token(selection_s)
            if outcome:
                return FootballBetPresentation(
                    main_label=self._with_period("Исход", period),
                    detail_label=outcome,
                )

        fallback = self._format_fallback(
            market_label=market_label_s,
            selection=selection_s,
            home_team=home_team,
            away_team=away_team,
        )
        return FootballBetPresentation(main_label=fallback)

    def _is_outcome(self, market_type: str, market_label: str) -> bool:
        label = market_label.lower()
        return market_type in {"1x2", "match_winner"} or label in {
            "match result",
            "full time result",
            "исход",
        }

    def _is_double_chance(self, market_type: str, market_label: str, selection: str) -> bool:
        if market_type in {"double_chance", "double chance"}:
            return True
        combined = f"{market_label} {selection}".upper()
        return any(token in combined for token in ("DOUBLE CHANCE", "ДВОЙНОЙ ШАНС", "1X", "12", "X2", "1Х", "Х2"))

    def _is_total(self, market_type: str, market_label: str, selection: str) -> bool:
        combined = f"{market_label} {selection}".lower()
        return market_type == "total_goals" or "тотал" in combined or "total" in combined

    def _is_handicap(self, market_type: str, market_label: str) -> bool:
        combined = f"{market_type} {market_label}".lower()
        return market_type == "handicap" or "handicap" in combined or "фора" in combined

    def _is_btts(self, market_type: str, market_label: str) -> bool:
        combined = f"{market_type} {market_label}".lower()
        return market_type == "both_teams_to_score" or "both teams" in combined or "обе забьют" in combined

    def _is_time_match(self, market_type: str, market_label: str, selection: str) -> bool:
        combined = f"{market_type} {market_label} {selection}".lower()
        return any(token in combined for token in ("тайм/матч", "half time/full time", "half/full time"))

    def _is_time_match_total(self, market_type: str, market_label: str, selection: str) -> bool:
        combined = f"{market_type} {market_label} {selection}".lower()
        return "/" in combined and "+" in combined and any(token in combined for token in ("over", "under", "тб", "тм", "больше", "меньше"))

    def _is_correct_score(self, market_type: str, market_label: str, selection: str) -> bool:
        combined = f"{market_type} {market_label} {selection}".lower()
        return "correct score" in combined or "точный сч" in combined or bool(self._extract_score(selection or market_label))

    def _is_win_margin(self, market_type: str, market_label: str) -> bool:
        combined = f"{market_type} {market_label}".lower()
        return "margin" in combined or "разниц" in combined

    def _is_odd_even(self, market_type: str, market_label: str) -> bool:
        combined = f"{market_type} {market_label}".lower()
        return "odd" in combined or "even" in combined or "чет" in combined or "чёт" in combined

    def _is_clean_sheet(self, market_type: str, market_label: str) -> bool:
        combined = f"{market_type} {market_label}".lower()
        return "clean sheet" in combined or "не пропуст" in combined

    def _is_any_team_to_win(self, market_type: str, market_label: str) -> bool:
        combined = f"{market_type} {market_label}".lower()
        return "any team" in combined or "любая команда победит" in combined

    def _detect_period(self, *values: str) -> str | None:
        text = " ".join(v.lower() for v in values if v)
        if any(token in text for token in ("1 тайм", "1-й тайм", "first half", "1st half")):
            return "1 тайм"
        if any(token in text for token in ("2 тайм", "2-й тайм", "second half", "2nd half")):
            return "2 тайм"
        if any(token in text for token in ("основное время", "full time", "match")):
            return None
        return None

    def _normalize_outcome_token(self, raw: str) -> str | None:
        token = self._clean_token(raw).upper()
        if token in self._HOME_TOKENS:
            return "П1"
        if token in self._AWAY_TOKENS:
            return "П2"
        if token in self._DRAW_TOKENS:
            return "Х"
        return None

    def _normalize_combo_token(self, raw: str) -> str | None:
        token = self._clean_token(raw).upper()
        if token in self._HOME_TOKENS:
            return "П1"
        if token in self._AWAY_TOKENS:
            return "П2"
        if token in self._DRAW_TOKENS:
            return "Н"
        return None

    def _normalize_double_chance(self, raw: str) -> str | None:
        token = self._clean_token(raw).upper().replace(" ", "")
        token = token.replace("HOME", "1").replace("AWAY", "2").replace("DRAW", "X")
        token = token.replace("П1", "1").replace("П2", "2").replace("Х", "X")
        if token in {"1X", "12", "X2"}:
            return token.replace("X", "Х")
        return None

    def _normalize_yes_no(self, raw: str) -> str | None:
        token = self._clean_token(raw).upper()
        if token in self._YES_TOKENS:
            return "да"
        if token in self._NO_TOKENS:
            return "нет"
        if self._contains_alias_token(token, self._YES_TOKENS):
            return "да"
        if self._contains_alias_token(token, self._NO_TOKENS):
            return "нет"
        return None

    def _normalize_odd_even(self, raw: str) -> str | None:
        token = self._clean_token(raw).upper()
        if token in self._ODD_TOKENS:
            return "нечёт"
        if token in self._EVEN_TOKENS:
            return "чёт"
        if self._contains_alias_token(token, self._ODD_TOKENS):
            return "нечёт"
        if self._contains_alias_token(token, self._EVEN_TOKENS):
            return "чёт"
        return None

    def _normalize_total_side(self, raw: str) -> str | None:
        token = self._clean_token(raw).upper()
        if token in self._OVER_TOKENS:
            return "ТБ"
        if token in self._UNDER_TOKENS:
            return "ТМ"
        if self._contains_alias_token(token, self._OVER_TOKENS):
            return "ТБ"
        if self._contains_alias_token(token, self._UNDER_TOKENS):
            return "ТМ"
        return None

    def _contains_alias_token(self, token: str, aliases: set[str]) -> bool:
        words = {part for part in re.split(r"[^A-ZА-Я0-9Ё]+", token) if part}
        for alias in aliases:
            alias_u = alias.upper()
            if alias_u in self._SHORT_TOKEN_ALIASES:
                if alias_u in words:
                    return True
                continue
            if alias_u in token:
                return True
        return False

    def _format_total(
        self,
        *,
        market_label: str,
        selection: str,
        home_team: str | None,
        away_team: str | None,
    ) -> str | None:
        total_side = self._normalize_total_side(selection) or self._normalize_total_side(market_label)
        total_value = self._extract_numeric_value(selection) or self._extract_numeric_value(market_label)
        label_l = market_label.lower()
        selection_l = selection.lower()
        team = self._extract_team_reference(
            f"{market_label} {selection}",
            home_team=home_team,
            away_team=away_team,
        )
        if "team total" in label_l or "индивидуаль" in label_l:
            prefix = f"Тотал {team}" if team else "Командный тотал"
            if total_side and total_value:
                return f"{prefix} {total_side} {total_value}"
            return prefix
        if total_side == "ТБ" and total_value:
            return f"Тотал больше {total_value}"
        if total_side == "ТМ" and total_value:
            return f"Тотал меньше {total_value}"
        if total_side and total_value:
            return f"{total_side} {total_value}"
        if "more" in selection_l or "больше" in selection_l:
            return f"Тотал больше {total_value}".strip()
        if "less" in selection_l or "меньше" in selection_l:
            return f"Тотал меньше {total_value}".strip()
        return None

    def _format_handicap(
        self,
        *,
        market_label: str,
        selection: str,
        home_team: str | None,
        away_team: str | None,
    ) -> str | None:
        text = f"{market_label} {selection}"
        value = self._extract_signed_value(selection) or self._extract_signed_value(market_label)
        team = self._extract_team_reference(text, home_team=home_team, away_team=away_team)
        if team and value:
            return f"Фора {team} {value}"
        if team:
            return f"Фора {team}"
        if value:
            return f"Фора {value}"
        return None

    def _format_time_match_total(self, raw: str) -> str | None:
        if "+" not in raw:
            return None
        left, right = [part.strip() for part in raw.split("+", 1)]
        combo = self._normalize_result_combo(left)
        total = self._format_total(market_label="", selection=right, home_team=None, away_team=None)
        if combo and total:
            total_short = total.replace("Тотал больше", "ТБ").replace("Тотал меньше", "ТМ")
            return f"{combo} + {total_short}"
        return None

    def _normalize_result_combo(self, raw: str) -> str | None:
        if "/" not in raw:
            return None
        parts = [self._normalize_combo_token(part) for part in raw.split("/", 1)]
        if any(part is None for part in parts):
            return None
        return f"{parts[0]}/{parts[1]}"

    def _format_win_margin(
        self,
        *,
        market_label: str,
        selection: str,
        home_team: str | None,
        away_team: str | None,
    ) -> str | None:
        text = f"{market_label} {selection}".lower()
        if "draw" in text or "нич" in text:
            return "Ничья"
        team = self._extract_team_reference(f"{market_label} {selection}", home_team=home_team, away_team=away_team)
        margin = self._extract_margin(selection) or self._extract_margin(market_label)
        if team and margin:
            return f"{team} {margin}"
        if margin:
            return margin
        return None

    def _format_clean_sheet(
        self,
        *,
        market_label: str,
        selection: str,
        home_team: str | None,
        away_team: str | None,
    ) -> str | None:
        team = self._extract_team_reference(market_label, home_team=home_team, away_team=away_team)
        yes_no = self._normalize_yes_no(selection)
        if team and yes_no:
            return f"Не пропустит {team}: {yes_no}"
        if yes_no:
            return f"Не пропустит: {yes_no}"
        return None

    def _format_fallback(
        self,
        *,
        market_label: str,
        selection: str,
        home_team: str | None,
        away_team: str | None,
    ) -> str:
        selection_pretty = self._prettify_selection(selection, home_team=home_team, away_team=away_team)
        label = market_label.strip()
        if "+" in selection_pretty or "/" in selection_pretty:
            basis = selection_pretty or label
            return f"Комбинированный рынок — {basis}".strip()
        if label and selection_pretty:
            return f"{label}: {selection_pretty}"
        return selection_pretty or label or "Ставка"

    def _prettify_selection(self, raw: str, *, home_team: str | None, away_team: str | None) -> str:
        if not raw:
            return ""
        combo = self._normalize_result_combo(raw)
        if combo:
            return combo
        double = self._normalize_double_chance(raw)
        if double:
            return double
        outcome = self._normalize_outcome_token(raw)
        if outcome:
            return outcome
        yes_no = self._normalize_yes_no(raw)
        if yes_no:
            return yes_no
        odd_even = self._normalize_odd_even(raw)
        if odd_even:
            return odd_even
        total_side = self._normalize_total_side(raw)
        if total_side:
            value = self._extract_numeric_value(raw)
            return f"{total_side} {value}".strip()
        if "HOME" in raw.upper() and home_team:
            return raw.upper().replace("HOME", home_team)
        if "AWAY" in raw.upper() and away_team:
            return raw.upper().replace("AWAY", away_team)
        return raw.strip()

    def _extract_team_reference(self, text: str, *, home_team: str | None, away_team: str | None) -> str | None:
        raw = (text or "").strip()
        upper = raw.upper()
        home = self._humanize_team(home_team)
        away = self._humanize_team(away_team)
        if home and home.lower() in raw.lower():
            return home
        if away and away.lower() in raw.lower():
            return away
        if any(token in upper for token in self._HOME_TOKENS):
            return home or "хозяева"
        if any(token in upper for token in self._AWAY_TOKENS):
            return away or "гости"
        return None

    def _extract_score(self, raw: str) -> str | None:
        match = re.search(r"\b(\d+:\d+)\b", raw or "")
        return match.group(1) if match else None

    def _extract_numeric_value(self, raw: str) -> str | None:
        match = re.search(r"([0-9]+(?:[.,][0-9]+)?)", raw or "")
        if not match:
            return None
        return match.group(1).replace(",", ".")

    def _extract_signed_value(self, raw: str) -> str | None:
        match = re.search(r"([+-]\d+(?:[.,]\d+)?)", raw or "")
        if not match:
            return None
        return match.group(1).replace(",", ".")

    def _extract_margin(self, raw: str) -> str | None:
        match = re.search(r"(\d\+?|\d+\+?)", raw or "")
        if not match:
            return None
        return match.group(1)

    def _with_period(self, label: str, period: str | None) -> str:
        if not period:
            return label
        return f"{period}: {label[0].lower() + label[1:]}" if label else period

    def _clean_token(self, raw: str) -> str:
        return re.sub(r"[\s_()-]+", " ", (raw or "").strip()).strip()

    def _humanize_team(self, raw: str | None) -> str:
        text = (raw or "").strip()
        return self._TEAM_TRANSLIT.get(text, text)
