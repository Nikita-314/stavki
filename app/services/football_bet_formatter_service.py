from __future__ import annotations

import re
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class FootballBetPresentation:
    main_label: str
    detail_label: str | None = None
    detected_special_scope: str | None = None
    detected_period_scope: str | None = None


@dataclass(frozen=True)
class FootballTotalContext:
    total_scope: str
    period_scope: str
    target_scope: str
    team_name: str | None
    total_line: str | None
    total_side: str | None
    normalized_text: str


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

        scope_blob = " ".join(x for x in [section_s, subsection_s, market_label_s, selection_s] if x)
        inferred_period_scope = self._detect_period_scope(scope_blob)

        pres = self._format_bet_body(
            market_type_l=market_type_l,
            market_label_s=market_label_s,
            selection_s=selection_s,
            section_s=section_s,
            subsection_s=subsection_s,
            combined_text=combined_text,
            home_team=home_team,
            away_team=away_team,
            inferred_period_scope=inferred_period_scope,
        )
        return self._attach_scope_defaults(pres, inferred_period_scope=inferred_period_scope)

    def _attach_scope_defaults(
        self,
        pres: FootballBetPresentation,
        *,
        inferred_period_scope: str,
    ) -> FootballBetPresentation:
        if pres.detected_period_scope is None:
            return replace(pres, detected_period_scope=inferred_period_scope)
        return pres

    def _format_bet_body(
        self,
        *,
        market_type_l: str,
        market_label_s: str,
        selection_s: str,
        section_s: str,
        subsection_s: str,
        combined_text: str,
        home_team: str | None,
        away_team: str | None,
        inferred_period_scope: str,
    ) -> FootballBetPresentation:
        clean_ml = self._strip_winline_markers(market_label_s)
        special_scope = self._detect_special_scope(section_s, subsection_s, market_label_s, selection_s)

        if self._is_time_match_total(market_type_l, market_label_s, selection_s):
            combo = self._format_time_match_total(selection_s or market_label_s)
            if combo:
                return FootballBetPresentation(main_label="Тайм/матч + тотал", detail_label=combo)

        if self._is_time_match(market_type_l, market_label_s, selection_s):
            combo = self._normalize_result_combo(selection_s or market_label_s)
            if combo:
                return FootballBetPresentation(main_label=f"Тайм/матч: {combo}")

        period = self._detect_period(section_s, subsection_s, market_label_s)

        if special_scope == "corners":
            return self._format_corners_family(
                market_type_l=market_type_l,
                market_label_raw=market_label_s,
                market_label_clean=clean_ml,
                selection_s=selection_s,
                section_s=section_s,
                subsection_s=subsection_s,
                detected_period_scope=inferred_period_scope,
                home_team=home_team,
                away_team=away_team,
            )

        # 3-way European handicaps: must not be formatted as «Исход»+yes/no via fallback
        if special_scope != "corners" and self._is_european_handicap_3way_label(
            market_label_s, market_type_l
        ):
            if self._normalize_yes_no(selection_s):
                return FootballBetPresentation(
                    main_label="(служебно) Европейский гандикап: исход да/нет, не 1-Х-2"
                )
            d = self._european_3way_detail(
                selection_s, home_team=home_team, away_team=away_team
            )
            if d:
                return FootballBetPresentation(
                    main_label=self._with_period(f"Европейский гандикап: {d}", period)
                )
            pretty = self._prettify_selection(
                selection_s, home_team=home_team, away_team=away_team
            ) or (selection_s or "—")
            if self._normalize_yes_no(pretty):
                return FootballBetPresentation(
                    main_label="(служебно) Европейский гандикап: исход да/нет, не 1-Х-2"
                )
            return FootballBetPresentation(
                main_label=self._with_period(f"Европейский гандикап: {pretty}", period)
            )

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
                return FootballBetPresentation(main_label=total_label)

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

    def _strip_winline_markers(self, text: str) -> str:
        if not text:
            return ""
        s = str(text)
        s = re.sub(r"\[a\]", "", s, flags=re.I)
        s = re.sub(r"@(?:NP|1HT|2HT|1|2)@", "", s, flags=re.I)
        s = re.sub(r"\(\s*\)", "", s)
        s = re.sub(r"\s+", " ", s).strip(" :|")
        return s

    def _detect_special_scope(
        self,
        section_name: str,
        subsection_name: str,
        market_label: str,
        selection: str,
    ) -> str | None:
        blob = " ".join(x for x in [section_name, subsection_name, market_label, selection] if x).lower()
        if self._has_corner_market_keyword(blob):
            return "corners"
        if any(token in blob for token in ("карточ", "booking")):
            return "cards"
        return None

    def _has_corner_market_keyword(self, text: str) -> bool:
        lowered = (text or "").lower()
        return "углов" in lowered or "corner" in lowered

    def _format_corners_family(
        self,
        *,
        market_type_l: str,
        market_label_raw: str,
        market_label_clean: str,
        selection_s: str,
        section_s: str,
        subsection_s: str,
        detected_period_scope: str,
        home_team: str | None,
        away_team: str | None,
    ) -> FootballBetPresentation:
        meta = {"detected_special_scope": "corners", "detected_period_scope": detected_period_scope}

        if self._is_corner_total_candidate(market_type_l, market_label_raw, market_label_clean, selection_s):
            line = self._format_corner_total_line(
                market_type_l=market_type_l,
                market_label=market_label_raw,
                selection=selection_s,
                home_team=home_team,
                away_team=away_team,
                section_s=section_s,
                subsection_s=subsection_s,
            )
            if line:
                return FootballBetPresentation(main_label=line, **meta)

        if self._is_corner_handicap_candidate(market_type_l, market_label_clean, selection_s):
            line = self._format_corner_handicap_line(
                market_label=market_label_clean,
                selection=selection_s,
                home_team=home_team,
                away_team=away_team,
                detected_period_scope=detected_period_scope,
            )
            if line:
                return FootballBetPresentation(main_label=line, **meta)

        if self._is_double_chance(market_type_l, market_label_clean, selection_s):
            dc = self._normalize_double_chance(selection_s or market_label_clean)
            if dc:
                suffix = self._corner_half_sentence_suffix(detected_period_scope)
                text = f"Двойной шанс по угловым: {dc}{suffix}"
                return FootballBetPresentation(main_label=text, **meta)

        if self._is_corners_match_winner_family(market_type_l, market_label_clean):
            line = self._format_corner_match_winner(selection_s, home_team, away_team, detected_period_scope)
            if line:
                return FootballBetPresentation(main_label=line, **meta)

        fb = self._format_corner_fallback(
            market_label_clean=market_label_clean,
            selection_s=selection_s,
            home_team=home_team,
            away_team=away_team,
            detected_period_scope=detected_period_scope,
        )
        return FootballBetPresentation(main_label=fb, **meta)

    def _corner_half_sentence_suffix(self, detected_period_scope: str) -> str:
        if detected_period_scope == "first_half":
            return " в 1-м тайме"
        if detected_period_scope == "second_half":
            return " во 2-м тайме"
        return ""

    def _is_corner_total_candidate(self, market_type_l: str, market_label_raw: str, market_label_clean: str, selection_s: str) -> bool:
        blob = f"{market_label_raw} {market_label_clean} {selection_s}".lower()
        if not self._has_corner_market_keyword(blob):
            return False
        if self._is_total(market_type_l, market_label_raw, selection_s) or self._is_total(market_type_l, market_label_clean, selection_s):
            return True
        return self._looks_like_corner_total_line(market_type_l, market_label_raw, market_label_clean, selection_s)

    def _looks_like_corner_total_line(self, market_type_l: str, market_label_raw: str, market_label_clean: str, selection_s: str) -> bool:
        blob = f"{market_label_raw} {market_label_clean} {selection_s}".lower()
        if not self._has_corner_market_keyword(blob):
            return False
        side = self._normalize_total_side(selection_s) or self._normalize_total_side(market_label_raw) or self._normalize_total_side(market_label_clean)
        line = self._extract_numeric_value(selection_s) or self._extract_numeric_value(market_label_raw) or self._extract_numeric_value(market_label_clean)
        return bool(side and line)

    def _format_corner_total_line(
        self,
        *,
        market_type_l: str,
        market_label: str,
        selection: str,
        home_team: str | None,
        away_team: str | None,
        section_s: str,
        subsection_s: str,
    ) -> str | None:
        ctx = self._describe_corner_total_context(
            market_type=market_type_l,
            market_label=market_label,
            selection=selection,
            home_team=home_team,
            away_team=away_team,
            section_name=section_s,
            subsection_name=subsection_s,
        )
        return ctx

    def _describe_corner_total_context(
        self,
        *,
        market_type: str | None,
        market_label: str | None,
        selection: str | None,
        home_team: str | None,
        away_team: str | None,
        section_name: str | None,
        subsection_name: str | None,
    ) -> str | None:
        market_type_l = (market_type or "").strip().lower()
        market_label_s = (market_label or "").strip()
        selection_s = (selection or "").strip()
        clean_ml = self._strip_winline_markers(market_label_s)
        combined = " ".join(
            value for value in [market_label_s, selection_s, section_name or "", subsection_name or ""] if value
        )
        if not self._has_corner_market_keyword(combined):
            return None
        if not (
            self._is_total(market_type_l, market_label_s, selection_s)
            or self._is_total(market_type_l, clean_ml, selection_s)
            or self._looks_like_corner_total_line(market_type_l, market_label_s, clean_ml, selection_s)
        ):
            return None
        period_scope = self._detect_period_scope(combined)
        team_name = self._extract_team_reference(combined, home_team=home_team, away_team=away_team)
        target_scope = self._detect_total_target_scope(
            text=combined,
            team_name=team_name,
            home_team=home_team,
            away_team=away_team,
        )
        total_side = self._normalize_total_side(selection_s) or self._normalize_total_side(market_label_s) or self._normalize_total_side(clean_ml)
        total_line = self._extract_numeric_value(selection_s) or self._extract_numeric_value(market_label_s) or self._extract_numeric_value(clean_ml)
        return self._render_corner_total_context(
            period_scope=period_scope,
            target_scope=target_scope,
            team_name=team_name,
            total_side=total_side,
            total_line=total_line,
        )

    def _render_corner_total_context(
        self,
        *,
        period_scope: str,
        target_scope: str,
        team_name: str | None,
        total_side: str | None,
        total_line: str | None,
    ) -> str:
        if total_side == "ТБ":
            side_text = "больше"
        elif total_side == "ТМ":
            side_text = "меньше"
        else:
            side_text = None

        period_prefix = ""
        if period_scope == "first_half":
            period_prefix = "1-й тайм: "
        elif period_scope == "second_half":
            period_prefix = "2-й тайм: "

        corner_total_word = "тотал угловых" if period_prefix else "Тотал угловых"

        if target_scope in {"home_team", "away_team", "team_total"}:
            target = team_name or "команды"
            base = f"{period_prefix}{corner_total_word} {target}".strip()
        else:
            base = f"{period_prefix}{corner_total_word}".strip()

        if side_text and total_line:
            return f"{base} {side_text} {total_line}"
        if total_line:
            return f"{base} {total_line}"
        return base

    def _is_corner_handicap_candidate(self, market_type_l: str, market_label_clean: str, selection_s: str) -> bool:
        blob = f"{market_label_clean} {selection_s}".lower()
        if not self._has_corner_market_keyword(blob):
            return False
        if self._is_handicap(market_type_l, market_label_clean):
            return True
        return bool(self._extract_signed_value(selection_s) or self._extract_signed_value(market_label_clean))

    def _format_corner_handicap_line(
        self,
        *,
        market_label: str,
        selection: str,
        home_team: str | None,
        away_team: str | None,
        detected_period_scope: str,
    ) -> str | None:
        text = f"{market_label} {selection}"
        value = self._extract_signed_value(selection) or self._extract_signed_value(market_label)
        team = self._extract_team_reference(text, home_team=home_team, away_team=away_team)
        suffix = self._corner_half_sentence_suffix(detected_period_scope)
        if team and value:
            return f"Фора по угловым {team} {value}{suffix}".strip()
        if team:
            return f"Фора по угловым {team}{suffix}".strip()
        if value:
            return f"Фора по угловым {value}{suffix}".strip()
        return None

    def _is_corners_match_winner_family(self, market_type_l: str, market_label_clean: str) -> bool:
        ll = market_label_clean.lower()
        if not self._has_corner_market_keyword(ll):
            return False
        if market_type_l in {"1x2", "match_winner"}:
            return True
        compact = ll.replace(" ", "")
        if "1x2" in compact or "1х2" in compact:
            return True
        if "исход" in ll:
            return True
        if "match result" in ll:
            return True
        return False

    def _format_corner_match_winner(
        self,
        selection_s: str,
        home_team: str | None,
        away_team: str | None,
        detected_period_scope: str,
    ) -> str | None:
        suffix = self._corner_half_sentence_suffix(detected_period_scope)
        resolved = self._corner_resolve_match_winner(selection_s, home_team, away_team)
        if resolved == "draw":
            return f"Ничья по угловым{suffix}"
        if isinstance(resolved, tuple) and resolved[0] == "team":
            return f"{resolved[1]} победит по угловым{suffix}"
        return None

    def _corner_resolve_match_winner(
        self,
        selection_s: str,
        home_team: str | None,
        away_team: str | None,
    ) -> str | tuple[str, str]:
        raw = (selection_s or "").strip()
        if not raw:
            return "unknown"
        ot = self._normalize_outcome_token(raw)
        if ot == "Х":
            return "draw"
        home_h = self._humanize_team(home_team)
        away_h = self._humanize_team(away_team)
        if ot == "П1" and home_h:
            return ("team", home_h)
        if ot == "П2" and away_h:
            return ("team", away_h)
        low = raw.lower()
        if home_h:
            if home_h.lower() == low or home_h.lower() in low:
                return ("team", home_h)
        if away_h:
            if away_h.lower() == low or away_h.lower() in low:
                return ("team", away_h)
        if low in {"ничья", "ничью"} or raw.upper() in self._DRAW_TOKENS:
            return "draw"
        return "unknown"

    def _format_corner_fallback(
        self,
        *,
        market_label_clean: str,
        selection_s: str,
        home_team: str | None,
        away_team: str | None,
        detected_period_scope: str,
    ) -> str:
        suffix = self._corner_half_sentence_suffix(detected_period_scope)
        pretty = self._prettify_selection(selection_s, home_team=home_team, away_team=away_team)
        if pretty:
            return f"Рынок по угловым — {pretty}{suffix}".strip()
        rest = self._strip_winline_markers(market_label_clean).strip(" :|")
        if rest:
            return f"Рынок по угловым — {rest}{suffix}".strip()
        return f"Рынок по угловым{suffix}".strip()

    def describe_total_context(
        self,
        *,
        market_type: str | None,
        market_label: str | None,
        selection: str | None,
        home_team: str | None = None,
        away_team: str | None = None,
        section_name: str | None = None,
        subsection_name: str | None = None,
    ) -> FootballTotalContext | None:
        market_type_l = (market_type or "").strip().lower()
        market_label_s = (market_label or "").strip()
        selection_s = (selection or "").strip()
        if not self._is_total(market_type_l, market_label_s, selection_s):
            return None
        combined = " ".join(
            value for value in [market_label_s, selection_s, section_name or "", subsection_name or ""] if value
        )
        period_scope = self._detect_period_scope(combined)
        team_name = self._extract_team_reference(combined, home_team=home_team, away_team=away_team)
        target_scope = self._detect_total_target_scope(
            text=combined,
            team_name=team_name,
            home_team=home_team,
            away_team=away_team,
        )
        total_side = self._normalize_total_side(selection_s) or self._normalize_total_side(market_label_s)
        total_line = self._extract_numeric_value(selection_s) or self._extract_numeric_value(market_label_s)
        total_scope = self._compose_total_scope(period_scope=period_scope, target_scope=target_scope)
        return FootballTotalContext(
            total_scope=total_scope,
            period_scope=period_scope,
            target_scope=target_scope,
            team_name=team_name,
            total_line=total_line,
            total_side=total_side,
            normalized_text=self._render_total_context(
                period_scope=period_scope,
                target_scope=target_scope,
                team_name=team_name,
                total_side=total_side,
                total_line=total_line,
            ),
        )

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

    def _is_european_handicap_3way_label(self, market_label: str, market_type_l: str) -> bool:
        """Winline often tags 3-way EU handicaps as match_winner + label, not as handicap type."""
        ml = (market_label or "").lower()
        if not (("европ" in ml or "european" in ml) and ("гандик" in ml or "handicap" in ml)):
            return False
        mtl = (market_type_l or "").lower()
        return mtl in ("1x2", "match_winner")

    def _european_3way_detail(
        self,
        selection_s: str,
        *,
        home_team: str | None,
        away_team: str | None,
    ) -> str | None:
        s = (selection_s or "").strip()
        if not s or self._normalize_yes_no(s):
            return None
        o = self._normalize_outcome_token(s)
        if o == "Х" or s.upper() in self._DRAW_TOKENS or s.lower() in ("draw", "ничья"):
            return "Х"
        if o == "П1":
            ht = self._humanize_team(home_team)
            return f"П1 {ht}" if ht else "П1"
        if o == "П2":
            at = self._humanize_team(away_team)
            return f"П2 {at}" if at else "П2"
        home = self._humanize_team(home_team)
        away = self._humanize_team(away_team)
        sl = s.lower()
        if home and (home.lower() in sl or sl in home.lower()):
            return home
        if away and (away.lower() in sl or sl in away.lower()):
            return away
        return s

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
        if any(token in text for token in ("1 тайм", "1-й тайм", "first half", "1st half", "@1ht@", "1ht")):
            return "1-й тайм"
        if any(token in text for token in ("2 тайм", "2-й тайм", "second half", "2nd half", "@2ht@", "2ht")):
            return "2-й тайм"
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
        context = self.describe_total_context(
            market_type="total_goals",
            market_label=market_label,
            selection=selection,
            home_team=home_team,
            away_team=away_team,
        )
        return context.normalized_text if context else None

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
        label = self._strip_winline_markers(market_label.strip())
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
        if self._contains_exact_word_token(upper, self._HOME_TOKENS):
            return home or "хозяева"
        if self._contains_exact_word_token(upper, self._AWAY_TOKENS):
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

    def _detect_period_scope(self, text: str) -> str:
        lowered = (text or "").lower()
        if any(token in lowered for token in ("1 тайм", "1-й тайм", "first half", "1st half", "@1ht@", "1ht")):
            return "first_half"
        if any(token in lowered for token in ("2 тайм", "2-й тайм", "second half", "2nd half", "@2ht@", "2ht")):
            return "second_half"
        return "match"

    def _detect_total_target_scope(
        self,
        *,
        text: str,
        team_name: str | None,
        home_team: str | None,
        away_team: str | None,
    ) -> str:
        lowered = (text or "").lower()
        if team_name:
            home = self._humanize_team(home_team)
            away = self._humanize_team(away_team)
            if home and team_name.lower() == home.lower():
                return "home_team"
            if away and team_name.lower() == away.lower():
                return "away_team"
            return "team_total"
        if any(token in lowered for token in ("team total", "индивидуаль", "команд", "it1", "it2")):
            return "team_total"
        return "match"

    def _compose_total_scope(self, *, period_scope: str, target_scope: str) -> str:
        if period_scope == "match" and target_scope == "match":
            return "match_total"
        return f"{period_scope}_{target_scope}_total"

    def _render_total_context(
        self,
        *,
        period_scope: str,
        target_scope: str,
        team_name: str | None,
        total_side: str | None,
        total_line: str | None,
    ) -> str:
        if total_side == "ТБ":
            side_text = "больше"
        elif total_side == "ТМ":
            side_text = "меньше"
        else:
            side_text = None

        period_prefix = ""
        if period_scope == "first_half":
            period_prefix = "1-й тайм "
        elif period_scope == "second_half":
            period_prefix = "2-й тайм "

        if target_scope in {"home_team", "away_team", "team_total"}:
            target = team_name or "команды"
            base = f"{period_prefix}тотал {target}".strip()
        else:
            base = f"{period_prefix}тотал".strip()

        if side_text and total_line:
            return f"{base} {side_text} {total_line}"
        if total_line:
            return f"{base} {total_line}"
        return base

    def _with_period(self, label: str, period: str | None) -> str:
        if not period:
            return label
        return f"{period}: {label[0].lower() + label[1:]}" if label else period

    def _clean_token(self, raw: str) -> str:
        return re.sub(r"[\s_()-]+", " ", (raw or "").strip()).strip()

    def _contains_exact_word_token(self, text: str, aliases: set[str]) -> bool:
        words = {part for part in re.split(r"[^A-ZА-Я0-9Ё]+", text or "") if part}
        return any(alias.upper() in words for alias in aliases)

    def _humanize_team(self, raw: str | None) -> str:
        text = (raw or "").strip()
        return self._TEAM_TRANSLIT.get(text, text)
