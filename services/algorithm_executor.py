"""
用户自定义算法 DSL 执行器
"""
from __future__ import annotations

from utils import jingcai_football as football_utils


FOOTBALL_OUTCOMES = ('胜', '平', '负')


def predict_jingcai_with_user_algorithm(run_key: str, matches: list[dict], predictor: dict) -> tuple[list[dict], dict]:
    user_algorithm = predictor.get('user_algorithm') or {}
    definition = user_algorithm.get('definition') or {}
    decision = definition.get('decision') or {}
    targets = definition.get('targets') or predictor.get('prediction_targets') or []
    filters = definition.get('filters') or []
    score_items = definition.get('score') or []
    allow_skip = bool(decision.get('allow_skip', True))
    min_confidence = _parse_float(decision.get('min_confidence'), default=0.0)
    pick_rule = str(decision.get('pick') or 'max_score').strip()

    items_payload = []
    debug_rows = []
    for match in matches:
        field_values = _build_football_field_values(match)
        passed_filters, filter_failures = _evaluate_filters(filters, field_values)
        score = _calculate_score(score_items, field_values)
        confidence = round(max(0.35, min(0.88, 0.38 + score * 0.48)), 2)

        if not passed_filters and allow_skip:
            predicted_spf = None
            predicted_rqspf = None
            confidence_value = None
            reasoning_summary = '未满足用户算法过滤条件，跳过预测'
            status = 'skipped'
        elif confidence < min_confidence and allow_skip:
            predicted_spf = None
            predicted_rqspf = None
            confidence_value = confidence
            reasoning_summary = '用户算法评分低于最低信心阈值，跳过预测'
            status = 'skipped'
        else:
            predicted_spf = _resolve_spf_pick(pick_rule, match)
            predicted_rqspf = _resolve_rqspf_pick(match) if 'rqspf' in targets else None
            if 'spf' not in targets:
                predicted_spf = None
            confidence_value = confidence
            reasoning_summary = _build_reasoning_summary(user_algorithm, score, field_values)
            status = 'pending'

        items_payload.append({
            'event_key': match.get('event_key') or '',
            'match_no': match.get('match_no') or '',
            'predicted_spf': predicted_spf,
            'predicted_rqspf': predicted_rqspf,
            'confidence': confidence_value,
            'reasoning_summary': reasoning_summary
        })
        debug_rows.append({
            'event_key': match.get('event_key') or '',
            'match_no': match.get('match_no') or '',
            'score': round(score, 4),
            'confidence': confidence_value,
            'status': status,
            'filter_failures': filter_failures,
            'predicted_spf': predicted_spf,
            'predicted_rqspf': predicted_rqspf
        })

    return items_payload, {
        'algorithm': f"user:{user_algorithm.get('id')}",
        'algorithm_name': user_algorithm.get('name') or definition.get('method_name') or '用户算法',
        'run_key': run_key,
        'rows': debug_rows
    }


def _build_football_field_values(match: dict) -> dict:
    detail_bundle = match.get('detail_bundle') or {}
    spf_odds = match.get('spf_odds') or {}
    rqspf = match.get('rqspf') or {}
    rqspf_odds = rqspf.get('odds') or {}
    probabilities = _normalized_probability_map(
        spf_odds.get('胜'),
        spf_odds.get('平'),
        spf_odds.get('负')
    )
    home_recent = _recent_form_snapshot(
        detail_bundle.get('recent_form_team1') or [],
        str(match.get('home_team') or ''),
        match.get('team1_id')
    )
    away_recent = _recent_form_snapshot(
        detail_bundle.get('recent_form_team2') or [],
        str(match.get('away_team') or ''),
        match.get('team2_id')
    )
    home_rank, away_rank = _extract_rank_values(detail_bundle)
    home_injury_count, away_injury_count = _extract_injury_counts(detail_bundle)

    values = {
        'league': match.get('league'),
        'match_time': match.get('match_time'),
        'home_team': match.get('home_team'),
        'away_team': match.get('away_team'),
        'spf_odds.win': _parse_float(spf_odds.get('胜')),
        'spf_odds.draw': _parse_float(spf_odds.get('平')),
        'spf_odds.lose': _parse_float(spf_odds.get('负')),
        'rqspf.handicap': _parse_float(rqspf.get('handicap')),
        'rqspf.odds.win': _parse_float(rqspf_odds.get('胜')),
        'rqspf.odds.draw': _parse_float(rqspf_odds.get('平')),
        'rqspf.odds.lose': _parse_float(rqspf_odds.get('负')),
        'home_recent_wins_6': home_recent['wins'],
        'away_recent_wins_6': away_recent['wins'],
        'home_goals_per_match_6': home_recent['goals_per_match'],
        'away_goals_per_match_6': away_recent['goals_per_match'],
        'home_conceded_per_match_6': home_recent['conceded_per_match'],
        'away_conceded_per_match_6': away_recent['conceded_per_match'],
        'home_goal_diff_per_match_6': home_recent['goal_diff_per_match'],
        'home_away_adjusted_form_6': home_recent['points_per_match'] - away_recent['points_per_match'],
        'home_rank': home_rank,
        'away_rank': away_rank,
        'rank_gap': (away_rank - home_rank) if home_rank and away_rank else None,
        'home_injury_count': home_injury_count,
        'away_injury_count': away_injury_count,
        'injury_advantage': max(-1.0, min(1.0, (away_injury_count - home_injury_count) * 0.2)),
        'implied_probability_spf_win': probabilities.get('胜'),
        'implied_probability_spf_draw': probabilities.get('平'),
        'implied_probability_spf_lose': probabilities.get('负'),
        'market_odds_consistency': probabilities.get('胜')
    }
    values['home_recent_wins_n'] = values['home_recent_wins_6']
    values['away_recent_wins_n'] = values['away_recent_wins_6']
    values['home_goals_per_match_n'] = values['home_goals_per_match_6']
    values['away_goals_per_match_n'] = values['away_goals_per_match_6']
    values['home_conceded_per_match_n'] = values['home_conceded_per_match_6']
    values['away_conceded_per_match_n'] = values['away_conceded_per_match_6']
    values.update(_extract_market_snapshot_values(detail_bundle))
    return values


def _evaluate_filters(filters: list[dict], values: dict) -> tuple[bool, list[dict]]:
    failures = []
    for item in filters:
        field = str(item.get('field') or '').strip()
        actual = values.get(field)
        operator = str(item.get('op') or '').strip()
        expected = item.get('value')
        if not _compare_value(actual, operator, expected):
            failures.append({'field': field, 'op': operator, 'value': expected, 'actual': actual})
    return not failures, failures


def _calculate_score(score_items: list[dict], values: dict) -> float:
    weighted_score = 0.0
    total_weight = 0.0
    for item in score_items:
        field = str(item.get('feature') or '').strip()
        raw_value = values.get(field)
        weight = _parse_float(item.get('weight'), default=0.0)
        transform = str(item.get('transform') or 'linear').strip()
        normalized_value = _normalize_feature_value(field, raw_value)
        if transform == 'inverse':
            normalized_value = 1.0 - normalized_value
        weighted_score += normalized_value * weight
        total_weight += weight
    if total_weight <= 0:
        return 0.0
    return max(0.0, min(1.0, weighted_score / total_weight))


def _compare_value(actual, operator: str, expected) -> bool:
    if operator == 'eq':
        return actual == expected
    if operator == 'neq':
        return actual != expected
    if operator == 'in':
        return actual in expected if isinstance(expected, list) else False

    actual_number = _parse_float(actual)
    if actual_number is None:
        return False
    if operator == 'between':
        if not isinstance(expected, list) or len(expected) != 2:
            return False
        lower = _parse_float(expected[0])
        upper = _parse_float(expected[1])
        return lower is not None and upper is not None and lower <= actual_number <= upper

    expected_number = _parse_float(expected)
    if expected_number is None:
        return False
    if operator == 'gt':
        return actual_number > expected_number
    if operator == 'gte':
        return actual_number >= expected_number
    if operator == 'lt':
        return actual_number < expected_number
    if operator == 'lte':
        return actual_number <= expected_number
    return False


def _normalize_feature_value(field: str, value) -> float:
    number = _parse_float(value)
    if number is None:
        return 0.0
    if field.startswith('implied_probability') or field == 'market_odds_consistency':
        return max(0.0, min(1.0, number))
    if field.endswith('_wins_6') or field.endswith('_wins_n'):
        return max(0.0, min(1.0, number / 6.0))
    if 'goals_per_match' in field or 'conceded_per_match' in field:
        return max(0.0, min(1.0, number / 3.0))
    if field == 'home_goal_diff_per_match_6':
        return max(0.0, min(1.0, (number + 3.0) / 6.0))
    if field == 'home_away_adjusted_form_6':
        return max(0.0, min(1.0, (number + 3.0) / 6.0))
    if field == 'injury_advantage':
        return max(0.0, min(1.0, (number + 1.0) / 2.0))
    if field == 'rank_gap':
        return max(0.0, min(1.0, number / 12.0))
    if field.startswith('spf_odds') or field.startswith('rqspf.odds'):
        return max(0.0, min(1.0, 1.0 / max(number, 1.01)))
    return max(0.0, min(1.0, number))


def _resolve_spf_pick(pick_rule: str, match: dict) -> str | None:
    if pick_rule in FOOTBALL_OUTCOMES:
        return pick_rule
    return _pick_lowest_odds_outcome(match.get('spf_odds') or {})


def _resolve_rqspf_pick(match: dict) -> str | None:
    return _pick_lowest_odds_outcome(((match.get('rqspf') or {}).get('odds') or {}))


def _pick_lowest_odds_outcome(odds_map: dict) -> str | None:
    ranked = []
    for outcome in FOOTBALL_OUTCOMES:
        odds = _parse_float((odds_map or {}).get(outcome))
        if odds is not None and odds > 0:
            ranked.append((outcome, odds))
    if not ranked:
        return None
    ranked.sort(key=lambda item: item[1])
    return ranked[0][0]


def _build_reasoning_summary(user_algorithm: dict, score: float, values: dict) -> str:
    name = user_algorithm.get('name') or '用户算法'
    home_goals = values.get('home_goals_per_match_6')
    away_conceded = values.get('away_conceded_per_match_6')
    win_odds = values.get('spf_odds.win')
    return (
        f"{name}评分{score:.2f}，"
        f"主队近6场进球{_format_number(home_goals)}，"
        f"客队近6场失球{_format_number(away_conceded)}，"
        f"主胜赔率{_format_number(win_odds)}"
    )


def _recent_form_snapshot(items: list[dict], team_name: str, team_id) -> dict:
    team_id_text = str(team_id or '').strip()
    wins = draws = goals_for = goals_against = matches = 0
    for item in items[:6]:
        score1 = football_utils.parse_int(item.get('score1'))
        score2 = football_utils.parse_int(item.get('score2'))
        team1 = str(item.get('team1') or '')
        team2 = str(item.get('team2') or '')
        team1_id = str(item.get('team1Id') or '').strip()
        team2_id = str(item.get('team2Id') or '').strip()
        if score1 is None or score2 is None:
            continue
        if team_id_text and team1_id == team_id_text:
            gf, ga = score1, score2
        elif team_id_text and team2_id == team_id_text:
            gf, ga = score2, score1
        elif team1 == team_name:
            gf, ga = score1, score2
        elif team2 == team_name:
            gf, ga = score2, score1
        else:
            continue
        matches += 1
        goals_for += gf
        goals_against += ga
        if gf > ga:
            wins += 1
        elif gf == ga:
            draws += 1

    if not matches:
        return {
            'wins': 0,
            'goals_per_match': 0.0,
            'conceded_per_match': 0.0,
            'goal_diff_per_match': 0.0,
            'points_per_match': 0.0
        }
    return {
        'wins': wins,
        'goals_per_match': goals_for / matches,
        'conceded_per_match': goals_against / matches,
        'goal_diff_per_match': (goals_for - goals_against) / matches,
        'points_per_match': (wins * 3 + draws) / matches
    }


def _extract_rank_values(detail_bundle: dict) -> tuple[float | None, float | None]:
    team_table = detail_bundle.get('team_table') or {}
    team1_items = ((team_table.get('team1') or {}).get('items') or {})
    team2_items = ((team_table.get('team2') or {}).get('items') or {})
    team1_all = (team1_items.get('all') or [{}])[0]
    team2_all = (team2_items.get('all') or [{}])[0]
    return _parse_float(team1_all.get('position')), _parse_float(team2_all.get('position'))


def _extract_injury_counts(detail_bundle: dict) -> tuple[int, int]:
    injury = detail_bundle.get('injury') or {}
    return len(injury.get('team1') or []), len(injury.get('team2') or [])


def _extract_market_snapshot_values(detail_bundle: dict) -> dict:
    snapshots = detail_bundle.get('odds_snapshots') or {}
    euro = snapshots.get('euro') or {}
    initial = euro.get('initial') or {}
    current = euro.get('current') or {}
    initial_win = _parse_float(initial.get('win'))
    current_win = _parse_float(current.get('win'))
    return {
        'euro.initial.win': initial_win,
        'euro.current.win': current_win,
        'euro.drop.win': (initial_win - current_win) if initial_win is not None and current_win is not None else None,
        'asia.line_changed': 0.0,
        'asia.home_trend': 0.0
    }


def _normalized_probability_map(home_odds, draw_odds, away_odds) -> dict[str, float]:
    normalized = [
        ('胜', _parse_float(home_odds)),
        ('平', _parse_float(draw_odds)),
        ('负', _parse_float(away_odds))
    ]
    normalized = [(outcome, odds) for outcome, odds in normalized if odds is not None and odds > 0]
    inverse_sum = sum(1 / odds for _, odds in normalized)
    if inverse_sum <= 0:
        return {}
    return {
        outcome: (1 / odds) / inverse_sum
        for outcome, odds in normalized
    }


def _parse_float(value, default=None):
    parsed = football_utils.parse_float(value)
    return default if parsed is None else parsed


def _format_number(value) -> str:
    number = _parse_float(value)
    if number is None:
        return '--'
    return f'{number:.2f}'
