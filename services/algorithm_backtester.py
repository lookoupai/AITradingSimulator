"""
用户自定义算法历史回测
"""
from __future__ import annotations

from services.algorithm_executor import predict_jingcai_with_user_algorithm
from utils import jingcai_football as football_utils


def backtest_jingcai_user_algorithm(db, definition: dict, limit: int = 50) -> dict:
    safe_limit = max(1, min(int(limit or 50), 200))
    requested_targets = football_utils.normalize_target_list(definition.get('targets') or [])
    events = db.get_recent_lottery_events(
        'jingcai_football',
        limit=min(500, safe_limit * 3),
        source_provider='sina'
    )
    matches = []
    skipped_events = 0
    for event in events:
        match = _build_match_from_event(db, event)
        if not _has_actual_result(match, requested_targets):
            skipped_events += 1
            continue
        matches.append(match)
        if len(matches) >= safe_limit:
            break

    if not matches:
        return _empty_backtest_payload(safe_limit, skipped_events, requested_targets)

    user_algorithm = {
        'id': 0,
        'name': definition.get('method_name') or '用户算法回测',
        'definition': definition
    }
    items_payload, debug_payload = predict_jingcai_with_user_algorithm(
        'history-backtest',
        matches,
        {
            'id': 0,
            'prediction_targets': requested_targets,
            'user_algorithm': user_algorithm
        }
    )
    debug_map = {
        item.get('event_key'): item
        for item in (debug_payload.get('rows') or [])
    }
    records = [
        _build_backtest_record(match, item, debug_map.get(match.get('event_key')) or {}, requested_targets)
        for match, item in zip(matches, items_payload)
    ]
    prediction_count = sum(1 for item in records if _record_has_prediction(item, requested_targets))
    skip_count = len(records) - prediction_count
    return {
        'limit': safe_limit,
        'sample_size': len(records),
        'prediction_count': prediction_count,
        'skip_count': skip_count,
        'skipped_unsettled_count': skipped_events,
        'skip_rate': round(skip_count / len(records) * 100, 2) if records else None,
        'hit_rate': {
            target: _build_target_stats(records, target)
            for target in requested_targets
        },
        'profit_summary': {
            target: _build_profit_stats(records, target)
            for target in requested_targets
        },
        'risk_flags': _build_risk_flags(records, prediction_count, skip_count),
        'records': records
    }


def _empty_backtest_payload(limit: int, skipped_events: int, requested_targets: list[str]) -> dict:
    return {
        'limit': limit,
        'sample_size': 0,
        'prediction_count': 0,
        'skip_count': 0,
        'skipped_unsettled_count': skipped_events,
        'skip_rate': None,
        'hit_rate': {
            target: _build_target_stats([], target)
            for target in requested_targets
        },
        'profit_summary': {
            target: _build_profit_stats([], target)
            for target in requested_targets
        },
        'risk_flags': ['本地暂无可用于回测的已开奖竞彩足球样本。'],
        'records': []
    }


def _build_match_from_event(db, event: dict) -> dict:
    meta_payload = event.get('meta_payload') or {}
    result_payload = event.get('result_payload') or {}
    detail_bundle = _load_detail_bundle(db, event)
    rqspf = meta_payload.get('rqspf') or {}
    actual_spf = football_utils.normalize_prediction_outcome(
        result_payload.get('actual_spf')
    ) or football_utils.derive_spf_result(result_payload.get('score1'), result_payload.get('score2'))
    actual_rqspf = football_utils.normalize_prediction_outcome(
        result_payload.get('actual_rqspf')
    ) or football_utils.derive_rqspf_result(
        result_payload.get('score1'),
        result_payload.get('score2'),
        rqspf.get('handicap')
    )

    return {
        'lottery_type': 'jingcai_football',
        'event_key': event.get('event_key') or '',
        'batch_key': event.get('batch_key') or '',
        'match_no': meta_payload.get('match_no') or meta_payload.get('match_no_value') or '',
        'league': event.get('league') or '',
        'home_team': event.get('home_team') or '',
        'away_team': event.get('away_team') or '',
        'event_name': event.get('event_name') or '',
        'match_time': event.get('event_time') or '',
        'spf_odds': meta_payload.get('spf_odds') or {},
        'rqspf': rqspf,
        'score1': result_payload.get('score1'),
        'score2': result_payload.get('score2'),
        'score_text': meta_payload.get('score_text') or _build_score_text(result_payload),
        'actual_spf': actual_spf,
        'actual_rqspf': actual_rqspf,
        'detail_bundle': detail_bundle
    }


def _load_detail_bundle(db, event: dict) -> dict:
    meta_payload = event.get('meta_payload') or {}
    bundle = dict(meta_payload.get('detail_bundle') or {})
    detail_rows = db.get_lottery_event_details(
        'jingcai_football',
        event.get('event_key') or '',
        source_provider=event.get('source_provider') or 'sina'
    )
    for key, row in detail_rows.items():
        bundle[key] = row.get('payload') or {}
    return bundle


def _build_score_text(result_payload: dict) -> str:
    score1 = result_payload.get('score1')
    score2 = result_payload.get('score2')
    if score1 is None or score2 is None:
        return ''
    return f'{score1}:{score2}'


def _has_actual_result(match: dict, requested_targets: list[str]) -> bool:
    if 'spf' in requested_targets and match.get('actual_spf'):
        return True
    if 'rqspf' in requested_targets and match.get('actual_rqspf'):
        return True
    return False


def _build_backtest_record(match: dict, item: dict, debug: dict, requested_targets: list[str]) -> dict:
    hits = {}
    odds = {}
    for target in requested_targets:
        predicted = item.get(f'predicted_{target}')
        actual = match.get(f'actual_{target}')
        hits[target] = None if predicted is None or actual is None else int(predicted == actual)
        odds[target] = _resolve_prediction_odds(match, target, predicted)
    return {
        'event_key': match.get('event_key') or '',
        'match_no': match.get('match_no') or '',
        'event_time': match.get('match_time') or '',
        'title': _build_match_title(match),
        'predicted_spf': item.get('predicted_spf'),
        'predicted_rqspf': item.get('predicted_rqspf'),
        'actual_spf': match.get('actual_spf'),
        'actual_rqspf': match.get('actual_rqspf'),
        'confidence': item.get('confidence'),
        'score': debug.get('score'),
        'status': debug.get('status') or 'pending',
        'hits': hits,
        'odds': odds,
        'reasoning_summary': item.get('reasoning_summary') or ''
    }


def _build_match_title(match: dict) -> str:
    if match.get('event_name'):
        return match['event_name']
    home_team = match.get('home_team') or '主队'
    away_team = match.get('away_team') or '客队'
    league = match.get('league') or ''
    prefix = f'[{league}] ' if league else ''
    return f'{prefix}{home_team} vs {away_team}'


def _record_has_prediction(record: dict, requested_targets: list[str]) -> bool:
    return any(record.get(f'predicted_{target}') is not None for target in requested_targets)


def _build_target_stats(records: list[dict], target: str) -> dict:
    values = [
        (record.get('hits') or {}).get(target)
        for record in records
        if (record.get('hits') or {}).get(target) is not None
    ]
    hit_count = sum(values) if values else 0
    sample_count = len(values)
    return {
        'hit_count': hit_count,
        'sample_count': sample_count,
        'hit_rate': round(hit_count / sample_count * 100, 2) if sample_count else None,
        'ratio_text': f'{hit_count}/{sample_count}' if sample_count else '--'
    }


def _resolve_prediction_odds(match: dict, target: str, predicted: str | None) -> float | None:
    if not predicted:
        return None
    if target == 'spf':
        return football_utils.parse_float((match.get('spf_odds') or {}).get(predicted))
    if target == 'rqspf':
        return football_utils.parse_float(((match.get('rqspf') or {}).get('odds') or {}).get(predicted))
    return None


def _build_profit_stats(records: list[dict], target: str) -> dict:
    stake = 0.0
    gross_return = 0.0
    missing_odds = 0
    for record in records:
        hit = (record.get('hits') or {}).get(target)
        if hit is None:
            continue
        odds = football_utils.parse_float((record.get('odds') or {}).get(target))
        if odds is None:
            missing_odds += 1
            continue
        stake += 1.0
        if hit:
            gross_return += odds
    net_profit = gross_return - stake
    return {
        'bet_count': int(stake),
        'stake': round(stake, 2),
        'gross_return': round(gross_return, 2),
        'net_profit': round(net_profit, 2),
        'roi': round(net_profit / stake * 100, 2) if stake else None,
        'missing_odds': missing_odds
    }


def _build_risk_flags(records: list[dict], prediction_count: int, skip_count: int) -> list[str]:
    flags = []
    sample_size = len(records)
    if sample_size < 20:
        flags.append('回测样本少于 20 场，命中率波动会很大。')
    if sample_size and skip_count / sample_size >= 0.5:
        flags.append('跳过比例较高，实际可下注机会可能不足。')
    if prediction_count and prediction_count < 10:
        flags.append('产生预测的样本少于 10 场，收益模拟参考价值有限。')
    return flags
