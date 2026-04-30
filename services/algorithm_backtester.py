"""
用户自定义算法历史回测
"""
from __future__ import annotations

from datetime import datetime

from services.algorithm_executor import predict_jingcai_with_user_algorithm
from utils import jingcai_football as football_utils


def backtest_jingcai_user_algorithm(
    db,
    definition: dict,
    limit: int = 50,
    filters: dict | None = None
) -> dict:
    filter_options = _normalize_backtest_filters(filters or {})
    safe_limit = filter_options.get('recent_n') or max(1, min(int(limit or 50), 200))
    requested_targets = _resolve_requested_targets(definition, filter_options)
    scan_limit = max(200, min(2000, safe_limit * 8))
    events = db.get_recent_lottery_events(
        'jingcai_football',
        limit=scan_limit,
        source_provider='sina'
    )
    matches = []
    skipped_events = 0
    skipped_by_filter = 0
    for event in events:
        if not _event_matches_backtest_filters(event, filter_options):
            skipped_by_filter += 1
            continue
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
    effective_sample_count = _build_effective_sample_count(records, requested_targets)
    data_quality = _build_backtest_data_quality(db, matches, records)
    sample_bias_flags = _build_sample_bias_flags(records, requested_targets, filter_options, skip_count, data_quality)
    confidence_report = _build_confidence_report(
        records=records,
        prediction_count=prediction_count,
        skip_count=skip_count,
        effective_sample_count=effective_sample_count,
        data_quality=data_quality,
        sample_bias_flags=sample_bias_flags
    )
    return {
        'limit': safe_limit,
        'filters': filter_options,
        'targets': requested_targets,
        'sample_size': len(records),
        'effective_sample_count': effective_sample_count,
        'prediction_count': prediction_count,
        'skip_count': skip_count,
        'skipped_unsettled_count': skipped_events,
        'skipped_filter_count': skipped_by_filter,
        'skip_rate': round(skip_count / len(records) * 100, 2) if records else None,
        'hit_rate': {
            target: _build_target_stats(records, target)
            for target in requested_targets
        },
        'hit_rate_trend': {
            target: _build_hit_rate_trend(records, target)
            for target in requested_targets
        },
        'streaks': {
            target: _build_streak_stats(records, target)
            for target in requested_targets
        },
        'profit_summary': {
            target: _build_profit_stats(records, target)
            for target in requested_targets
        },
        'max_drawdown': {
            target: _build_max_drawdown(records, target)
            for target in requested_targets
        },
        'skip_reason_stats': _build_skip_reason_stats(records),
        'odds_interval_performance': {
            target: _build_odds_interval_performance(records, target)
            for target in requested_targets
        },
        'chart_data': _build_backtest_chart_data(records, requested_targets),
        'data_quality': data_quality,
        'confidence_report': confidence_report,
        'sample_bias_flags': sample_bias_flags,
        'risk_flags': _build_risk_flags(records, prediction_count, skip_count, effective_sample_count, sample_bias_flags),
        'records': records
    }


def _empty_backtest_payload(limit: int, skipped_events: int, requested_targets: list[str]) -> dict:
    return {
        'limit': limit,
        'filters': {},
        'targets': requested_targets,
        'sample_size': 0,
        'effective_sample_count': 0,
        'prediction_count': 0,
        'skip_count': 0,
        'skipped_unsettled_count': skipped_events,
        'skipped_filter_count': 0,
        'skip_rate': None,
        'hit_rate': {
            target: _build_target_stats([], target)
            for target in requested_targets
        },
        'hit_rate_trend': {target: [] for target in requested_targets},
        'streaks': {
            target: _build_streak_stats([], target)
            for target in requested_targets
        },
        'profit_summary': {
            target: _build_profit_stats([], target)
            for target in requested_targets
        },
        'max_drawdown': {
            target: _build_max_drawdown([], target)
            for target in requested_targets
        },
        'skip_reason_stats': {},
        'odds_interval_performance': {
            target: _build_odds_interval_performance([], target)
            for target in requested_targets
        },
        'chart_data': {
            target: {
                'hit_rate_trend': [],
                'equity_curve': [],
                'odds_intervals': []
            }
            for target in requested_targets
        },
        'data_quality': {
            'effective_sample_count': 0,
            'field_completeness_rate': None,
            'missing_field_stats': {},
            'local_history': {}
        },
        'confidence_report': _build_confidence_report([], 0, 0, 0, {}, []),
        'sample_bias_flags': [],
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
    data_quality = _build_match_data_quality(match, requested_targets)
    return {
        'event_key': match.get('event_key') or '',
        'match_no': match.get('match_no') or '',
        'event_time': match.get('match_time') or '',
        'league': match.get('league') or '',
        'title': _build_match_title(match),
        'predicted_spf': item.get('predicted_spf'),
        'predicted_rqspf': item.get('predicted_rqspf'),
        'actual_spf': match.get('actual_spf'),
        'actual_rqspf': match.get('actual_rqspf'),
        'confidence': item.get('confidence'),
        'score': debug.get('score'),
        'status': debug.get('status') or 'pending',
        'skip_reason': debug.get('skip_reason') or '',
        'filter_failures': debug.get('filter_failures') or [],
        'hits': hits,
        'odds': odds,
        'data_quality': data_quality,
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


def _build_hit_rate_trend(records: list[dict], target: str, window_size: int = 10) -> list[dict]:
    values = [
        (record.get('hits') or {}).get(target)
        for record in reversed(records)
        if (record.get('hits') or {}).get(target) is not None
    ]
    trend = []
    for start in range(0, len(values), window_size):
        window = values[start:start + window_size]
        if not window:
            continue
        hit_count = sum(window)
        trend.append({
            'index': len(trend) + 1,
            'sample_count': len(window),
            'hit_count': hit_count,
            'hit_rate': round(hit_count / len(window) * 100, 2)
        })
    return trend


def _build_streak_stats(records: list[dict], target: str) -> dict:
    values = [
        (record.get('hits') or {}).get(target)
        for record in reversed(records)
        if (record.get('hits') or {}).get(target) is not None
    ]
    max_hit = max_miss = current_hit = current_miss = 0
    for value in values:
        if value:
            current_hit += 1
            current_miss = 0
            max_hit = max(max_hit, current_hit)
        else:
            current_miss += 1
            current_hit = 0
            max_miss = max(max_miss, current_miss)
    latest_streak_type = 'none'
    latest_streak_count = 0
    for value in reversed(values):
        current_type = 'hit' if value else 'miss'
        if latest_streak_type == 'none':
            latest_streak_type = current_type
        if latest_streak_type != current_type:
            break
        latest_streak_count += 1
    return {
        'max_hit_streak': max_hit,
        'max_miss_streak': max_miss,
        'latest_streak_type': latest_streak_type,
        'latest_streak_count': latest_streak_count
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


def _build_max_drawdown(records: list[dict], target: str) -> dict:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    curve = []
    for record in reversed(records):
        hit = (record.get('hits') or {}).get(target)
        if hit is None:
            continue
        odds = football_utils.parse_float((record.get('odds') or {}).get(target))
        if odds is None:
            continue
        equity += odds - 1.0 if hit else -1.0
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
        curve.append(round(equity, 2))
    return {
        'amount': round(max_drawdown, 2),
        'unit': '每场 1 注',
        'equity_curve': curve[-50:]
    }


def _build_skip_reason_stats(records: list[dict]) -> dict:
    labels = {
        'filter_not_matched': '过滤条件未满足',
        'low_confidence': '低于最低信心阈值',
        'no_prediction': '未生成预测'
    }
    stats: dict[str, dict] = {}
    for record in records:
        if _record_has_prediction(record, list((record.get('hits') or {}).keys())):
            continue
        reason = record.get('skip_reason') or 'no_prediction'
        stats.setdefault(reason, {'count': 0, 'label': labels.get(reason, reason)})
        stats[reason]['count'] += 1
    return stats


def _build_odds_interval_performance(records: list[dict], target: str) -> list[dict]:
    intervals = [
        ('1.00-1.49', 1.0, 1.5),
        ('1.50-1.99', 1.5, 2.0),
        ('2.00-2.99', 2.0, 3.0),
        ('3.00+', 3.0, None)
    ]
    result = []
    for label, lower, upper in intervals:
        values = []
        for record in records:
            hit = (record.get('hits') or {}).get(target)
            odds = football_utils.parse_float((record.get('odds') or {}).get(target))
            if hit is None or odds is None:
                continue
            if odds < lower:
                continue
            if upper is not None and odds >= upper:
                continue
            values.append(hit)
        hit_count = sum(values) if values else 0
        result.append({
            'range': label,
            'sample_count': len(values),
            'hit_count': hit_count,
            'hit_rate': round(hit_count / len(values) * 100, 2) if values else None
        })
    return result


def _build_backtest_chart_data(records: list[dict], requested_targets: list[str]) -> dict:
    return {
        target: {
            'hit_rate_trend': _build_hit_rate_trend(records, target),
            'equity_curve': _build_max_drawdown(records, target).get('equity_curve') or [],
            'odds_intervals': _build_odds_interval_performance(records, target)
        }
        for target in requested_targets
    }


def _build_effective_sample_count(records: list[dict], requested_targets: list[str]) -> int:
    return sum(
        1
        for record in records
        if any((record.get('hits') or {}).get(target) is not None for target in requested_targets)
    )


def _build_backtest_data_quality(db, matches: list[dict], records: list[dict]) -> dict:
    missing_stats: dict[str, int] = {}
    completeness_values = []
    for record in records:
        quality = record.get('data_quality') or {}
        completeness = quality.get('field_completeness_rate')
        if completeness is not None:
            completeness_values.append(float(completeness))
        for field in quality.get('missing_fields') or []:
            missing_stats[field] = missing_stats.get(field, 0) + 1

    total_local_events = 0
    try:
        total_local_events = db.count_lottery_events('jingcai_football', source_provider='sina')
    except Exception:
        total_local_events = 0

    return {
        'effective_sample_count': len(matches),
        'field_completeness_rate': round(sum(completeness_values) / len(completeness_values), 2) if completeness_values else None,
        'missing_field_stats': missing_stats,
        'local_history': {
            'source_provider': 'sina',
            'total_event_count': total_local_events,
            'settled_sample_count': len(matches),
            'enough_for_backtest': len(matches) >= 20
        }
    }


def _build_match_data_quality(match: dict, requested_targets: list[str]) -> dict:
    detail_bundle = match.get('detail_bundle') or {}
    checks = {
        'spf_odds': _has_complete_odds(match.get('spf_odds') or {}),
        'recent_form': bool(detail_bundle.get('recent_form_team1')) and bool(detail_bundle.get('recent_form_team2')),
        'team_table': bool(detail_bundle.get('team_table')),
        'injury': 'injury' in detail_bundle and isinstance(detail_bundle.get('injury'), dict),
        'euro_odds_snapshot': bool(detail_bundle.get('odds_snapshots')) or bool(detail_bundle.get('odds_euro'))
    }
    if 'rqspf' in requested_targets:
        checks['rqspf_odds'] = _has_complete_odds(((match.get('rqspf') or {}).get('odds') or {}))
        checks['rqspf_handicap'] = football_utils.parse_float((match.get('rqspf') or {}).get('handicap')) is not None
    missing = [field for field, ok in checks.items() if not ok]
    return {
        'missing_fields': missing,
        'field_completeness_rate': round((len(checks) - len(missing)) / len(checks) * 100, 2) if checks else None
    }


def _has_complete_odds(odds_map: dict) -> bool:
    return all(football_utils.parse_float((odds_map or {}).get(outcome)) is not None for outcome in ('胜', '平', '负'))


def _build_confidence_report(
    records: list[dict],
    prediction_count: int,
    skip_count: int,
    effective_sample_count: int,
    data_quality: dict,
    sample_bias_flags: list[str]
) -> dict:
    sample_size = len(records)
    field_completeness = data_quality.get('field_completeness_rate')
    skip_rate = round(skip_count / sample_size * 100, 2) if sample_size else None
    reasons = []
    score = 100

    if effective_sample_count < 20:
        score -= 45
        reasons.append('有效样本少于 20 场')
    elif effective_sample_count < 50:
        score -= 25
        reasons.append('有效样本少于 50 场')
    elif effective_sample_count < 100:
        score -= 10
        reasons.append('有效样本少于 100 场')

    if prediction_count < 10:
        score -= 25
        reasons.append('实际出手少于 10 场')
    if skip_rate is not None and skip_rate >= 70:
        score -= 20
        reasons.append('跳过率高于 70%')
    elif skip_rate is not None and skip_rate >= 50:
        score -= 10
        reasons.append('跳过率高于 50%')

    if field_completeness is not None and field_completeness < 60:
        score -= 20
        reasons.append('字段完整率低于 60%')
    elif field_completeness is not None and field_completeness < 80:
        score -= 10
        reasons.append('字段完整率低于 80%')

    if sample_bias_flags:
        score -= min(20, len(sample_bias_flags) * 6)
        reasons.extend(sample_bias_flags[:3])

    score = max(0, min(100, score))
    if effective_sample_count < 20 or prediction_count < 10:
        level = 'insufficient'
        label = '样本不足'
    elif score >= 80:
        level = 'high'
        label = '较高可信'
    elif score >= 60:
        level = 'medium'
        label = '中等可信'
    else:
        level = 'low'
        label = '低可信'

    return {
        'level': level,
        'label': label,
        'score': score,
        'effective_sample_count': effective_sample_count,
        'prediction_count': prediction_count,
        'skip_rate': skip_rate,
        'field_completeness_rate': field_completeness,
        'reasons': reasons or ['样本、出手率和字段完整率未触发明显风险。']
    }


def _build_sample_bias_flags(
    records: list[dict],
    requested_targets: list[str],
    filter_options: dict,
    skip_count: int,
    data_quality: dict
) -> list[str]:
    flags = []
    sample_size = len(records)
    leagues = {str(record.get('league') or '').strip() for record in records if str(record.get('league') or '').strip()}
    if len(leagues) == 1 and sample_size >= 5:
        flags.append('样本集中在单一联赛，可能存在联赛风格偏差。')
    if filter_options.get('market_type') in {'spf', 'rqspf'}:
        flags.append(f"仅回测 {str(filter_options.get('market_type')).upper()}，不能代表其他玩法。")
    if filter_options.get('start_date') or filter_options.get('end_date') or filter_options.get('recent_n'):
        flags.append('回测使用了时间或最近场次筛选，可能受短期赛程影响。')
    if sample_size and skip_count / sample_size >= 0.5:
        flags.append('跳过样本占比较高，命中率可能只代表少数高筛选场次。')

    field_completeness = data_quality.get('field_completeness_rate')
    if field_completeness is not None and field_completeness < 80:
        flags.append('字段完整率偏低，评分可能偏向赔率等少数字段。')

    for target in requested_targets:
        odds_values = [
            football_utils.parse_float((record.get('odds') or {}).get(target))
            for record in records
            if (record.get('hits') or {}).get(target) is not None
        ]
        odds_values = [value for value in odds_values if value is not None]
        if len(odds_values) < 5:
            continue
        low_odds_count = sum(1 for value in odds_values if value < 1.5)
        high_odds_count = sum(1 for value in odds_values if value >= 3.0)
        if low_odds_count / len(odds_values) >= 0.6:
            flags.append(f'{target.upper()} 样本过度集中在 1.50 以下低赔率区间。')
        if high_odds_count / len(odds_values) >= 0.6:
            flags.append(f'{target.upper()} 样本过度集中在 3.00 以上高赔率区间。')

    return list(dict.fromkeys(flags))


def _build_risk_flags(
    records: list[dict],
    prediction_count: int,
    skip_count: int,
    effective_sample_count: int,
    sample_bias_flags: list[str] | None = None
) -> list[str]:
    flags = []
    sample_size = len(records)
    if sample_size < 20:
        flags.append('回测样本少于 20 场，命中率波动会很大。')
    if effective_sample_count < 20:
        flags.append('有效样本不足 20 场，请先补齐历史赛果和赔率后再判断算法优劣。')
    if sample_size and skip_count / sample_size >= 0.5:
        flags.append('跳过比例较高，实际可下注机会可能不足。')
    if prediction_count and prediction_count < 10:
        flags.append('产生预测的样本少于 10 场，收益模拟参考价值有限。')
    flags.extend(sample_bias_flags or [])
    return flags


def _normalize_backtest_filters(filters: dict) -> dict:
    market_type = str(filters.get('market_type') or filters.get('target') or '').strip().lower()
    if market_type not in {'spf', 'rqspf'}:
        market_type = ''
    targets = filters.get('targets')
    normalized_targets = football_utils.normalize_target_list(targets) if isinstance(targets, list) else []
    if market_type:
        normalized_targets = [market_type]

    leagues = filters.get('leagues')
    if isinstance(leagues, str):
        leagues = [leagues]
    normalized_leagues = [
        str(item).strip()
        for item in (leagues or [])
        if str(item).strip()
    ]

    return {
        'start_date': _normalize_date_text(filters.get('start_date')),
        'end_date': _normalize_date_text(filters.get('end_date')),
        'recent_n': _normalize_int(filters.get('recent_n'), minimum=1, maximum=200),
        'leagues': normalized_leagues,
        'targets': normalized_targets,
        'market_type': market_type or 'all'
    }


def _resolve_requested_targets(definition: dict, filter_options: dict) -> list[str]:
    targets = filter_options.get('targets') or football_utils.normalize_target_list(definition.get('targets') or [])
    return targets or ['spf']


def _event_matches_backtest_filters(event: dict, filters: dict) -> bool:
    event_date = _event_date_text(event)
    start_date = filters.get('start_date')
    end_date = filters.get('end_date')
    if start_date and event_date and event_date < start_date:
        return False
    if end_date and event_date and event_date > end_date:
        return False
    leagues = set(filters.get('leagues') or [])
    if leagues and str(event.get('league') or '').strip() not in leagues:
        return False
    return True


def _event_date_text(event: dict) -> str:
    value = str(event.get('event_date') or event.get('event_time') or '').strip()
    if len(value) >= 10:
        return value[:10]
    return ''


def _normalize_date_text(value) -> str:
    text = str(value or '').strip()
    if not text:
        return ''
    try:
        return datetime.fromisoformat(text[:10]).strftime('%Y-%m-%d')
    except ValueError:
        return ''


def _normalize_int(value, minimum: int, maximum: int) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return max(minimum, min(number, maximum))
