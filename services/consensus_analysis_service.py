"""
竞彩足球（及未来其它彩种）方案共识分析服务

核心思路：
- 给定一组方案 ID 集合（用户的方案 / 平台所有方案），分析它们历史预测的共识规律：
  1. 各方案自身命中率（按字段如 spf/rqspf 拆分）
  2. 当 N 个方案预测同一结果时该结果的命中率（共识强度 vs 命中率）
  3. 任意两两方案预测一致时的命中率（找出"黄金搭档"组合）
  4. 今日所有未结算比赛的共识推荐（结合上面的历史规律给出当下建议）

数据全部来自 prediction_runs / prediction_items 两张表，方案池动态化，
新增/移除方案不需要改代码。
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import combinations
from typing import Any, Iterable

from lotteries.registry import get_lottery_definition, normalize_lottery_type
from utils import jingcai_football as football_utils


# 历史样本上限（避免大表全量扫描）
HISTORICAL_QUERY_LIMIT = 20000

# 历史命中率被认为"可靠"的最小样本量。
# 低于此值时今日推荐里的命中率仅作参考，不参与排序加权。
MIN_RELIABLE_SAMPLE = 20

# 用于"加权共识"特征：当方案样本量低于此阈值时不计权重（视为中性 0）
MIN_SAMPLE_FOR_WEIGHT = 30
# 三选一随机命中率基准（spf / rqspf 都是胜平负三分类）
RANDOM_BASELINE_RATE = 33.33

# 低命中排除信号阈值。这里使用“比赛场次”而不是方案命中样本数。
LOW_HIT_MIN_SAMPLE = 10
LOW_HIT_MEDIUM_SAMPLE = 20
LOW_HIT_HIGH_SAMPLE = 30
LOW_HIT_STRONG_RATE = 20.0
LOW_HIT_WEAK_RATE = 25.0
LOW_HIT_MAX_COMBO_SIZE = 3

SPF_ODDS_SEGMENTS = (
    ('ultra_low', 0.0, 1.55, '超低赔'),
    ('low', 1.55, 2.20, '低赔'),
    ('medium', 2.20, 3.50, '中赔'),
    ('high', 3.50, None, '高赔')
)


def build_consensus_analysis(
    db,
    *,
    user_id: int | None,
    lottery_type: str = 'jingcai_football',
    time_window_days: int | None = 30,
    predictor_ids: list[int] | None = None
) -> dict:
    """
    构建一份共识分析快照。

    参数：
        db: Database 实例
        user_id: 限定方案池为该用户；None 表示分析平台所有方案（仅管理员调用）
        lottery_type: 'jingcai_football' 或 'pc28'
        time_window_days: 历史窗口，按 created_at 过滤最近 N 天；None 表示不限。

    返回：见 plan 文件。所有率以百分比 float 形式给出，未达成时为 None。
    """
    normalized = normalize_lottery_type(lottery_type)
    definition = get_lottery_definition(normalized)

    # 字段：只取该彩种声明做共识分析的字段
    target_label_map = {key: label for key, label in definition.target_options}
    fields = [
        {'key': key, 'label': target_label_map.get(key, key)}
        for key in (definition.consensus_fields or ())
    ]

    # 1. 方案池
    predictors_pool = _select_predictor_pool(
        db,
        user_id=user_id,
        lottery_type=normalized,
        predictor_ids=predictor_ids,
    )
    predictor_ids = [p['id'] for p in predictors_pool]

    if not predictor_ids or not fields:
        return _empty_analysis(normalized, fields, time_window_days)

    # 2 + 3. 拉取已结算 + 待结算预测，按彩种路由到不同 fetcher
    if normalized == 'pc28':
        settled_items = _fetch_pc28_predictions_as_items(
            db,
            predictor_ids=predictor_ids,
            consensus_fields=definition.consensus_fields,
            only_settled=True,
            time_window_days=time_window_days
        )
        pending_items = _fetch_pc28_predictions_as_items(
            db,
            predictor_ids=predictor_ids,
            consensus_fields=definition.consensus_fields,
            only_settled=False,
            only_pending=True,
            recent_issues_limit=None
        )
    else:
        settled_items = _fetch_prediction_items(
            db,
            predictor_ids=predictor_ids,
            lottery_type=normalized,
            only_settled=True,
            time_window_days=time_window_days
        )
        pending_items = _fetch_prediction_items(
            db,
            predictor_ids=predictor_ids,
            lottery_type=normalized,
            only_settled=False,
            only_pending=True,
            time_window_days=None
        )
        settled_items = _attach_jingcai_market_context(db, settled_items)
        pending_items = _attach_jingcai_market_context(db, pending_items)

    # 4. 各方案自身命中率
    per_predictor = _build_per_predictor_stats(settled_items, predictors_pool, fields)

    # 4a. 从归档表补充历史（仅竞彩足球有归档；PC28 暂不归档）
    if normalized == 'jingcai_football':
        archive_per_predictor = _load_archived_per_predictor(
            db,
            predictor_ids=predictor_ids,
            time_window_days=time_window_days,
            fields=fields
        )
        per_predictor = _merge_per_predictor_with_archive(per_predictor, archive_per_predictor, fields)
    else:
        archive_per_predictor = {}

    # 5. 按比赛重新组织：{(run_key, event_key): [item, item, ...]}
    matches = _group_by_match(settled_items)

    # 6. 共识数 vs 命中率
    consensus_by_count = _build_consensus_by_count(matches, fields)

    # 7. 两两方案一致时的命中率
    pair_combinations = _build_pair_combinations(matches, predictor_ids, fields)
    pair_segment_combinations = _build_pair_segment_combinations(matches, predictor_ids, fields)
    per_predictor_segments = _build_per_predictor_segment_stats(settled_items, predictors_pool, fields)

    # 7a. 低命中风险信号：按预测值拆分共识方案数、两方案组合、三方案组合。
    low_hit_signals = _build_low_hit_signals(
        matches=matches,
        predictor_ids=predictor_ids,
        predictors_pool=predictors_pool,
        fields=fields
    )

    # 7b. 计算每个方案的"质量权重"（命中率 - 随机基准），供今日推荐排序使用
    predictor_weights = _compute_predictor_weights(per_predictor, fields, normalized)

    # 8. 今日推荐（基于 pending_items + 历史规律 + 加权信号 + 低命中排除）
    today_recommendations = _build_today_recommendations(
        pending_items=pending_items,
        consensus_by_count=consensus_by_count,
        pair_combinations=pair_combinations,
        pair_segment_combinations=pair_segment_combinations,
        low_hit_signals=low_hit_signals,
        predictors_pool=predictors_pool,
        fields=fields,
        predictor_weights=predictor_weights
    )

    return {
        'lottery_type': normalized,
        'lottery_label': definition.label,
        'fields': fields,
        'window_days': time_window_days,
        'predictors': [
            {
                'id': p['id'],
                'name': p.get('name') or f"方案#{p['id']}",
                'engine_type': p.get('engine_type') or 'ai',
                'algorithm_key': p.get('algorithm_key') or '',
                'enabled': bool(p.get('enabled')),
                'user_id': p.get('user_id')
            }
            for p in predictors_pool
        ],
        'sample_count': len(matches),
        'settled_item_count': len(settled_items),
        'pending_item_count': len(pending_items),
        'archive_used': bool(archive_per_predictor),
        'per_predictor': per_predictor,
        'per_predictor_segments': per_predictor_segments,
        'consensus_by_count': consensus_by_count,
        'pair_combinations': pair_combinations,
        'pair_segment_combinations': pair_segment_combinations,
        'low_hit_signals': low_hit_signals,
        'today_recommendations': today_recommendations
    }


# ----------------------- 内部辅助 -----------------------

def _empty_analysis(lottery_type: str, fields: list[dict], window: int | None) -> dict:
    return {
        'lottery_type': lottery_type,
        'lottery_label': get_lottery_definition(lottery_type).label,
        'fields': fields,
        'window_days': window,
        'predictors': [],
        'sample_count': 0,
        'settled_item_count': 0,
        'pending_item_count': 0,
        'archive_used': False,
        'per_predictor': [],
        'per_predictor_segments': {f['key']: [] for f in fields},
        'consensus_by_count': {f['key']: [] for f in fields},
        'pair_combinations': {f['key']: [] for f in fields},
        'pair_segment_combinations': {f['key']: [] for f in fields},
        'low_hit_signals': {f['key']: [] for f in fields},
        'today_recommendations': []
    }


def _select_predictor_pool(
    db,
    *,
    user_id: int | None,
    lottery_type: str,
    predictor_ids: list[int] | None = None,
) -> list[dict]:
    """
    选取参与分析的方案。规则：
    - 限定 lottery_type
    - enabled=1
    - user_id 给定时只取该用户的，否则取全部
    """
    if user_id is not None:
        all_predictors = db.get_predictors_by_user(int(user_id), include_secret=False)
    else:
        all_predictors = db.get_all_predictors(include_secret=False)

    selected = [
        p for p in (all_predictors or [])
        if (p.get('lottery_type') or '') == lottery_type
        and bool(p.get('enabled'))
    ]
    if predictor_ids is None:
        return selected
    allowed = {int(item) for item in predictor_ids}
    return [item for item in selected if int(item['id']) in allowed]


def _fetch_prediction_items(
    db,
    *,
    predictor_ids: list[int],
    lottery_type: str,
    only_settled: bool = True,
    only_pending: bool = False,
    time_window_days: int | None = None
) -> list[dict]:
    """直接 SQL 拉取 prediction_items，已根据 predictor_id 集合过滤。"""
    if not predictor_ids:
        return []

    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        placeholders = ','.join('?' for _ in predictor_ids)
        sql = f"""
            SELECT id, predictor_id, lottery_type, run_key, event_key,
                   issue_no, title, prediction_payload, actual_payload,
                   hit_payload, status, created_at, settled_at
            FROM prediction_items
            WHERE lottery_type = ? AND predictor_id IN ({placeholders})
        """
        params: list[Any] = [lottery_type, *predictor_ids]

        if only_settled:
            sql += " AND status = 'settled'"
        elif only_pending:
            sql += " AND status = 'pending'"

        if time_window_days is not None:
            cutoff = (datetime.utcnow() - timedelta(days=int(time_window_days))).strftime('%Y-%m-%d %H:%M:%S')
            sql += " AND created_at >= ?"
            params.append(cutoff)

        sql += f" ORDER BY created_at DESC LIMIT {HISTORICAL_QUERY_LIMIT}"

        cursor.execute(sql, params)
        rows = cursor.fetchall()
    finally:
        conn.close()

    items: list[dict] = []
    for row in rows:
        try:
            prediction_payload = json.loads(row['prediction_payload'] or '{}')
        except (TypeError, ValueError):
            prediction_payload = {}
        try:
            hit_payload = json.loads(row['hit_payload'] or '{}')
        except (TypeError, ValueError):
            hit_payload = {}
        try:
            actual_payload = json.loads(row['actual_payload'] or '{}')
        except (TypeError, ValueError):
            actual_payload = {}

        items.append({
            'id': row['id'],
            'predictor_id': row['predictor_id'],
            'lottery_type': row['lottery_type'],
            'run_key': row['run_key'],
            'event_key': row['event_key'],
            'issue_no': row['issue_no'],
            'title': row['title'],
            'status': row['status'],
            'created_at': row['created_at'],
            'settled_at': row['settled_at'],
            'prediction': prediction_payload,
            'hit': hit_payload,
            'actual': actual_payload
        })
    return items


def _attach_jingcai_market_context(db, items: list[dict]) -> list[dict]:
    """为竞彩足球样本补赛事盘口快照，供分层统计使用。"""
    if not items:
        return items

    event_keys = [
        str(item.get('event_key') or '').strip()
        for item in items
        if str(item.get('event_key') or '').strip()
    ]
    if not event_keys:
        return items

    event_map = db.get_lottery_event_map('jingcai_football', event_keys, source_provider='sina')
    fallback_event_map = db.get_lottery_event_map('jingcai_football', event_keys)

    for item in items:
        event_key = str(item.get('event_key') or '').strip()
        event = event_map.get(event_key) or fallback_event_map.get(event_key) or {}
        meta_payload = event.get('meta_payload') or {}
        item['market_context'] = {
            'spf_odds': meta_payload.get('spf_odds') or {},
            'rqspf': meta_payload.get('rqspf') or {},
            'league': event.get('league') or '',
            'home_team': event.get('home_team') or '',
            'away_team': event.get('away_team') or ''
        }
    return items


def _fetch_pc28_predictions_as_items(
    db,
    *,
    predictor_ids: list[int],
    consensus_fields: tuple[str, ...],
    only_settled: bool = True,
    only_pending: bool = False,
    time_window_days: int | None = None,
    recent_issues_limit: int | None = None
) -> list[dict]:
    """
    PC28 数据适配层：把 predictions 表的行"伪装"成跟竞彩 prediction_items 同形的字典，
    让下游的 _group_by_match / _build_consensus_by_count / _build_pair_combinations
    等函数可以零修改地处理 PC28 数据。

    伪装规则：
        - run_key   = ''                     （PC28 不需要批次概念）
        - event_key = issue_no               （每期作为一场"比赛"）
        - title     = f'第 {issue_no} 期'
        - prediction = {field_key: prediction_<field>}  仅含 consensus_fields 中的字段
        - hit        = {field_key: hit_<field>}         同上
        - actual     = {field_key: actual_<field>}      同上

    `time_window_days` 按 created_at 过滤最近 N 天，用于共识分析。
    `recent_issues_limit` 解释为"最近 N 期"，仅用于聊天上下文等小样本抽取。
    """
    if not predictor_ids:
        return []
    fields = tuple(f for f in (consensus_fields or ()) if f in {'big_small', 'odd_even', 'combo', 'number'})
    if not fields:
        return []

    # 选要查询的列
    pred_cols = ', '.join(f'prediction_{f}' for f in fields)
    hit_cols = ', '.join(f'hit_{f}' for f in fields)
    actual_cols = ', '.join(f'actual_{f}' for f in fields)

    placeholders = ','.join('?' for _ in predictor_ids)
    select_cols = f"""
        id, predictor_id, issue_no, status, created_at, settled_at,
        {pred_cols}, {hit_cols}, {actual_cols}
    """
    base_where = f"lottery_type = 'pc28' AND predictor_id IN ({placeholders})"

    status_clause = ''
    if only_settled:
        status_clause = " AND status = 'settled'"
    elif only_pending:
        status_clause = " AND status = 'pending'"

    time_clause = ''
    time_params: list[Any] = []
    if time_window_days is not None:
        try:
            window_days = max(1, int(time_window_days))
        except (TypeError, ValueError):
            window_days = 7
        cutoff = (datetime.utcnow() - timedelta(days=window_days)).strftime('%Y-%m-%d %H:%M:%S')
        time_clause = " AND created_at >= ?"
        time_params.append(cutoff)

    if recent_issues_limit is not None:
        try:
            limit_n = max(1, int(recent_issues_limit))
        except (TypeError, ValueError):
            limit_n = HISTORICAL_QUERY_LIMIT
        limit_n = min(limit_n, HISTORICAL_QUERY_LIMIT)
        sql = f"""
            WITH recent_issues AS (
                SELECT issue_no
                FROM predictions
                WHERE {base_where}{status_clause}{time_clause}
                GROUP BY issue_no
                ORDER BY CAST(issue_no AS INTEGER) DESC
                LIMIT ?
            )
            SELECT {select_cols}
            FROM predictions
            WHERE {base_where}{status_clause}{time_clause}
              AND issue_no IN (SELECT issue_no FROM recent_issues)
            ORDER BY CAST(issue_no AS INTEGER) DESC, predictor_id ASC
            LIMIT {HISTORICAL_QUERY_LIMIT}
        """
        params: list[Any] = [
            *predictor_ids, *time_params,
            limit_n,
            *predictor_ids, *time_params
        ]
    else:
        sql = f"""
            SELECT {select_cols}
            FROM predictions
            WHERE {base_where}{status_clause}{time_clause}
            ORDER BY CAST(issue_no AS INTEGER) DESC, predictor_id ASC
            LIMIT {HISTORICAL_QUERY_LIMIT}
        """
        params = [*predictor_ids, *time_params]

    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
    finally:
        conn.close()

    items: list[dict] = []
    for row in rows:
        prediction = {}
        hit = {}
        actual = {}
        for f in fields:
            pv = row[f'prediction_{f}']
            if pv not in (None, ''):
                prediction[f] = pv
            hv = row[f'hit_{f}']
            if hv is not None:
                hit[f] = int(hv)
            av = row[f'actual_{f}']
            if av not in (None, ''):
                actual[f] = av

        items.append({
            'id': row['id'],
            'predictor_id': row['predictor_id'],
            'lottery_type': 'pc28',
            'run_key': '',
            'event_key': str(row['issue_no']),
            'issue_no': str(row['issue_no']),
            'title': f"第 {row['issue_no']} 期",
            'status': row['status'],
            'created_at': row['created_at'],
            'settled_at': row['settled_at'],
            'prediction': prediction,
            'hit': hit,
            'actual': actual
        })
    return items


def _group_by_match(items: list[dict]) -> dict[tuple, list[dict]]:
    grouped: dict[tuple, list[dict]] = defaultdict(list)
    for item in items:
        key = (item.get('run_key') or '', item.get('event_key') or '')
        grouped[key].append(item)
    return grouped


def _build_market_segment(item: dict, field_key: str, pred_val: str | None) -> dict:
    """把竞彩足球样本映射成可统计的盘口语义分层；其它彩种返回 all。"""
    if item.get('lottery_type') != 'jingcai_football':
        return {'key': 'all', 'label': '全部样本'}

    market_context = item.get('market_context') or {}
    if field_key == 'spf':
        return _build_spf_segment(market_context, pred_val)
    if field_key == 'rqspf':
        return _build_rqspf_segment(market_context, pred_val)
    return {'key': 'all', 'label': '全部样本'}


def _iter_all_and_specific_segments(segment: dict) -> Iterable[tuple[str, str]]:
    """返回全量分层和具体分层；当具体分层也是 all 时只返回一次。"""
    candidates = (
        ('all', '全部样本'),
        (
            str((segment or {}).get('key') or 'all'),
            str((segment or {}).get('label') or '全部样本')
        )
    )
    seen: set[tuple[str, str]] = set()
    for item in candidates:
        if item in seen:
            continue
        seen.add(item)
        yield item


def _build_spf_segment(market_context: dict, pred_val: str | None) -> dict:
    odds_map = market_context.get('spf_odds') or {}
    odds_value = football_utils.parse_float(odds_map.get(pred_val)) if pred_val else None
    if odds_value is None:
        return {'key': 'all', 'label': '全部样本'}

    sorted_odds = []
    for outcome in ('胜', '平', '负'):
        outcome_odds = football_utils.parse_float(odds_map.get(outcome))
        if outcome_odds is not None:
            sorted_odds.append((outcome, outcome_odds))
    sorted_odds.sort(key=lambda pair: pair[1])

    role = 'unknown'
    role_label = '未知角色'
    if sorted_odds:
        if pred_val == sorted_odds[0][0]:
            role = 'favorite'
            role_label = '低赔方'
        elif len(sorted_odds) >= 2 and pred_val == sorted_odds[-1][0]:
            role = 'underdog'
            role_label = '高赔方'
        else:
            role = 'mid'
            role_label = '中赔项'

    odds_band = 'unknown'
    odds_band_label = '未知赔率'
    for band_key, lower, upper, label in SPF_ODDS_SEGMENTS:
        if odds_value >= lower and (upper is None or odds_value < upper):
            odds_band = band_key
            odds_band_label = label
            break

    return {
        'key': f'spf:{role}:{odds_band}',
        'label': f'{role_label}/{odds_band_label}',
        'role': role,
        'role_label': role_label,
        'odds_band': odds_band,
        'odds_band_label': odds_band_label,
        'odds_value': odds_value
    }


def _build_rqspf_segment(market_context: dict, pred_val: str | None) -> dict:
    rqspf = market_context.get('rqspf') or {}
    handicap = football_utils.parse_int(rqspf.get('handicap'))
    if handicap is None:
        handicap = football_utils.parse_int(rqspf.get('handicap_text'))
    if handicap is None:
        return {'key': 'all', 'label': '全部样本'}

    direction = 'level'
    direction_label = '平手'
    if handicap < 0:
        direction = 'home_give'
        direction_label = '主让'
    elif handicap > 0:
        direction = 'home_receive'
        direction_label = '主受让'

    semantic_key = _resolve_rqspf_semantic(direction, pred_val)
    semantic_label = {
        'cover': '让胜',
        'push': '让平',
        'fail_cover': '让负',
        'protected_win': '让胜',
        'protected_push': '让平',
        'protected_fail': '让负',
        'home_win': '胜',
        'draw': '平',
        'away_win': '负',
        'unknown': '未知语义'
    }.get(semantic_key, '未知语义')

    return {
        'key': f'rqspf:{direction}:{semantic_key}',
        'label': f'{direction_label}/{semantic_label}',
        'direction': direction,
        'direction_label': direction_label,
        'semantic': semantic_key,
        'semantic_label': semantic_label,
        'handicap': handicap
    }


def _resolve_rqspf_semantic(direction: str, pred_val: str | None) -> str:
    if direction == 'home_give':
        return {
            '胜': 'cover',
            '平': 'push',
            '负': 'fail_cover'
        }.get(pred_val, 'unknown')
    if direction == 'home_receive':
        return {
            '胜': 'protected_win',
            '平': 'protected_push',
            '负': 'protected_fail'
        }.get(pred_val, 'unknown')
    return {
        '胜': 'home_win',
        '平': 'draw',
        '负': 'away_win'
    }.get(pred_val, 'unknown')


def _build_per_predictor_stats(
    items: list[dict],
    predictors_pool: list[dict],
    fields: list[dict]
) -> list[dict]:
    """对每个方案，按字段分别统计 (有效预测数, 命中数, 命中率)。"""
    accum: dict[int, dict[str, dict]] = defaultdict(
        lambda: {f['key']: {'total': 0, 'hit': 0} for f in fields}
    )
    for item in items:
        pid = item['predictor_id']
        prediction = item.get('prediction') or {}
        hits = item.get('hit') or {}
        for field in fields:
            fkey = field['key']
            pred_val = prediction.get(fkey)
            hit_val = hits.get(fkey)
            # 跳过未预测/无法判定的
            if pred_val in (None, '', 'null'):
                continue
            if hit_val is None:
                continue
            accum[pid][fkey]['total'] += 1
            accum[pid][fkey]['hit'] += int(bool(hit_val))

    result = []
    for predictor in predictors_pool:
        pid = predictor['id']
        per_field = {}
        for field in fields:
            stat = accum.get(pid, {}).get(field['key'], {'total': 0, 'hit': 0})
            per_field[field['key']] = {
                'total': stat['total'],
                'hit': stat['hit'],
                'rate': _safe_rate(stat['hit'], stat['total'])
            }
        result.append({
            'predictor_id': pid,
            'predictor_name': predictor.get('name') or f"方案#{pid}",
            'engine_type': predictor.get('engine_type') or 'ai',
            'metrics': per_field
        })
    # 按 spf 命中率倒序，只是默认排序，前端可以再排
    primary_field = fields[0]['key'] if fields else None
    if primary_field:
        result.sort(
            key=lambda x: (x['metrics'][primary_field]['rate'] or 0),
            reverse=True
        )
    return result


def _build_per_predictor_segment_stats(
    items: list[dict],
    predictors_pool: list[dict],
    fields: list[dict]
) -> dict[str, list[dict]]:
    """按盘口语义分层统计单方案命中率；不把全量样本混入分层结果。"""
    name_lookup = {int(p['id']): p.get('name') or f"方案#{p['id']}" for p in predictors_pool}
    output: dict[str, list[dict]] = {}

    for field in fields:
        fkey = field['key']
        stats: dict[tuple, dict] = defaultdict(lambda: {'total': 0, 'hit': 0})
        for item in items:
            pred_val = (item.get('prediction') or {}).get(fkey)
            hit_val = (item.get('hit') or {}).get(fkey)
            if pred_val in (None, '', 'null') or hit_val is None:
                continue

            segment = _build_market_segment(item, fkey, pred_val)
            if segment['key'] == 'all':
                continue

            pid = int(item['predictor_id'])
            key = (pid, pred_val, segment['key'], segment['label'])
            stats[key]['total'] += 1
            stats[key]['hit'] += int(bool(hit_val))

        rows = []
        for (pid, pred_val, segment_key, segment_label), stat in stats.items():
            rows.append({
                'predictor_id': pid,
                'predictor_name': name_lookup.get(pid, f"方案#{pid}"),
                'value': pred_val,
                'market_segment': segment_key,
                'market_segment_label': segment_label,
                'total': stat['total'],
                'hit': stat['hit'],
                'rate': _safe_rate(stat['hit'], stat['total'])
            })
        rows.sort(key=lambda x: (
            str(x.get('market_segment_label') or ''),
            str(x.get('predictor_name') or ''),
            str(x.get('value') or ''),
            -int(x.get('total') or 0)
        ))
        output[fkey] = rows

    return output


def _compute_predictor_weights(
    per_predictor: list[dict],
    fields: list[dict],
    lottery_type: str = 'jingcai_football'
) -> dict[int, dict[str, float]]:
    """
    给每个方案在每个字段上算一个"质量权重"。

    定义：weight = (历史命中率 - 随机基准) / 100
    随机基准按彩种和字段查表：
      - 竞彩足球 spf/rqspf → 33.33（三选一）
      - PC28 combo → 25（四选一）

    每个彩种的最小样本量阈值不同（PC28 方案区分度小，需更多样本）。
    样本不足或 rate 缺失时 weight = 0（中性）。

    返回 {predictor_id: {field_key: float}}。
    """
    definition = get_lottery_definition(lottery_type)
    min_sample = int(definition.consensus_min_sample_for_weight or MIN_SAMPLE_FOR_WEIGHT)

    out: dict[int, dict[str, float]] = {}
    for entry in per_predictor or []:
        pid = entry.get('predictor_id')
        if pid is None:
            continue
        per_field: dict[str, float] = {}
        metrics = entry.get('metrics') or {}
        for field in fields:
            fkey = field['key']
            baseline = definition.baseline_for(fkey)
            metric = metrics.get(fkey) or {}
            rate = metric.get('rate')
            total = int(metric.get('total') or 0)
            if rate is None or total < min_sample:
                per_field[fkey] = 0.0
            else:
                per_field[fkey] = round((float(rate) - baseline) / 100.0, 4)
        out[int(pid)] = per_field
    return out


def _build_consensus_by_count(
    matches: dict[tuple, list[dict]],
    fields: list[dict]
) -> dict[str, list[dict]]:
    """
    对每场比赛：
      - 统计每个字段下，每个预测值被多少方案支持
      - 对该值的所有支持方案的命中数累计到 (n_agree, value) 分组
    输出按字段分组的列表。
    """
    output: dict[str, list[dict]] = {}
    for field in fields:
        fkey = field['key']
        bucket: dict[tuple, dict] = defaultdict(
            lambda: {'total': 0, 'hit': 0, 'match_total': 0, 'match_hit': 0}
        )
        for items in matches.values():
            value_supporters: dict[str, dict[int, dict]] = defaultdict(dict)
            for item in items:
                pred_val = (item.get('prediction') or {}).get(fkey)
                hit_val = (item.get('hit') or {}).get(fkey)
                if pred_val in (None, '', 'null') or hit_val is None:
                    continue
                # 按 predictor_id 去重：同一方案同一比赛只计一次
                pid = item.get('predictor_id')
                if pid is not None:
                    value_supporters[pred_val][pid] = item
            for pred_val, pid_map in value_supporters.items():
                supporters = list(pid_map.values())
                n_agree = len(supporters)
                if n_agree < 1:
                    continue
                segment = _build_market_segment(supporters[0], fkey, pred_val)
                segment_items = tuple(_iter_all_and_specific_segments(segment))
                match_hit = int(bool((supporters[0].get('hit') or {}).get(fkey)))
                for segment_key, segment_label in segment_items:
                    bucket[(n_agree, pred_val, segment_key, segment_label)]['match_total'] += 1
                    bucket[(n_agree, pred_val, segment_key, segment_label)]['match_hit'] += match_hit
                for sup in supporters:
                    hit_val = (sup.get('hit') or {}).get(fkey)
                    for segment_key, segment_label in segment_items:
                        bucket[(n_agree, pred_val, segment_key, segment_label)]['total'] += 1
                        bucket[(n_agree, pred_val, segment_key, segment_label)]['hit'] += int(bool(hit_val))

        rows = []
        for (n_agree, pred_val, segment_key, segment_label), stat in bucket.items():
            rows.append({
                'agree_count': n_agree,
                'value': pred_val,
                'market_segment': segment_key,
                'market_segment_label': segment_label,
                'total': stat['total'],
                'hit': stat['hit'],
                'rate': _safe_rate(stat['hit'], stat['total']),
                'match_total': stat['match_total'],
                'match_hit': stat['match_hit'],
                'match_rate': _safe_rate(stat['match_hit'], stat['match_total'])
            })
        rows.sort(key=lambda x: (x['agree_count'], x['value']))
        output[fkey] = rows
    return output


def _build_pair_combinations(
    matches: dict[tuple, list[dict]],
    predictor_ids: list[int],
    fields: list[dict]
) -> dict[str, list[dict]]:
    """对每对方案，统计他们预测一致时的命中率（合并两人的命中样本）。"""
    output: dict[str, list[dict]] = {}
    sorted_pids = sorted(predictor_ids)
    for field in fields:
        fkey = field['key']
        pair_stats: dict[tuple, dict] = defaultdict(lambda: {'total': 0, 'hit': 0})
        for items in matches.values():
            preds_by_pid: dict[int, dict] = {}
            for item in items:
                pred_val = (item.get('prediction') or {}).get(fkey)
                hit_val = (item.get('hit') or {}).get(fkey)
                if pred_val in (None, '', 'null') or hit_val is None:
                    continue
                preds_by_pid[item['predictor_id']] = item
            for p1, p2 in combinations(sorted_pids, 2):
                if p1 not in preds_by_pid or p2 not in preds_by_pid:
                    continue
                v1 = preds_by_pid[p1]['prediction'][fkey]
                v2 = preds_by_pid[p2]['prediction'][fkey]
                if v1 != v2:
                    continue
                # 两个方案预测一致：同一比赛同一预测值，命中结果相同，只计 1 次比赛样本
                h = preds_by_pid[p1]['hit'].get(fkey)
                pair_stats[(p1, p2)]['total'] += 1
                pair_stats[(p1, p2)]['hit'] += int(bool(h))

        rows = []
        for (p1, p2), stat in pair_stats.items():
            rows.append({
                'pair': [p1, p2],
                'total': stat['total'],
                'hit': stat['hit'],
                'rate': _safe_rate(stat['hit'], stat['total'])
            })
        rows.sort(key=lambda x: (-(x['rate'] or 0), -x['total']))
        output[fkey] = rows
    return output


def _build_pair_segment_combinations(
    matches: dict[tuple, list[dict]],
    predictor_ids: list[int],
    fields: list[dict]
) -> dict[str, list[dict]]:
    """按盘口语义分层统计两两方案同值一致时的比赛命中率。"""
    output: dict[str, list[dict]] = {}
    sorted_pids = sorted(predictor_ids)

    for field in fields:
        fkey = field['key']
        stats: dict[tuple, dict] = defaultdict(lambda: {'total': 0, 'hit': 0})
        for items in matches.values():
            preds_by_pid: dict[int, dict] = {}
            for item in items:
                pred_val = (item.get('prediction') or {}).get(fkey)
                hit_val = (item.get('hit') or {}).get(fkey)
                if pred_val in (None, '', 'null') or hit_val is None:
                    continue
                preds_by_pid[int(item['predictor_id'])] = item

            for p1, p2 in combinations(sorted_pids, 2):
                if p1 not in preds_by_pid or p2 not in preds_by_pid:
                    continue
                pred_val = (preds_by_pid[p1].get('prediction') or {}).get(fkey)
                if pred_val != (preds_by_pid[p2].get('prediction') or {}).get(fkey):
                    continue

                segment = _build_market_segment(preds_by_pid[p1], fkey, pred_val)
                if segment['key'] == 'all':
                    continue

                key = (p1, p2, pred_val, segment['key'], segment['label'])
                stats[key]['total'] += 1
                stats[key]['hit'] += int(bool((preds_by_pid[p1].get('hit') or {}).get(fkey)))

        rows = []
        for (p1, p2, pred_val, segment_key, segment_label), stat in stats.items():
            rows.append({
                'pair': [p1, p2],
                'value': pred_val,
                'market_segment': segment_key,
                'market_segment_label': segment_label,
                'total': stat['total'],
                'hit': stat['hit'],
                'rate': _safe_rate(stat['hit'], stat['total'])
            })
        rows.sort(key=lambda x: (
            str(x.get('market_segment_label') or ''),
            tuple(x.get('pair') or []),
            str(x.get('value') or ''),
            -int(x.get('total') or 0)
        ))
        output[fkey] = rows

    return output


def _build_low_hit_signals(
    *,
    matches: dict[tuple, list[dict]],
    predictor_ids: list[int],
    predictors_pool: list[dict],
    fields: list[dict]
) -> dict[str, list[dict]]:
    """
    统计“低命中排除”信号。

    与 pair_combinations 不同，这里必须按预测值拆分，并且统一使用比赛场次作为样本数：
    - consensus_count：N 个方案同场支持同一值时，该值的历史命中率
    - combo_2 / combo_3：具体 2/3 个方案同场支持同一值时，该值的历史命中率
    """
    name_lookup = {int(p['id']): p.get('name') or f"方案#{p['id']}" for p in predictors_pool}
    output: dict[str, list[dict]] = {}

    for field in fields:
        fkey = field['key']
        rows: list[dict] = []

        consensus_stats = _collect_consensus_count_match_stats(matches, fkey)
        for (agree_count, value, segment_key, segment_label), stat in consensus_stats.items():
            signal = _make_low_hit_signal(
                field_key=fkey,
                signal_type='consensus_count',
                value=value,
                sample_matches=stat['total'],
                hit_matches=stat['hit'],
                predictor_ids=[],
                predictor_names=[],
                agree_count=agree_count,
                market_segment=segment_key,
                market_segment_label=segment_label
            )
            if signal:
                rows.append(signal)

        for combo_size in range(2, LOW_HIT_MAX_COMBO_SIZE + 1):
            combo_stats = _collect_value_combo_match_stats(
                matches=matches,
                predictor_ids=predictor_ids,
                field_key=fkey,
                combo_size=combo_size
            )
            for (combo_ids, value, segment_key, segment_label), stat in combo_stats.items():
                ids = list(combo_ids)
                signal = _make_low_hit_signal(
                    field_key=fkey,
                    signal_type=f'combo_{combo_size}',
                    value=value,
                    sample_matches=stat['total'],
                    hit_matches=stat['hit'],
                    predictor_ids=ids,
                    predictor_names=[name_lookup.get(pid, str(pid)) for pid in ids],
                    agree_count=combo_size,
                    market_segment=segment_key,
                    market_segment_label=segment_label
                )
                if signal:
                    rows.append(signal)

        rows.sort(key=lambda row: (
            -int(row.get('severity') or 0),
            float(row.get('rate') if row.get('rate') is not None else 100.0),
            -int(row.get('sample_matches') or 0),
            row.get('type') or '',
            row.get('value') or ''
        ))
        output[fkey] = rows

    return output


def _collect_consensus_count_match_stats(
    matches: dict[tuple, list[dict]],
    field_key: str
) -> dict[tuple, dict]:
    stats: dict[tuple, dict] = defaultdict(lambda: {'total': 0, 'hit': 0})
    for items in matches.values():
        value_supporters: dict[str, list[dict]] = defaultdict(list)
        for item in items:
            pred_val = (item.get('prediction') or {}).get(field_key)
            hit_val = (item.get('hit') or {}).get(field_key)
            if pred_val in (None, '', 'null') or hit_val is None:
                continue
            value_supporters[pred_val].append(item)

        for value, supporters in value_supporters.items():
            if not supporters:
                continue
            segment = _build_market_segment(supporters[0], field_key, value)
            for segment_key, segment_label in _iter_all_and_specific_segments(segment):
                key = (len(supporters), value, segment_key, segment_label)
                stats[key]['total'] += 1
                stats[key]['hit'] += int(bool((supporters[0].get('hit') or {}).get(field_key)))
    return stats


def _collect_value_combo_match_stats(
    *,
    matches: dict[tuple, list[dict]],
    predictor_ids: list[int],
    field_key: str,
    combo_size: int
) -> dict[tuple, dict]:
    stats: dict[tuple, dict] = defaultdict(lambda: {'total': 0, 'hit': 0})
    predictor_id_set = set(int(pid) for pid in predictor_ids)

    for items in matches.values():
        value_supporters: dict[str, list[tuple[int, int, dict]]] = defaultdict(list)
        for item in items:
            pid = int(item['predictor_id'])
            if pid not in predictor_id_set:
                continue
            pred_val = (item.get('prediction') or {}).get(field_key)
            hit_val = (item.get('hit') or {}).get(field_key)
            if pred_val in (None, '', 'null') or hit_val is None:
                continue
            value_supporters[pred_val].append((pid, int(bool(hit_val)), item))

        for value, supporters in value_supporters.items():
            if len(supporters) < combo_size:
                continue
            for combo in combinations(sorted(supporters), combo_size):
                combo_ids = tuple(pid for pid, _, _ in combo)
                segment = _build_market_segment(combo[0][2], field_key, value)
                for segment_key, segment_label in _iter_all_and_specific_segments(segment):
                    key = (combo_ids, value, segment_key, segment_label)
                    stats[key]['total'] += 1
                    stats[key]['hit'] += combo[0][1]
    return stats


def _make_low_hit_signal(
    *,
    field_key: str,
    signal_type: str,
    value: str,
    sample_matches: int,
    hit_matches: int,
    predictor_ids: list[int],
    predictor_names: list[str],
    agree_count: int,
    market_segment: str = 'all',
    market_segment_label: str = '全部样本'
) -> dict | None:
    rate = _safe_rate(hit_matches, sample_matches)
    if rate is None:
        return None
    level = _classify_low_hit_level(sample_matches, rate)
    if not level:
        return None

    level_label = {
        'strong': '高风险',
        'weak': '谨慎参考',
        'watch': '观察'
    }[level]
    severity = {'strong': 3, 'weak': 2, 'watch': 1}[level]

    return {
        'field': field_key,
        'type': signal_type,
        'value': value,
        'predictor_ids': predictor_ids,
        'predictor_names': predictor_names,
        'agree_count': agree_count,
        'market_segment': market_segment,
        'market_segment_label': market_segment_label,
        'sample_matches': sample_matches,
        'hit_matches': hit_matches,
        'rate': rate,
        'level': level,
        'level_label': level_label,
        'severity': severity,
        'reason': _format_low_hit_reason(sample_matches, rate, level)
    }


def _classify_low_hit_level(sample_matches: int, rate: float) -> str | None:
    if sample_matches < LOW_HIT_MIN_SAMPLE:
        return None
    if sample_matches >= LOW_HIT_HIGH_SAMPLE:
        if rate < LOW_HIT_STRONG_RATE:
            return 'strong'
        if rate < LOW_HIT_WEAK_RATE:
            return 'weak'
        return None
    if sample_matches >= LOW_HIT_MEDIUM_SAMPLE:
        if rate < LOW_HIT_STRONG_RATE:
            return 'strong'
        if rate < LOW_HIT_WEAK_RATE:
            return 'weak'
        return None
    if rate < LOW_HIT_STRONG_RATE:
        return 'watch'
    return None


def _format_low_hit_reason(sample_matches: int, rate: float, level: str) -> str:
    if level == 'strong':
        return f"历史 {sample_matches} 场，命中率 {rate:.2f}%，低于高风险阈值"
    if level == 'weak':
        return f"历史 {sample_matches} 场，命中率 {rate:.2f}%，低于谨慎参考阈值"
    return f"历史 {sample_matches} 场，命中率 {rate:.2f}%，样本偏少，仅作观察"


def _build_today_recommendations(
    *,
    pending_items: list[dict],
    consensus_by_count: dict[str, list[dict]],
    pair_combinations: dict[str, list[dict]],
    pair_segment_combinations: dict[str, list[dict]] | None,
    low_hit_signals: dict[str, list[dict]],
    predictors_pool: list[dict],
    fields: list[dict],
    predictor_weights: dict[int, dict[str, float]] | None = None
) -> list[dict]:
    """
    对每场未结算比赛：
      - 找出每个字段的共识值（票数最多的预测值）
      - 关联历史"共识=N且值=V"时的命中率作为参考（粗粒度分组）
      - 当共识方案数 >= 2 时，优先查"实际这几个方案 + 同盘口类型"的两两组合命中率，
        缺失时回退全量两两组合，让强方案集合的真实表现可以独立判断
      - 标记 is_reliable：粗粒度分组样本 >= MIN_RELIABLE_SAMPLE
      - 计算 weighted_strength：本场该字段所有支持方案的"质量权重"加和。
        仅用于后台排序，前端不展示，让"强方案集合一致"排在"含烂方案的群体共识"之前。
    """
    if not pending_items:
        return []
    name_lookup = {p['id']: p.get('name') or f"方案#{p['id']}" for p in predictors_pool}
    matches = _group_by_match(pending_items)
    weights_lookup = predictor_weights or {}

    # 把 consensus_by_count 转成 dict 便于查询
    rate_lookup = {
        fkey: {(row['agree_count'], row['value'], row.get('market_segment') or 'all'): row for row in rows}
        for fkey, rows in consensus_by_count.items()
    }

    # pair_combinations 转 lookup：(p1, p2) (排序后) -> 行
    pair_lookup: dict[str, dict[tuple, dict]] = {}
    for fkey, rows in pair_combinations.items():
        sub: dict[tuple, dict] = {}
        for row in rows:
            pair = row.get('pair') or []
            if len(pair) != 2:
                continue
            key = tuple(sorted(int(x) for x in pair))
            sub[key] = row
        pair_lookup[fkey] = sub

    pair_segment_lookup: dict[str, dict[tuple, dict]] = {}
    for fkey, rows in (pair_segment_combinations or {}).items():
        sub: dict[tuple, dict] = {}
        for row in rows:
            pair = row.get('pair') or []
            if len(pair) != 2:
                continue
            value = row.get('value')
            segment_key = row.get('market_segment')
            if value in (None, '', 'null') or not segment_key:
                continue
            key = (*tuple(sorted(int(x) for x in pair)), value, segment_key)
            sub[key] = row
        pair_segment_lookup[fkey] = sub

    low_hit_lookup = _build_low_hit_lookup(low_hit_signals)

    recommendations = []
    pool_size = len(predictors_pool)
    for (run_key, event_key), items in matches.items():
        # 取该场任意一项的标题
        title = next((it.get('title') or '' for it in items), '')
        per_field_rec = []
        for field in fields:
            fkey = field['key']
            supporter_items_by_value: dict[str, list[dict]] = defaultdict(list)
            value_supporters: dict[str, list[int]] = defaultdict(list)
            for item in items:
                pred_val = (item.get('prediction') or {}).get(fkey)
                if pred_val in (None, '', 'null'):
                    continue
                value_supporters[pred_val].append(item['predictor_id'])
                supporter_items_by_value[pred_val].append(item)
            if not value_supporters:
                continue
            # 共识值 = 票数最多的，若并列取首个
            consensus_value, supporters = max(
                value_supporters.items(),
                key=lambda kv: (len(kv[1]), kv[0])
            )
            agree_count = len(supporters)
            segment = _build_market_segment((supporter_items_by_value.get(consensus_value) or [items[0]])[0], fkey, consensus_value)
            historical = rate_lookup.get(fkey, {}).get((agree_count, consensus_value, segment['key']))
            if not historical:
                historical = rate_lookup.get(fkey, {}).get((agree_count, consensus_value, 'all'))
            historical_rate = historical.get('match_rate') if historical else None
            historical_sample = int(historical.get('match_total') if historical else 0)
            is_reliable = historical_sample >= MIN_RELIABLE_SAMPLE and historical_rate is not None

            # 细粒度：实际共识方案两两组合的历史表现
            pair_breakdown = _build_pair_breakdown_for_supporters(
                supporters=supporters,
                field_key=fkey,
                prediction_value=consensus_value,
                market_segment=segment,
                pair_lookup=pair_lookup.get(fkey) or {},
                pair_segment_lookup=pair_segment_lookup.get(fkey) or {},
                name_lookup=name_lookup
            )

            # 加权共识强度：本场该字段所有支持方案的权重加和。
            # 强方案权重 +0.10~+0.30、烂方案权重 -0.20~-0.05、新/中性方案 0。
            # 用于排序时给"强方案集合"加分、给"含烂方案的群体共识"减分。
            weighted_strength = round(sum(
                (weights_lookup.get(int(pid), {}) or {}).get(fkey, 0.0)
                for pid in supporters
            ), 4)
            low_hit_by_value = _build_low_hit_matches_for_field(
                value_supporters=value_supporters,
                supporter_items_by_value=supporter_items_by_value,
                field_key=fkey,
                low_hit_lookup=low_hit_lookup.get(fkey) or {}
            )

            per_field_rec.append({
                'field': fkey,
                'field_label': field['label'],
                'consensus_value': consensus_value,
                'market_segment': segment['key'],
                'market_segment_label': segment['label'],
                'agree_count': agree_count,
                'pool_size': pool_size,
                'predicting_count': sum(len(pids) for pids in value_supporters.values()),
                'supporters': supporters,
                'supporter_names': [name_lookup.get(pid, str(pid)) for pid in supporters],
                'all_predictions': {
                    val: {
                        'count': len(pids),
                        'predictors': pids
                    }
                    for val, pids in value_supporters.items()
                },
                # 粗粒度分组：所有 N 方案一致预测同值的历史平均命中率
                'historical_rate': historical_rate,
                'historical_sample': historical_sample,
                'is_reliable': is_reliable,
                'reliability_threshold': MIN_RELIABLE_SAMPLE,
                # 细粒度：实际方案两两组合
                'pair_breakdown': pair_breakdown,
                # 加权信号（仅用于后台排序，前端不展示）
                'weighted_strength': weighted_strength,
                # 低命中排除：按本场各预测值分别匹配历史低命中规则。
                'low_hit_by_value': low_hit_by_value
            })
        if per_field_rec:
            recommendations.append({
                'run_key': run_key,
                'event_key': event_key,
                'title': title,
                'fields': per_field_rec
            })

    # 排序：贝叶斯收缩 — 小样本不会因为偶然 100% 排到前面；
    # 同时叠加 weighted_strength：让"强方案一致"高于"含烂方案的群体共识"。
    # weighted_strength 本身就是该字段所有支持方案的权重之和，
    # 已经反映了"多少强方案 + 多少烂方案"的净效益，不需要再乘 agree_count。
    # 我们把它放大到与 rate 同量级（× 100），并乘上一个可调系数 W_BOOST 来强化加权奖励。
    # W_BOOST = 2.5 让烂方案带来的负权重足以"压过 1 个共识方案的 agree×rate 增量"，
    # 实战意义：含 1 个烂方案的 N+1 人共识不应该排在干净的 N 人共识前面。
    W_BOOST = 2.5
    def score(rec):
        best = float('-inf')
        for f in rec['fields']:
            agree = f.get('agree_count') or 0
            sample = f.get('historical_sample') or 0
            shrinkage = min(1.0, sample / float(MIN_RELIABLE_SAMPLE)) if MIN_RELIABLE_SAMPLE > 0 else 1.0
            coarse_rate = (f.get('historical_rate') or 0) * shrinkage
            coarse_score = agree * coarse_rate
            pair_avg = (f.get('pair_breakdown') or {}).get('avg_rate') or 0
            pair_n = (f.get('pair_breakdown') or {}).get('total_sample') or 0
            pair_shrinkage = min(1.0, pair_n / float(MIN_RELIABLE_SAMPLE)) if MIN_RELIABLE_SAMPLE > 0 else 1.0
            pair_score = pair_avg * pair_shrinkage * agree
            # weighted_strength 是支持方案"质量加和"，正值代表强方案多，负值代表烂方案多。
            # 放大到 [-50, +100] 范围（× 100 × W_BOOST）；含烂方案时 weighted 为负或较低。
            weighted_bonus = (f.get('weighted_strength') or 0) * 100 * W_BOOST
            best = max(
                best,
                coarse_score + weighted_bonus,
                pair_score + weighted_bonus
            )
        return best
    recommendations.sort(key=score, reverse=True)
    return recommendations


def _build_low_hit_lookup(low_hit_signals: dict[str, list[dict]]) -> dict[str, dict[str, list[dict]]]:
    lookup: dict[str, dict[str, list[dict]]] = {}
    for field_key, rows in (low_hit_signals or {}).items():
        by_value: dict[str, list[dict]] = defaultdict(list)
        for row in rows or []:
            value = row.get('value')
            if value in (None, '', 'null'):
                continue
            by_value[str(value)].append(row)
        lookup[field_key] = dict(by_value)
    return lookup


def _build_low_hit_matches_for_field(
    *,
    value_supporters: dict[str, list[int]],
    supporter_items_by_value: dict[str, list[dict]],
    field_key: str,
    low_hit_lookup: dict[str, list[dict]]
) -> dict[str, dict]:
    matched: dict[str, dict] = {}
    for value, supporters in value_supporters.items():
        supporters_set = set(int(pid) for pid in supporters)
        segment = _build_market_segment((supporter_items_by_value.get(value) or [{}])[0], field_key, value)
        signals = []
        for signal in low_hit_lookup.get(str(value), []):
            signal_segment = signal.get('market_segment') or 'all'
            if signal_segment not in {'all', segment['key']}:
                continue
            signal_type = signal.get('type')
            if signal_type == 'consensus_count':
                if int(signal.get('agree_count') or 0) != len(supporters_set):
                    continue
            else:
                predictor_ids = set(int(pid) for pid in (signal.get('predictor_ids') or []))
                if not predictor_ids or not predictor_ids.issubset(supporters_set):
                    continue
            signals.append(signal)

        if not signals:
            continue

        strongest = max(signals, key=lambda row: int(row.get('severity') or 0))
        lowest = min(signals, key=lambda row: (
            float(row.get('rate') if row.get('rate') is not None else 100.0),
            -int(row.get('sample_matches') or 0)
        ))
        level = strongest.get('level') or 'watch'
        severity = int(strongest.get('severity') or 1)
        if severity == 1 and len(signals) >= 2:
            level = 'weak'
            severity = 2

        level_label = {
            'strong': '高风险',
            'weak': '谨慎参考',
            'watch': '观察'
        }.get(level, '观察')

        matched[str(value)] = {
            'field': field_key,
            'value': value,
            'market_segment': segment['key'],
            'market_segment_label': segment['label'],
            'level': level,
            'level_label': level_label,
            'severity': severity,
            'signal_count': len(signals),
            'best_rate': lowest.get('rate'),
            'best_sample_matches': lowest.get('sample_matches'),
            'best_hit_matches': lowest.get('hit_matches'),
            'signals': sorted(signals, key=lambda row: (
                float(row.get('rate') if row.get('rate') is not None else 100.0),
                -int(row.get('severity') or 0),
                -int(row.get('sample_matches') or 0)
            ))[:5]
        }
    return matched


def _build_pair_breakdown_for_supporters(
    *,
    supporters: list[int],
    field_key: str,
    prediction_value: str,
    market_segment: dict,
    pair_lookup: dict[tuple, dict],
    pair_segment_lookup: dict[tuple, dict],
    name_lookup: dict[int, str]
) -> dict:
    """
    给定本场实际共识的 supporters（>=2 个方案），优先从同盘口类型组合查历史命中率，
    缺失时回退全量组合，并计算 avg_rate / max_rate。

    返回结构：
        {
          "pairs": [
              {"pair":[p1,p2], "names":[n1,n2], "rate":..., "total":..., "hit":...},
              ...
          ],
          "avg_rate": float | None,    # 加权平均（按各 pair 的 total）
          "max_rate": float | None,
          "max_pair": [p1, p2] | None,
          "total_sample": int           # 所有 pair 的 total 之和
        }
    """
    pairs_out: list[dict] = []
    if len(supporters) < 2:
        return {'pairs': [], 'avg_rate': None, 'max_rate': None,
                'max_pair': None, 'total_sample': 0, 'source': 'none'}

    segment_key = (market_segment or {}).get('key') or 'all'
    segment_label = (market_segment or {}).get('label') or '全部样本'
    segment_match_count = 0
    for p1, p2 in combinations(sorted(supporters), 2):
        row = None
        source = 'all'
        if segment_key != 'all':
            row = pair_segment_lookup.get((p1, p2, prediction_value, segment_key))
            if row:
                source = 'segment'
                segment_match_count += 1
        if not row:
            row = pair_lookup.get((p1, p2))
        if not row:
            continue
        pairs_out.append({
            'pair': [p1, p2],
            'names': [name_lookup.get(p1, str(p1)), name_lookup.get(p2, str(p2))],
            'source': source,
            'market_segment': row.get('market_segment') if source == 'segment' else 'all',
            'market_segment_label': row.get('market_segment_label') if source == 'segment' else '全部样本',
            'rate': row.get('rate'),
            'total': int(row.get('total') or 0),
            'hit': int(row.get('hit') or 0)
        })

    if not pairs_out:
        return {'pairs': [], 'avg_rate': None, 'max_rate': None,
                'max_pair': None, 'total_sample': 0, 'source': 'none'}

    total_sample = sum(p['total'] for p in pairs_out)
    if total_sample <= 0:
        avg_rate = None
    else:
        weighted_hit = sum(p['hit'] for p in pairs_out)
        avg_rate = round(100.0 * weighted_hit / total_sample, 2)

    max_p = max(pairs_out, key=lambda p: p.get('rate') or -1)
    return {
        'pairs': sorted(pairs_out, key=lambda p: -(p.get('rate') or 0)),
        'avg_rate': avg_rate,
        'max_rate': max_p.get('rate'),
        'max_pair': max_p.get('pair'),
        'total_sample': total_sample,
        'source': 'segment' if segment_match_count else 'all',
        'segment_match_count': segment_match_count,
        'market_segment': segment_key if segment_match_count else 'all',
        'market_segment_label': segment_label if segment_match_count else '全部样本'
    }


def _safe_rate(hit: int, total: int) -> float | None:
    if total <= 0:
        return None
    return round(100.0 * hit / total, 2)


def _load_archived_per_predictor(
    db,
    *,
    predictor_ids: list[int],
    time_window_days: int | None,
    fields: list[dict]
) -> dict[int, dict[str, dict]]:
    """
    从 jingcai_prediction_daily_summary 读取已归档的单方案历史命中率。
    返回结构：{predictor_id: {field_key: {'total': N, 'hit': M}}}

    daily_summary 是日粒度聚合，丢失了 (run_key, event_key) 明细，所以只能用于
    重建"单方案历史命中率"这个指标，无法重建两两组合或共识规律。
    """
    if not predictor_ids:
        return {}

    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        placeholders = ','.join('?' for _ in predictor_ids)
        sql = f"""
            SELECT predictor_id, summary_date, hit_breakdown_json
            FROM jingcai_prediction_daily_summary
            WHERE predictor_id IN ({placeholders})
        """
        params: list[Any] = list(predictor_ids)
        if time_window_days is not None:
            cutoff = (datetime.utcnow() - timedelta(days=int(time_window_days))).strftime('%Y-%m-%d')
            sql += " AND summary_date >= ?"
            params.append(cutoff)
        cursor.execute(sql, params)
        rows = cursor.fetchall()
    except Exception:
        # 表可能还不存在（旧库），忽略即可
        return {}
    finally:
        conn.close()

    result: dict[int, dict[str, dict]] = defaultdict(
        lambda: {f['key']: {'total': 0, 'hit': 0} for f in fields}
    )
    field_keys = {f['key'] for f in fields}
    for row in rows:
        pid = int(row['predictor_id'])
        try:
            breakdown = json.loads(row['hit_breakdown_json'] or '{}')
        except (TypeError, ValueError):
            continue
        if not isinstance(breakdown, dict):
            continue
        for field_key, stat in breakdown.items():
            if field_key not in field_keys or not isinstance(stat, dict):
                continue
            result[pid][field_key]['total'] += int(stat.get('total') or 0)
            result[pid][field_key]['hit'] += int(stat.get('hit') or 0)
    return dict(result)


def _merge_per_predictor_with_archive(
    per_predictor: list[dict],
    archive: dict[int, dict[str, dict]],
    fields: list[dict]
) -> list[dict]:
    """把归档数据加到 per_predictor 的 total/hit 中并重算 rate。"""
    if not archive:
        return per_predictor
    for entry in per_predictor:
        pid = entry['predictor_id']
        archive_entry = archive.get(pid)
        if not archive_entry:
            continue
        for field in fields:
            fkey = field['key']
            archived = archive_entry.get(fkey) or {}
            metric = entry['metrics'][fkey]
            metric['total'] += int(archived.get('total') or 0)
            metric['hit'] += int(archived.get('hit') or 0)
            metric['rate'] = _safe_rate(metric['hit'], metric['total'])
    # 重新按主字段排序
    primary_field = fields[0]['key'] if fields else None
    if primary_field:
        per_predictor.sort(
            key=lambda x: (x['metrics'][primary_field]['rate'] or 0),
            reverse=True
        )
    return per_predictor


def build_export_envelope(analysis: dict, *, scope: str) -> dict:
    """把分析结果包成与 PC28 export 风格一致的标准信封。"""
    return {
        'schema_version': '1.0',
        'source_type': 'ai_trading_simulator',
        'export_type': 'consensus_analysis',
        'lottery_type': analysis.get('lottery_type'),
        'scope': scope,
        'window_days': analysis.get('window_days'),
        'generated_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'data': analysis
    }


def build_pc28_consensus_execution_export(
    db,
    *,
    predictor_ids: list[int] | None = None,
    field: str = 'odd_even',
    min_agreement: int = 2,
    min_historical_rate: float | None = None,
    min_historical_sample: int = 0,
    time_window_days: int | None = 7,
) -> dict:
    """生成单条可执行的 PC28 共识信号。

    共识计算仍复用历史分析服务；本函数只负责把最新满足门槛的共识
    转换成 pc28touzhu 可消费的 execution-view，避免执行侧重复实现预测逻辑。
    """
    normalized_field = str(field or '').strip()
    if normalized_field not in {'big_small', 'odd_even', 'combo'}:
        raise ValueError('field 仅支持 big_small、odd_even 或 combo')
    agreement = max(2, int(min_agreement or 2))
    historical_rate_floor = None
    if min_historical_rate not in (None, ''):
        historical_rate_floor = float(min_historical_rate)
        if historical_rate_floor < 0 or historical_rate_floor > 100:
            raise ValueError('min_historical_rate 必须在 0 到 100 之间')
    historical_sample_floor = max(0, int(min_historical_sample or 0))
    analysis = build_consensus_analysis(
        db,
        user_id=None,
        lottery_type='pc28',
        time_window_days=time_window_days,
        predictor_ids=predictor_ids,
    )
    pool = analysis.get('predictors') or []
    pool_ids = sorted(int(item['id']) for item in pool if item.get('id') is not None)
    candidates = []
    for recommendation in analysis.get('today_recommendations') or []:
        issue_no = str(recommendation.get('event_key') or '').strip()
        if not issue_no:
            continue
        field_row = next(
            (item for item in recommendation.get('fields') or [] if item.get('field') == normalized_field),
            None,
        )
        if not field_row or int(field_row.get('agree_count') or 0) < agreement:
            continue
        distributions = field_row.get('all_predictions') or {}
        counts = sorted(
            (int(item.get('count') or 0) for item in distributions.values()),
            reverse=True,
        )
        if len(counts) > 1 and counts[0] == counts[1]:
            continue
        value = str(field_row.get('consensus_value') or '').strip()
        if not value:
            continue
        historical_rate = field_row.get('historical_rate')
        historical_sample = int(field_row.get('historical_sample') or 0)
        if historical_sample < historical_sample_floor:
            continue
        if historical_rate_floor is not None:
            if historical_rate is None or float(historical_rate) < historical_rate_floor:
                continue
        candidates.append((int(issue_no) if issue_no.isdigit() else -1, issue_no, field_row))

    candidates.sort(key=lambda item: item[0], reverse=True)
    generated_at = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    if not candidates:
        return {
            'schema_version': '1.0',
            'source_type': 'ai_trading_simulator',
            'export_type': 'consensus_execution',
            'lottery_type': 'pc28',
            'field': normalized_field,
            'min_agreement': agreement,
            'min_historical_rate': historical_rate_floor,
            'min_historical_sample': historical_sample_floor,
            'predictor_ids': pool_ids,
            'generated_at': generated_at,
            'items': [],
        }

    _, issue_no, field_row = candidates[0]
    value = str(field_row['consensus_value'])
    supporter_ids = sorted(int(item) for item in field_row.get('supporters') or [])
    historical_rate = field_row.get('historical_rate')
    confidence = (
        round(float(historical_rate) / 100.0, 4)
        if historical_rate is not None
        else round(len(supporter_ids) / max(1, len(pool_ids)), 4)
    )
    config_parts = [normalized_field, str(agreement)]
    if historical_rate_floor is not None:
        config_parts.extend([
            'rate%s' % ('%g' % historical_rate_floor),
            'sample%s' % historical_sample_floor,
        ])
    config_parts.append('-'.join(str(item) for item in pool_ids))
    config_key = '-'.join(config_parts)
    signal = {
        'bet_type': normalized_field,
        'bet_value': value,
        'confidence': confidence,
        'message_text': f'{value}10',
        'normalized_payload': {
            'primary_metric': normalized_field,
            'consensus_count': len(supporter_ids),
            'pool_size': len(pool_ids),
            'supporter_predictor_ids': supporter_ids,
            'predictor_ids': pool_ids,
            'consensus_config': config_key,
            'historical_rate': historical_rate,
            'historical_sample': int(field_row.get('historical_sample') or 0),
            'historical_rate_threshold': historical_rate_floor,
            'historical_sample_threshold': historical_sample_floor,
            'profit_rule_id': 'pc28_high',
            'odds_profile': 'regular',
            'share_level': 'records',
        },
    }
    return {
        'schema_version': '1.0',
        'source_type': 'ai_trading_simulator',
        'export_type': 'consensus_execution',
        'lottery_type': 'pc28',
        'field': normalized_field,
        'min_agreement': agreement,
        'min_historical_rate': historical_rate_floor,
        'min_historical_sample': historical_sample_floor,
        'predictor_ids': pool_ids,
        'generated_at': generated_at,
        'items': [{
            'schema_version': '1.0',
            'signal_id': f'pc28-consensus-{config_key}-{issue_no}',
            'source_type': 'ai_trading_simulator',
            'source_ref': {
                'platform': 'AITradingSimulator',
                'algorithm_key': 'pc28_consensus_v1',
                'consensus_config': config_key,
                'predictor_ids': pool_ids,
                'supporter_predictor_ids': supporter_ids,
                'historical_rate': historical_rate,
                'historical_sample': int(field_row.get('historical_sample') or 0),
            },
            'lottery_type': 'pc28',
            'issue_no': issue_no,
            'published_at': generated_at,
            'signals': [signal],
        }],
    }


def build_pc28_best_pair_execution_export(
    db,
    *,
    predictor_ids: list[int] | None = None,
    min_historical_rate: float = 28.0,
    min_historical_sample: int = 300,
    time_window_days: int = 30,
) -> dict:
    """从当期同值共识的 PC28 方案对中选择历史命中率最高者。"""
    historical_rate_floor = float(min_historical_rate)
    if historical_rate_floor < 0 or historical_rate_floor > 100:
        raise ValueError('min_historical_rate 必须在 0 到 100 之间')
    historical_sample_floor = max(1, int(min_historical_sample or 0))
    window_days = max(1, int(time_window_days or 30))

    analysis = build_consensus_analysis(
        db,
        user_id=None,
        lottery_type='pc28',
        time_window_days=window_days,
        predictor_ids=predictor_ids,
    )
    pool = analysis.get('predictors') or []
    pool_ids = sorted(int(item['id']) for item in pool if item.get('id') is not None)
    name_lookup = {
        int(item['id']): item.get('name') or f"方案#{item['id']}"
        for item in pool
        if item.get('id') is not None
    }
    generated_at = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    envelope = {
        'schema_version': '1.0',
        'source_type': 'ai_trading_simulator',
        'export_type': 'best_consensus_pair_execution',
        'lottery_type': 'pc28',
        'field': 'combo',
        'selection_mode': 'best_consensus_pair',
        'min_historical_rate': historical_rate_floor,
        'min_historical_sample': historical_sample_floor,
        'window_days': window_days,
        'predictor_ids': pool_ids,
        'generated_at': generated_at,
        'qualified_pairs': [],
        'items': [],
    }
    if not pool_ids:
        return envelope

    pair_lookup = {}
    for row in (analysis.get('pair_combinations') or {}).get('combo') or []:
        pair = tuple(sorted(int(item) for item in (row.get('pair') or [])))
        if len(pair) != 2:
            continue
        pair_lookup[pair] = {
            'predictor_ids': list(pair),
            'predictor_names': [name_lookup.get(item, f'方案#{item}') for item in pair],
            'historical_rate': row.get('rate'),
            'historical_sample': int(row.get('total') or 0),
            'historical_hit': int(row.get('hit') or 0),
        }

    recommendations = sorted(
        analysis.get('today_recommendations') or [],
        key=lambda item: (
            int(item.get('event_key'))
            if str(item.get('event_key') or '').isdigit()
            else -1
        ),
        reverse=True,
    )
    if not recommendations:
        return envelope

    recommendation = recommendations[0]
    issue_no = str(recommendation.get('event_key') or '').strip()
    field_row = next(
        (item for item in recommendation.get('fields') or [] if item.get('field') == 'combo'),
        None,
    )
    if not issue_no or not field_row:
        return envelope

    qualified = []
    for value, distribution in (field_row.get('all_predictions') or {}).items():
        supporters = sorted(int(item) for item in (distribution.get('predictors') or []))
        for pair in combinations(supporters, 2):
            pair_row = pair_lookup.get(tuple(pair))
            if not pair_row:
                continue
            rate = pair_row.get('historical_rate')
            sample = int(pair_row.get('historical_sample') or 0)
            if rate is None or float(rate) < historical_rate_floor:
                continue
            if sample < historical_sample_floor:
                continue
            qualified.append({
                **pair_row,
                'bet_value': str(value),
            })

    qualified.sort(
        key=lambda item: (
            -float(item['historical_rate']),
            -int(item['historical_sample']),
            tuple(item['predictor_ids']),
        )
    )
    envelope['qualified_pairs'] = qualified
    if not qualified:
        return envelope

    selected = qualified[0]
    value = str(selected['bet_value'])
    config_key = 'combo-best-pair-rate%s-sample%s-window%s-%s' % (
        '%g' % historical_rate_floor,
        historical_sample_floor,
        window_days,
        '-'.join(str(item) for item in pool_ids),
    )
    signal = {
        'bet_type': 'combo',
        'bet_value': value,
        'confidence': round(float(selected['historical_rate']) / 100.0, 4),
        'message_text': f'{value}10',
        'normalized_payload': {
            'primary_metric': 'combo',
            'selection_mode': 'best_consensus_pair',
            'consensus_count': 2,
            'predictor_ids': pool_ids,
            'supporter_predictor_ids': selected['predictor_ids'],
            'supporter_predictor_names': selected['predictor_names'],
            'historical_rate': selected['historical_rate'],
            'historical_sample': int(selected['historical_sample']),
            'historical_rate_threshold': historical_rate_floor,
            'historical_sample_threshold': historical_sample_floor,
            'historical_window_days': window_days,
            'profit_rule_id': 'pc28_high',
            'odds_profile': 'regular',
            'share_level': 'records',
        },
    }
    envelope['selected_pair'] = selected
    envelope['items'] = [{
        'schema_version': '1.0',
        'signal_id': f'pc28-best-consensus-pair-{config_key}-{issue_no}',
        'source_type': 'ai_trading_simulator',
        'source_ref': {
            'platform': 'AITradingSimulator',
            'algorithm_key': 'pc28_best_consensus_pair_v1',
            'selection_config': config_key,
            'predictor_ids': pool_ids,
            'supporter_predictor_ids': selected['predictor_ids'],
            'supporter_predictor_names': selected['predictor_names'],
            'historical_rate': selected['historical_rate'],
            'historical_sample': int(selected['historical_sample']),
        },
        'lottery_type': 'pc28',
        'issue_no': issue_no,
        'published_at': generated_at,
        'signals': [signal],
    }]
    return envelope
