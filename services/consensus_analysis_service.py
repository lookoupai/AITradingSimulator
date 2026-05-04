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


# 历史样本上限（避免大表全量扫描）
HISTORICAL_QUERY_LIMIT = 20000


def build_consensus_analysis(
    db,
    *,
    user_id: int | None,
    lottery_type: str = 'jingcai_football',
    time_window_days: int | None = 30
) -> dict:
    """
    构建一份共识分析快照。

    参数：
        db: Database 实例
        user_id: 限定方案池为该用户；None 表示分析平台所有方案（仅管理员调用）
        lottery_type: 当前仅有 jingcai_football 实际可用
        time_window_days: 历史窗口（天），None 表示全部历史

    返回：见 plan 文件。所有率以百分比 float 形式给出，未达成时为 None。
    """
    normalized = normalize_lottery_type(lottery_type)
    definition = get_lottery_definition(normalized)
    fields = [
        {'key': key, 'label': label}
        for key, label in definition.target_options
    ]

    # 1. 方案池
    predictors_pool = _select_predictor_pool(db, user_id=user_id, lottery_type=normalized)
    predictor_ids = [p['id'] for p in predictors_pool]

    if not predictor_ids:
        return _empty_analysis(normalized, fields, time_window_days)

    # 2. 拉取已结算的历史预测项（用于历史规律）
    settled_items = _fetch_prediction_items(
        db,
        predictor_ids=predictor_ids,
        lottery_type=normalized,
        only_settled=True,
        time_window_days=time_window_days
    )

    # 3. 拉取最近 / 未结算的预测项（用于今日推荐）
    pending_items = _fetch_prediction_items(
        db,
        predictor_ids=predictor_ids,
        lottery_type=normalized,
        only_settled=False,
        only_pending=True,
        time_window_days=None  # 未结算的不卡时间窗
    )

    # 4. 各方案自身命中率
    per_predictor = _build_per_predictor_stats(settled_items, predictors_pool, fields)

    # 4a. 从 jingcai_prediction_daily_summary 补充已归档的历史命中率（仅单方案级，
    #     因为 daily_summary 是日聚合，无法重建明细级共识/组合指标）
    archive_per_predictor = _load_archived_per_predictor(
        db,
        predictor_ids=predictor_ids,
        time_window_days=time_window_days,
        fields=fields
    )
    per_predictor = _merge_per_predictor_with_archive(per_predictor, archive_per_predictor, fields)

    # 5. 按比赛重新组织：{(run_key, event_key): [item, item, ...]}
    matches = _group_by_match(settled_items)

    # 6. 共识数 vs 命中率
    consensus_by_count = _build_consensus_by_count(matches, fields)

    # 7. 两两方案一致时的命中率
    pair_combinations = _build_pair_combinations(matches, predictor_ids, fields)

    # 8. 今日推荐（基于 pending_items + 历史规律）
    today_recommendations = _build_today_recommendations(
        pending_items=pending_items,
        consensus_by_count=consensus_by_count,
        pair_combinations=pair_combinations,
        predictors_pool=predictors_pool,
        fields=fields
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
        'consensus_by_count': consensus_by_count,
        'pair_combinations': pair_combinations,
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
        'consensus_by_count': {f['key']: [] for f in fields},
        'pair_combinations': {f['key']: [] for f in fields},
        'today_recommendations': []
    }


def _select_predictor_pool(db, *, user_id: int | None, lottery_type: str) -> list[dict]:
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

    return [
        p for p in (all_predictors or [])
        if (p.get('lottery_type') or '') == lottery_type
        and bool(p.get('enabled'))
    ]


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
            sql += " AND status != 'settled'"

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


def _group_by_match(items: list[dict]) -> dict[tuple, list[dict]]:
    grouped: dict[tuple, list[dict]] = defaultdict(list)
    for item in items:
        key = (item.get('run_key') or '', item.get('event_key') or '')
        grouped[key].append(item)
    return grouped


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


def _build_consensus_by_count(
    matches: dict[tuple, list[dict]],
    fields: list[dict]
) -> dict[str, list[dict]]:
    """
    对每场比赛：
      - 统计每个字段下，每个预测值被多少方案支持
      - 对该值的所有支持方案的命中数累计 (n_agree, value) 桶
    输出按字段分组的列表。
    """
    output: dict[str, list[dict]] = {}
    for field in fields:
        fkey = field['key']
        bucket: dict[tuple, dict] = defaultdict(lambda: {'total': 0, 'hit': 0})
        for items in matches.values():
            value_supporters: dict[str, list[dict]] = defaultdict(list)
            for item in items:
                pred_val = (item.get('prediction') or {}).get(fkey)
                hit_val = (item.get('hit') or {}).get(fkey)
                if pred_val in (None, '', 'null') or hit_val is None:
                    continue
                value_supporters[pred_val].append(item)
            for pred_val, supporters in value_supporters.items():
                n_agree = len(supporters)
                if n_agree < 1:
                    continue
                for sup in supporters:
                    hit_val = (sup.get('hit') or {}).get(fkey)
                    bucket[(n_agree, pred_val)]['total'] += 1
                    bucket[(n_agree, pred_val)]['hit'] += int(bool(hit_val))

        rows = []
        for (n_agree, pred_val), stat in bucket.items():
            rows.append({
                'agree_count': n_agree,
                'value': pred_val,
                'total': stat['total'],
                'hit': stat['hit'],
                'rate': _safe_rate(stat['hit'], stat['total'])
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
            for p1, p2 in combinations(predictor_ids, 2):
                if p1 not in preds_by_pid or p2 not in preds_by_pid:
                    continue
                v1 = preds_by_pid[p1]['prediction'][fkey]
                v2 = preds_by_pid[p2]['prediction'][fkey]
                if v1 != v2:
                    continue
                # 两个方案预测一致：计入两条命中样本
                for pid in (p1, p2):
                    h = preds_by_pid[pid]['hit'].get(fkey)
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


def _build_today_recommendations(
    *,
    pending_items: list[dict],
    consensus_by_count: dict[str, list[dict]],
    pair_combinations: dict[str, list[dict]],
    predictors_pool: list[dict],
    fields: list[dict]
) -> list[dict]:
    """
    对每场未结算比赛：
      - 找出每个字段的共识值（票数最多的预测值）
      - 关联历史"共识=N且值=V"时的命中率作为参考
      - 同时给出本场支持该共识的方案列表
    """
    if not pending_items:
        return []
    name_lookup = {p['id']: p.get('name') or f"方案#{p['id']}" for p in predictors_pool}
    matches = _group_by_match(pending_items)

    # 把 consensus_by_count 转成 dict 便于查询
    rate_lookup = {
        fkey: {(row['agree_count'], row['value']): row for row in rows}
        for fkey, rows in consensus_by_count.items()
    }

    recommendations = []
    for (run_key, event_key), items in matches.items():
        # 取该场任意一项的标题
        title = next((it.get('title') or '' for it in items), '')
        per_field_rec = []
        for field in fields:
            fkey = field['key']
            value_supporters: dict[str, list[int]] = defaultdict(list)
            for item in items:
                pred_val = (item.get('prediction') or {}).get(fkey)
                if pred_val in (None, '', 'null'):
                    continue
                value_supporters[pred_val].append(item['predictor_id'])
            if not value_supporters:
                continue
            # 共识值 = 票数最多的，若并列取首个
            consensus_value, supporters = max(
                value_supporters.items(),
                key=lambda kv: (len(kv[1]), kv[0])
            )
            agree_count = len(supporters)
            historical = rate_lookup.get(fkey, {}).get((agree_count, consensus_value))
            per_field_rec.append({
                'field': fkey,
                'field_label': field['label'],
                'consensus_value': consensus_value,
                'agree_count': agree_count,
                'supporters': supporters,
                'supporter_names': [name_lookup.get(pid, str(pid)) for pid in supporters],
                'all_predictions': {
                    val: {
                        'count': len(pids),
                        'predictors': pids
                    }
                    for val, pids in value_supporters.items()
                },
                'historical_rate': historical.get('rate') if historical else None,
                'historical_sample': historical.get('total') if historical else 0
            })
        if per_field_rec:
            recommendations.append({
                'run_key': run_key,
                'event_key': event_key,
                'title': title,
                'fields': per_field_rec
            })
    # 按 "最高字段共识数 * 历史命中率" 简单打分排序
    def score(rec):
        best = 0.0
        for f in rec['fields']:
            rate = f.get('historical_rate') or 0
            best = max(best, f.get('agree_count', 0) * rate)
        return best
    recommendations.sort(key=score, reverse=True)
    return recommendations


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
