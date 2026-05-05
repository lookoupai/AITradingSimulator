"""
个性化共识规则服务

三大职责：
1. 让 AI 基于"该用户当前的方案池 + 共识统计数据"生成专属决策规则
2. 评分今日比赛：哪些场命中了哪些规则
3. 检测方案池漂移：规则生成时与现在的方案池差异有多大

设计原则：
- AI 只在"生成规则"时介入；评分和漂移检测都是纯后端逻辑
- 规则的 condition_match 是结构化条件，识别几种 type 即可自动评分；
  其它 type 时前端只展示文字说明，跳过自动评分（容错）
"""
from __future__ import annotations

import json
from typing import Any

from ai_trader import AIPredictor
from services.consensus_analysis_service import (
    _compute_predictor_weights,
    RANDOM_BASELINE_RATE,
    MIN_SAMPLE_FOR_WEIGHT
)


# AI 提示词
SYSTEM_PROMPT = (
    '你是 AITradingSimulator 平台的竞彩足球方案规则生成助手。'
    '基于用户当前方案池和历史共识统计数据，生成 3-6 条可操作的决策规则。'
    '禁止编造未给出的数据。禁止输出 Markdown / 代码块 / JSON 之外的字符。'
    '你必须只输出一个 JSON 对象，schema 严格如下：\n'
    '{\n'
    '  "summary": "整体性描述（一段中文文字，<=200字）",\n'
    '  "rules": [\n'
    '    {\n'
    '      "id": "rule_<n>",\n'
    '      "title": "短标题（<=15字）",\n'
    '      "field": "spf 或 rqspf",\n'
    '      "condition_natural": "自然语言条件描述（前端展示用）",\n'
    '      "condition_match": { "type": ..., 其它字段 },\n'
    '      "action": "建议动作描述",\n'
    '      "confidence": "high | medium | low",\n'
    '      "rationale": "为什么这条规则成立（引用真实数字）"\n'
    '    }\n'
    '  ]\n'
    '}\n'
    'condition_match.type 必须是以下之一（用平台能自动评分的）：\n'
    '- "pair_agree": {"type":"pair_agree","predictors":[id1,id2],"field":"rqspf","value":"胜"}  '
    '  含义：这两个 predictor 在该字段上预测同值时触发；如果 value 给定则要求 = 该值，未给则任意一致都触发\n'
    '- "n_agree": {"type":"n_agree","n":3,"field":"rqspf","value":"胜"}  '
    '  含义：当 N 个方案在该字段上预测同一值时触发；value 可省略\n'
    '- "all_agree": {"type":"all_agree","field":"spf","value":"胜"}  '
    '  含义：方案池全部方案预测同值时触发\n'
    '- "majority_agree": {"type":"majority_agree","threshold":0.6,"field":"rqspf"}  '
    '  含义：超过 threshold 比例方案预测同一值时触发\n'
    '- "exclude_predictor": {"type":"exclude_predictor","predictor":13}  '
    '  含义：仅作为"应忽略此方案"的提示，不参与今日评分\n'
    '生成规则时务必结合用户的真实方案 ID 和命中率数据，不要编造方案。'
)

KNOWN_CONDITION_TYPES = {
    'pair_agree',
    'n_agree',
    'all_agree',
    'majority_agree',
    'exclude_predictor'
}

# 样本量阈值：低于此阈值时拒绝调 AI 生成
MIN_SAMPLE_COUNT = 30


def generate_consensus_rules(
    *,
    api_key: str,
    api_url: str,
    model_name: str,
    api_mode: str,
    consensus_summary: dict,
    user_message: str = '',
    temperature: float = 0.3
) -> dict:
    """
    调用 AI 生成规则。返回 {summary, rules, raw_response, response_model, latency_ms, ...}
    """
    if not consensus_summary or not consensus_summary.get('predictors'):
        raise ValueError('共识统计数据为空，无法生成规则')

    sample_count = int(consensus_summary.get('settled_item_count') or 0)
    if sample_count < MIN_SAMPLE_COUNT:
        raise ValueError(f'样本量不足（{sample_count} 条），至少需要 {MIN_SAMPLE_COUNT} 条已结算预测才能生成稳定的规则')

    prompt = _build_prompt(consensus_summary=consensus_summary, user_message=user_message)
    client = AIPredictor(
        api_key=api_key,
        api_url=api_url,
        model_name=model_name,
        api_mode=api_mode,
        temperature=temperature
    )
    result = client.run_json_task(
        prompt=prompt,
        system_prompt=SYSTEM_PROMPT,
        max_output_tokens=2400
    )
    payload = result.get('payload') or {}
    if not isinstance(payload, dict):
        raise ValueError('AI 返回的不是 JSON 对象')

    raw_rules = payload.get('rules')
    if not isinstance(raw_rules, list) or not raw_rules:
        raise ValueError('AI 没有生成任何规则')

    # 标准化规则
    cleaned_rules = []
    for idx, item in enumerate(raw_rules):
        if not isinstance(item, dict):
            continue
        cleaned = _normalize_rule(item, default_id=f'rule_{idx + 1}')
        if cleaned:
            cleaned_rules.append(cleaned)

    if not cleaned_rules:
        raise ValueError('AI 生成的规则全部无效')

    return {
        'summary': str(payload.get('summary') or '').strip(),
        'rules': cleaned_rules,
        'prompt': prompt,
        'response_model': result.get('response_model'),
        'api_mode': result.get('api_mode'),
        'finish_reason': result.get('finish_reason'),
        'latency_ms': result.get('latency_ms'),
        'raw_response': (result.get('raw_response') or '')[:2000]
    }


def score_today_against_rules(
    *,
    rules: list[dict],
    today_recommendations: list[dict],
    today_matches_detail: list[dict] | None = None
) -> list[dict]:
    """
    返回每场比赛的命中规则列表：
    [
      {
        "event_key": ...,
        "title": ...,
        "matched_rules": [
          {"rule_id":..., "rule_title":..., "field":..., "action":..., "confidence":..., "rationale":...}
        ]
      },
      ...
    ]
    只包含至少命中一条规则的比赛。
    """
    if not rules or not today_recommendations:
        return []

    # 把 today_matches_detail 索引到 (event_key) 方便查 predictor_id -> prediction value
    detail_by_event: dict[str, list[dict]] = {}
    for match in (today_matches_detail or []):
        ev = match.get('event_key') or ''
        if ev:
            detail_by_event[ev] = match.get('predictions') or []

    output = []
    for rec in today_recommendations:
        event_key = rec.get('event_key') or ''
        title = rec.get('title') or ''
        # 把每个 field 的预测分布做成查询表
        field_distribution = {f.get('field'): f for f in (rec.get('fields') or [])}
        # 也把 detail 转成 {predictor_id: {field: pred_value}}
        pred_lookup: dict[int, dict] = {}
        for pred in detail_by_event.get(event_key, []):
            pid = pred.get('predictor_id')
            if pid is not None:
                pred_lookup[int(pid)] = pred.get('prediction') or {}

        matched = []
        for rule in rules:
            cm = rule.get('condition_match') or {}
            ctype = cm.get('type')
            if ctype not in KNOWN_CONDITION_TYPES:
                continue
            triggered, hit_value = _check_rule_triggered(
                cm, ctype, field_distribution, pred_lookup
            )
            if triggered:
                matched.append({
                    'rule_id': rule.get('id'),
                    'rule_title': rule.get('title'),
                    'field': rule.get('field'),
                    'action': rule.get('action'),
                    'confidence': rule.get('confidence'),
                    'rationale': rule.get('rationale'),
                    'consensus_value': hit_value
                })
        if matched:
            output.append({
                'event_key': event_key,
                'title': title,
                'matched_rules': matched
            })
    return output


def detect_pool_drift(
    *,
    snapshot: list[dict],
    current_pool: list[dict]
) -> dict:
    """
    检测规则生成时方案池与当前方案池的差异。
    severity 阈值：
      - none: 完全一致
      - minor: drift_ratio <= 0.3
      - major: drift_ratio > 0.3
    """
    snap_ids = {int(p.get('id')) for p in (snapshot or []) if p.get('id') is not None}
    snap_names = {int(p.get('id')): p.get('name') or str(p.get('id'))
                  for p in (snapshot or []) if p.get('id') is not None}
    cur_ids = {int(p.get('id')) for p in (current_pool or []) if p.get('id') is not None}
    cur_names = {int(p.get('id')): p.get('name') or str(p.get('id'))
                 for p in (current_pool or []) if p.get('id') is not None}

    added = sorted(cur_ids - snap_ids)
    removed = sorted(snap_ids - cur_ids)

    if not snap_ids and not cur_ids:
        return {
            'is_drifted': False,
            'added': [], 'removed': [],
            'drift_ratio': 0.0, 'severity': 'none'
        }

    union = snap_ids | cur_ids
    drift_ratio = (len(added) + len(removed)) / max(len(union), 1)
    if drift_ratio <= 0.0:
        severity = 'none'
    elif drift_ratio <= 0.3:
        severity = 'minor'
    else:
        severity = 'major'

    return {
        'is_drifted': bool(added or removed),
        'added': [{'id': pid, 'name': cur_names.get(pid, str(pid))} for pid in added],
        'removed': [{'id': pid, 'name': snap_names.get(pid, str(pid))} for pid in removed],
        'drift_ratio': round(drift_ratio, 3),
        'severity': severity
    }


# ----------------------- 内部辅助 -----------------------

def _build_prompt(*, consensus_summary: dict, user_message: str) -> str:
    pool_block = json.dumps(
        consensus_summary.get('predictors') or [],
        ensure_ascii=False, indent=2
    )
    per_predictor = consensus_summary.get('per_predictor') or []
    consensus_by_count = consensus_summary.get('consensus_by_count') or {}
    pair_combinations = consensus_summary.get('pair_combinations') or {}
    fields = consensus_summary.get('fields') or []

    # 计算方案质量分级（复用 consensus_analysis_service 的权重函数）
    quality_block = _build_quality_signals_block(per_predictor, fields)

    stats_block = json.dumps({
        'window_days': consensus_summary.get('window_days'),
        'sample_count': consensus_summary.get('sample_count'),
        'settled_item_count': consensus_summary.get('settled_item_count'),
        'archive_used': consensus_summary.get('archive_used'),
        'per_predictor': per_predictor,
        'consensus_by_count': consensus_by_count,
        'pair_combinations': pair_combinations
    }, ensure_ascii=False, indent=2)

    extra = (user_message or '').strip()
    extra_section = f'\n\n用户额外要求：\n{extra}\n' if extra else ''

    return f"""请为下面这个用户的方案池生成 3-6 条专属决策规则。

=== 当前方案池 ===
{pool_block}

=== 方案质量分级（基于历史命中率减随机基准 33.33%） ===
{quality_block}

=== 共识分析数据 ===
{stats_block}
{extra_section}
要求：
- 引用真实数字（pair_combinations / consensus_by_count 中的 rate/total/hit）。
- 规则中的 predictor id 必须来自当前方案池，不要编造。
- 至少给出一条 confidence: "high" 的规则；如有反向规律（例如全员一致反而错），也用一条规则提示。
- **优先考虑"强方案"组合（quality=high），警惕含"反指标"方案（quality=anti）的共识** — 反指标方案历史命中率低于随机，参与共识反而是危险信号。
- summary 用一句中文概括最重要的 1-2 个发现。
- 严格输出 JSON：{{"summary":"...","rules":[...]}}
"""


def _build_quality_signals_block(per_predictor: list[dict], fields: list[dict]) -> str:
    """
    把每个方案在每个字段上的"质量分级"渲染成 prompt 块。

    quality 标签：
      - "high"   : 权重 > 0.10 （命中率超过随机基准 10 个百分点以上）
      - "neutral": -0.05 <= 权重 <= 0.10
      - "anti"   : 权重 < -0.05 （反指标，命中率明显低于随机）
      - "n/a"    : 样本不足或缺失（视为中性，不参与判断）
    """
    if not per_predictor or not fields:
        return '（无方案数据）'

    weights = _compute_predictor_weights(per_predictor, fields)

    lines: list[dict] = []
    for entry in per_predictor:
        pid = int(entry.get('predictor_id') or 0)
        name = entry.get('predictor_name') or f'方案#{pid}'
        for field in fields:
            fkey = field['key']
            metric = (entry.get('metrics') or {}).get(fkey) or {}
            rate = metric.get('rate')
            total = int(metric.get('total') or 0)
            w = (weights.get(pid) or {}).get(fkey, 0.0)
            if rate is None or total < MIN_SAMPLE_FOR_WEIGHT:
                quality = 'n/a'
            elif w > 0.10:
                quality = 'high'
            elif w < -0.05:
                quality = 'anti'
            else:
                quality = 'neutral'
            lines.append({
                'predictor_id': pid,
                'predictor_name': name,
                'field': fkey,
                'field_label': field['label'],
                'rate_pct': rate,
                'sample': total,
                'weight': w,
                'quality': quality
            })

    return json.dumps(lines, ensure_ascii=False, indent=2)


def _normalize_rule(item: dict, *, default_id: str) -> dict | None:
    """清洗 / 校验单条规则。返回标准化后的 dict 或 None（无效）。"""
    rule_id = str(item.get('id') or default_id).strip() or default_id
    title = str(item.get('title') or '').strip()
    field = str(item.get('field') or '').strip()
    if not title or not field:
        return None

    cm_raw = item.get('condition_match')
    cm = cm_raw if isinstance(cm_raw, dict) else {}
    ctype = str(cm.get('type') or '').strip()
    # 不识别的 type 也保留（前端展示），但 field 必须有

    return {
        'id': rule_id,
        'title': title,
        'field': field,
        'condition_natural': str(item.get('condition_natural') or '').strip(),
        'condition_match': cm,
        'action': str(item.get('action') or '').strip() or '参考',
        'confidence': _normalize_confidence(item.get('confidence')),
        'rationale': str(item.get('rationale') or '').strip(),
        'auto_scorable': ctype in KNOWN_CONDITION_TYPES and ctype != 'exclude_predictor'
    }


def _normalize_confidence(value) -> str:
    text = str(value or '').strip().lower()
    if text in {'high', 'h', '高'}:
        return 'high'
    if text in {'low', 'l', '低'}:
        return 'low'
    return 'medium'


def _check_rule_triggered(
    cm: dict,
    ctype: str,
    field_distribution: dict[str, dict],
    pred_lookup: dict[int, dict]
) -> tuple[bool, str | None]:
    """
    判断当前比赛是否触发该规则。
    返回 (是否触发, 命中的预测值)。
    """
    if ctype == 'exclude_predictor':
        # 这种规则本身不参与评分（它是建议而非触发器）
        return False, None

    field_key = cm.get('field') or ''
    if not field_key:
        return False, None
    field_info = field_distribution.get(field_key)
    if not field_info:
        return False, None

    target_value = cm.get('value')  # 可选

    if ctype == 'all_agree':
        # 当前比赛该字段所有预测都一致
        all_preds = field_info.get('all_predictions') or {}
        if len(all_preds) == 1:
            sole_value = next(iter(all_preds.keys()))
            if target_value is None or sole_value == target_value:
                return True, sole_value
        return False, None

    if ctype == 'majority_agree':
        threshold = float(cm.get('threshold') or 0.6)
        all_preds = field_info.get('all_predictions') or {}
        total = sum(int(v.get('count') or 0) for v in all_preds.values())
        if total == 0:
            return False, None
        for value, info in all_preds.items():
            ratio = int(info.get('count') or 0) / total
            if ratio >= threshold and (target_value is None or value == target_value):
                return True, value
        return False, None

    if ctype == 'n_agree':
        n = int(cm.get('n') or 0)
        if n <= 0:
            return False, None
        all_preds = field_info.get('all_predictions') or {}
        for value, info in all_preds.items():
            count = int(info.get('count') or 0)
            if count >= n and (target_value is None or value == target_value):
                return True, value
        return False, None

    if ctype == 'pair_agree':
        predictors = cm.get('predictors') or []
        if len(predictors) < 2:
            return False, None
        try:
            pid_list = [int(p) for p in predictors]
        except (TypeError, ValueError):
            return False, None
        values = []
        for pid in pid_list:
            pred = pred_lookup.get(pid) or {}
            v = pred.get(field_key)
            if v in (None, '', 'null'):
                return False, None
            values.append(v)
        if len(set(values)) != 1:
            return False, None
        v = values[0]
        if target_value is None or v == target_value:
            return True, v
        return False, None

    return False, None
