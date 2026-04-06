"""
竞彩足球领域工具
"""
from __future__ import annotations

import json
from typing import Iterable, Optional


ALLOWED_TARGETS = ('spf', 'rqspf')
ALLOWED_PRIMARY_METRICS = ('spf', 'rqspf')
DEFAULT_PROFIT_RULE_ID = 'jingcai_snapshot'
TARGET_LABELS = {
    'spf': '胜平负',
    'rqspf': '让球胜平负',
    'spf_parlay': '胜平负二串一',
    'rqspf_parlay': '让球胜平负二串一'
}
RESULT_LABELS = ('胜', '平', '负')


def normalize_target_list(targets: Optional[Iterable[str]]) -> list[str]:
    """规范化竞彩足球预测目标"""
    if not targets:
        return list(ALLOWED_TARGETS)

    normalized: list[str] = []
    for target in targets:
        value = str(target or '').strip().lower()
        if value in ALLOWED_TARGETS and value not in normalized:
            normalized.append(value)

    return normalized or list(ALLOWED_TARGETS)


def normalize_primary_metric(value: Optional[str]) -> str:
    """规范化竞彩足球主玩法"""
    text = str(value or '').strip().lower()
    if text in ALLOWED_PRIMARY_METRICS:
        return text
    return 'spf'


def normalize_profit_metric(value: Optional[str]) -> str:
    """竞彩足球收益模拟默认只接受单场主玩法"""
    return normalize_primary_metric(value)


def normalize_profit_rule(value: Optional[str]) -> str:
    """竞彩足球当前只支持预测批次赔率快照规则"""
    text = str(value or '').strip().lower()
    if text == DEFAULT_PROFIT_RULE_ID:
        return text
    return DEFAULT_PROFIT_RULE_ID


def normalize_prediction_outcome(value) -> Optional[str]:
    """把胜平负结果统一成 胜/平/负"""
    if value is None:
        return None

    text = str(value).strip().lower()
    if not text:
        return None

    mapping = {
        '3': '胜',
        '1': '平',
        '0': '负',
        '胜': '胜',
        '主胜': '胜',
        'win': '胜',
        'home': '胜',
        '平': '平',
        '平局': '平',
        'draw': '平',
        '负': '负',
        '客胜': '负',
        'lose': '负',
        'loss': '负',
        'away': '负'
    }
    return mapping.get(text)


def parse_int(value) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def parse_float(value) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def parse_spf_odds(value) -> dict[str, Optional[float]]:
    """解析胜平负赔率字符串"""
    parts = [parse_float(item) for item in str(value or '').split(',')]
    while len(parts) < 3:
        parts.append(None)
    return {
        '胜': parts[0],
        '平': parts[1],
        '负': parts[2]
    }


def parse_rqspf_odds(value) -> dict[str, object]:
    """解析让球胜平负赔率字符串"""
    parts = [str(item).strip() for item in str(value or '').split(',')]
    while len(parts) < 4:
        parts.append('')

    handicap_text = parts[0]
    handicap = parse_int(handicap_text)
    return {
        'handicap': handicap,
        'handicap_text': handicap_text or '',
        'odds': {
            '胜': parse_float(parts[1]),
            '平': parse_float(parts[2]),
            '负': parse_float(parts[3])
        }
    }


def resolve_event_key(item: dict) -> str:
    """优先使用体彩比赛 ID，回退到新浪 matchId"""
    return str(item.get('tiCaiId') or item.get('matchId') or '').strip()


def build_match_name(league: str, team1: str, team2: str) -> str:
    league_text = str(league or '').strip()
    home_text = str(team1 or '').strip() or '主队'
    away_text = str(team2 or '').strip() or '客队'
    prefix = f'[{league_text}] ' if league_text else ''
    return f'{prefix}{home_text} vs {away_text}'


def derive_spf_result(score1, score2) -> Optional[str]:
    """根据比分推导胜平负"""
    home_score = parse_int(score1)
    away_score = parse_int(score2)
    if home_score is None or away_score is None:
        return None
    if home_score > away_score:
        return '胜'
    if home_score == away_score:
        return '平'
    return '负'


def derive_rqspf_result(score1, score2, handicap) -> Optional[str]:
    """根据比分和让球推导让球胜平负"""
    home_score = parse_int(score1)
    away_score = parse_int(score2)
    handicap_value = parse_int(handicap)
    if home_score is None or away_score is None or handicap_value is None:
        return None

    adjusted_home = home_score + handicap_value
    if adjusted_home > away_score:
        return '胜'
    if adjusted_home == away_score:
        return '平'
    return '负'


def is_match_settled(item: dict) -> bool:
    score1 = parse_int(item.get('score1'))
    score2 = parse_int(item.get('score2'))
    return score1 is not None and score2 is not None


def clamp_confidence(value) -> Optional[float]:
    number = parse_float(value)
    if number is None:
        return None
    return max(0.0, min(number, 1.0))


def rank_prediction_items(items: Iterable[dict], metric_key: Optional[str] = None) -> list[dict]:
    """按置信度和编号对预测项排序，可指定必须命中某个玩法字段"""
    candidates: list[dict] = []
    for item in items:
        payload = item.get('prediction_payload') or {}
        if item.get('status') not in {'pending', 'settled'}:
            continue
        if metric_key:
            if not payload.get(metric_key):
                continue
        elif not payload:
            continue
        candidates.append(item)

    return sorted(
        candidates,
        key=lambda item: (
            clamp_confidence(item.get('confidence')) if item.get('confidence') is not None else -1,
            item.get('issue_no') or ''
        ),
        reverse=True
    )


def resolve_snapshot_odds(meta_payload: dict, metric_key: str, outcome: Optional[str]) -> Optional[float]:
    """从赛事快照里提取指定玩法、指定结果的赔率"""
    if not outcome:
        return None

    if metric_key == 'spf':
        return parse_float((meta_payload.get('spf_odds') or {}).get(outcome))

    if metric_key == 'rqspf':
        rqspf = meta_payload.get('rqspf') or {}
        return parse_float((rqspf.get('odds') or {}).get(outcome))

    return None


def dump_json(value) -> str:
    return json.dumps(value, ensure_ascii=False)
