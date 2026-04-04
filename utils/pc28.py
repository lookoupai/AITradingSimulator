"""
PC28 领域工具
"""
from __future__ import annotations

import re
from typing import Iterable, Optional


ALLOWED_TARGETS = ('number', 'big_small', 'odd_even', 'combo')
ALLOWED_INJECTION_MODES = ('summary', 'raw')
ALLOWED_API_MODES = ('auto', 'chat_completions', 'responses')
ALLOWED_PRIMARY_METRICS = ('combo', 'number', 'big_small', 'odd_even', 'double_group', 'kill_group')

TARGET_LABELS = {
    'number': '号码',
    'big_small': '大小',
    'odd_even': '单双',
    'combo': '组合',
    'double_group': '双组',
    'kill_group': '杀组'
}

DOUBLE_GROUP_LABELS = {
    '单组': {'大单', '小双'},
    '双组': {'大双', '小单'}
}

OPPOSITE_COMBO_MAP = {
    '大单': '小双',
    '小双': '大单',
    '大双': '小单',
    '小单': '大双'
}


def normalize_target_list(targets: Optional[Iterable[str]]) -> list[str]:
    """规范化预测目标列表"""
    if not targets:
        return list(ALLOWED_TARGETS)

    normalized: list[str] = []
    for target in targets:
        value = str(target or '').strip().lower()
        if value in ALLOWED_TARGETS and value not in normalized:
            normalized.append(value)

    return normalized or list(ALLOWED_TARGETS)


def normalize_injection_mode(value: Optional[str]) -> str:
    """规范化数据注入模式"""
    text = str(value or '').strip().lower()
    if text in ALLOWED_INJECTION_MODES:
        return text
    return 'summary'


def normalize_api_mode(value: Optional[str]) -> str:
    """规范化 API 模式"""
    text = str(value or '').strip().lower()
    if text in ALLOWED_API_MODES:
        return text
    return 'auto'


def normalize_primary_metric(value: Optional[str]) -> str:
    """规范化主玩法"""
    text = str(value or '').strip().lower()
    if text in ALLOWED_PRIMARY_METRICS:
        return text
    return 'big_small'


def parse_pc28_number(value) -> Optional[int]:
    """解析 PC28 号码（00-27）"""
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        number = int(value)
        return number if 0 <= number <= 27 else None

    text = str(value).strip()
    if not text:
        return None

    if '+' in text:
        parts = [part.strip() for part in text.split('+')]
        if parts and all(part.isdigit() for part in parts):
            number = sum(int(part) for part in parts)
            return number if 0 <= number <= 27 else None

    if text.isdigit():
        number = int(text)
        return number if 0 <= number <= 27 else None

    match = re.search(r'\d{1,2}', text)
    if match:
        number = int(match.group(0))
        return number if 0 <= number <= 27 else None

    return None


def normalize_big_small(value) -> Optional[str]:
    """规范化大小标签"""
    if value is None:
        return None

    text = str(value).strip().lower()
    if not text:
        return None

    mapping = {
        '大': '大',
        'big': '大',
        'large': '大',
        'b': '大',
        '小': '小',
        'small': '小',
        's': '小'
    }

    return mapping.get(text)


def normalize_odd_even(value) -> Optional[str]:
    """规范化单双标签"""
    if value is None:
        return None

    text = str(value).strip().lower()
    if not text:
        return None

    mapping = {
        '单': '单',
        'odd': '单',
        'o': '单',
        '双': '双',
        'even': '双',
        'e': '双'
    }

    return mapping.get(text)


def normalize_combo(value) -> Optional[str]:
    """规范化组合标签"""
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    normalized_text = (
        text.replace('-', '')
        .replace('_', '')
        .replace('/', '')
        .replace(' ', '')
        .lower()
    )

    mapping = {
        '大单': '大单',
        'bigodd': '大单',
        '大双': '大双',
        'bigeven': '大双',
        '小单': '小单',
        'smallodd': '小单',
        '小双': '小双',
        'smalleven': '小双'
    }

    if text in {'大单', '大双', '小单', '小双'}:
        return text

    return mapping.get(normalized_text)


def build_combo(big_small: Optional[str], odd_even: Optional[str]) -> Optional[str]:
    """由大小和单双拼装组合标签"""
    if not big_small or not odd_even:
        return None
    if big_small not in {'大', '小'} or odd_even not in {'单', '双'}:
        return None
    return f'{big_small}{odd_even}'


def derive_double_group(combo: Optional[str]) -> Optional[str]:
    """由组合派生双组/单组"""
    if not combo:
        return None
    for label, combos in DOUBLE_GROUP_LABELS.items():
        if combo in combos:
            return label
    return None


def derive_kill_group(combo: Optional[str]) -> Optional[str]:
    """由组合派生杀组，当前口径为杀对角组合"""
    if not combo:
        return None
    return OPPOSITE_COMBO_MAP.get(combo)


def derive_pc28_attributes(number: int) -> dict:
    """根据号码派生 PC28 标签"""
    number = int(number)
    big_small = '小' if number <= 13 else '大'
    odd_even = '双' if number % 2 == 0 else '单'
    combo = build_combo(big_small, odd_even)

    return {
        'result_number': number,
        'result_number_text': f'{number:02d}',
        'big_small': big_small,
        'odd_even': odd_even,
        'combo': combo
    }


def next_issue_no(issue_no: Optional[str]) -> Optional[str]:
    """计算下一期号，保留原始位数"""
    if not issue_no:
        return None

    text = str(issue_no).strip()
    if not text.isdigit():
        return None

    next_value = str(int(text) + 1)
    return next_value.zfill(len(text))


def mask_api_key(api_key: Optional[str]) -> str:
    """隐藏 API Key，仅保留前后少量字符"""
    if not api_key:
        return ''

    text = str(api_key)
    if len(text) <= 8:
        return '*' * len(text)

    return f'{text[:4]}{"*" * (len(text) - 8)}{text[-4:]}'


def parse_pc28_triplet(value) -> list[int]:
    """解析 PC28 原始三位数，例如 6+7+5"""
    if value is None:
        return []

    if isinstance(value, (list, tuple)):
        numbers = []
        for item in value:
            if isinstance(item, (int, float)):
                numbers.append(int(item))
        return numbers[:3]

    text = str(value).strip()
    if not text:
        return []

    if '+' in text:
        parts = [part.strip() for part in text.split('+')]
        if all(part.isdigit() for part in parts):
            return [int(part) for part in parts[:3]]

    digits = re.findall(r'\d+', text)
    if len(digits) >= 3:
        return [int(digits[0]), int(digits[1]), int(digits[2])]

    return []
