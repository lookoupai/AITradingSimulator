"""
预测方案执行引擎目录与归一化工具
"""
from __future__ import annotations

from lotteries.registry import normalize_lottery_type


ALLOWED_ENGINE_TYPES = ('ai', 'machine')
DEFAULT_ENGINE_TYPE = 'ai'
ENGINE_TYPE_LABELS = {
    'ai': 'AI 模型',
    'machine': '机器算法'
}
MACHINE_ALGORITHM_CATALOG = {
    'pc28': (
        {
            'key': 'pc28_frequency_v1',
            'label': '频次趋势 V1',
            'description': '基于最近开奖的加权频次、遗漏和组合偏好做 deterministic 预测。'
        },
        {
            'key': 'pc28_omission_reversion_v1',
            'label': '遗漏回补 V1',
            'description': '更重视和值遗漏、冷热切换和组合回补，适合偏反转风格。'
        },
        {
            'key': 'pc28_combo_markov_v1',
            'label': '组合马尔可夫 V1',
            'description': '按大单/大双/小单/小双的历史转移关系预测下一组合，再反推和值。'
        },
    ),
    'jingcai_football': (
        {
            'key': 'football_odds_baseline_v1',
            'label': '赔率基线 V1',
            'description': '按市场赔率隐含概率输出胜平负与让球胜平负，不依赖 LLM。'
        },
        {
            'key': 'football_odds_form_weighted_v1',
            'label': '赔率+状态加权 V1',
            'description': '在赔率基线上叠加近期战绩、积分排名、伤停和欧赔变动做稳健修正。'
        },
        {
            'key': 'football_handicap_consistency_v1',
            'label': '让球一致性 V1',
            'description': '更重视让球方向、SPF 与 RQSPF 一致性，以及欧赔/亚盘共振。'
        },
        {
            'key': 'football_value_edge_v1',
            'label': '价值优势 V1',
            'description': '按模型概率相对赔率隐含概率的 edge/EV 筛选，过滤低赔率热门。'
        },
    )
}
DEFAULT_MACHINE_ALGORITHMS = {
    'pc28': 'pc28_frequency_v1',
    'jingcai_football': 'football_odds_baseline_v1'
}
USER_ALGORITHM_PREFIX = 'user:'
BUILTIN_ALGORITHM_PREFIX = 'builtin:'


def normalize_engine_type(value) -> str:
    text = str(value or '').strip().lower()
    if text in ALLOWED_ENGINE_TYPES:
        return text
    return DEFAULT_ENGINE_TYPE


def get_engine_type_label(value) -> str:
    normalized = normalize_engine_type(value)
    return ENGINE_TYPE_LABELS.get(normalized, ENGINE_TYPE_LABELS[DEFAULT_ENGINE_TYPE])


def list_machine_algorithms(lottery_type: str) -> list[dict]:
    normalized_lottery_type = normalize_lottery_type(lottery_type)
    return [dict(item) for item in MACHINE_ALGORITHM_CATALOG.get(normalized_lottery_type, ())]


def get_default_machine_algorithm(lottery_type: str) -> str:
    normalized_lottery_type = normalize_lottery_type(lottery_type)
    return DEFAULT_MACHINE_ALGORITHMS.get(normalized_lottery_type, '')


def is_user_algorithm_key(value) -> bool:
    text = str(value or '').strip()
    if not text.startswith(USER_ALGORITHM_PREFIX):
        return False
    return text[len(USER_ALGORITHM_PREFIX):].isdigit()


def get_user_algorithm_id(value) -> int | None:
    if not is_user_algorithm_key(value):
        return None
    return int(str(value).strip()[len(USER_ALGORITHM_PREFIX):])


def normalize_builtin_algorithm_key(value) -> str:
    text = str(value or '').strip()
    if text.startswith(BUILTIN_ALGORITHM_PREFIX):
        return text[len(BUILTIN_ALGORITHM_PREFIX):].strip()
    return text


def normalize_algorithm_key(lottery_type: str, engine_type: str, value) -> str:
    normalized_engine_type = normalize_engine_type(engine_type)
    if normalized_engine_type != 'machine':
        return ''

    options = list_machine_algorithms(lottery_type)
    allowed_keys = {item['key'] for item in options}
    text = str(value or '').strip()
    if is_user_algorithm_key(text):
        return text
    text = normalize_builtin_algorithm_key(text)
    if text in allowed_keys:
        return text
    return get_default_machine_algorithm(lottery_type)


def get_algorithm_label(lottery_type: str, engine_type: str, algorithm_key) -> str:
    normalized_engine_type = normalize_engine_type(engine_type)
    if normalized_engine_type != 'machine':
        return ''

    normalized_key = normalize_algorithm_key(lottery_type, normalized_engine_type, algorithm_key)
    if is_user_algorithm_key(normalized_key):
        return f'用户算法 #{get_user_algorithm_id(normalized_key)}'
    for item in list_machine_algorithms(lottery_type):
        if item['key'] == normalized_key:
            return item['label']
    return normalized_key or '内置算法'


def get_algorithm_description(lottery_type: str, engine_type: str, algorithm_key) -> str:
    normalized_engine_type = normalize_engine_type(engine_type)
    if normalized_engine_type != 'machine':
        return '按模型与提示词动态生成预测结果。'

    normalized_key = normalize_algorithm_key(lottery_type, normalized_engine_type, algorithm_key)
    if is_user_algorithm_key(normalized_key):
        return '使用当前用户自定义算法定义执行预测。'
    for item in list_machine_algorithms(lottery_type):
        if item['key'] == normalized_key:
            return str(item.get('description') or '').strip()
    return '使用平台内置机器算法执行预测。'


def uses_ai_engine(predictor: dict | None) -> bool:
    return normalize_engine_type((predictor or {}).get('engine_type')) == 'ai'


def resolve_execution_label(predictor: dict | None) -> str:
    predictor = predictor or {}
    lottery_type = normalize_lottery_type(predictor.get('lottery_type'))
    engine_type = normalize_engine_type(predictor.get('engine_type'))
    if engine_type == 'machine':
        return (
            str(predictor.get('algorithm_label') or '').strip()
            or get_algorithm_label(lottery_type, engine_type, predictor.get('algorithm_key'))
            or '内置机器算法'
        )
    return str(predictor.get('model_name') or '').strip() or '--'


def resolve_execution_description(predictor: dict | None) -> str:
    predictor = predictor or {}
    lottery_type = normalize_lottery_type(predictor.get('lottery_type'))
    engine_type = normalize_engine_type(predictor.get('engine_type'))
    if engine_type == 'machine':
        return get_algorithm_description(lottery_type, engine_type, predictor.get('algorithm_key'))

    method_name = str(predictor.get('prediction_method') or '').strip()
    if method_name:
        return f'按当前模型与提示词执行，策略说明为“{method_name}”。'
    return '按当前模型与提示词动态生成预测结果。'
