"""
彩种能力目录
"""
from __future__ import annotations

from dataclasses import dataclass, field

from utils import jingcai_football as football_utils
from utils import pc28 as pc28_utils


@dataclass(frozen=True)
class LotteryDefinition:
    lottery_type: str
    label: str
    target_options: tuple[tuple[str, str], ...]
    primary_metric_options: tuple[tuple[str, str], ...]
    supports_profit_simulation: bool
    supports_public_pages: bool
    supports_prompt_assistant: bool
    overview_mode: str
    # 共识分析能力配置：哪些字段做共识、各字段的随机基准、最小样本量阈值、窗口语义
    consensus_fields: tuple[str, ...] = ()
    consensus_baselines: tuple[tuple[str, float], ...] = ()
    consensus_min_sample_for_weight: int = 30
    # 'days' 表示 window 参数为天数（按 created_at 过滤），'issues' 表示按期数（LIMIT N）
    consensus_window_unit: str = 'days'

    def to_catalog_item(self) -> dict:
        return {
            'lottery_type': self.lottery_type,
            'lottery_label': self.label,
            'target_options': [
                {'key': key, 'label': label}
                for key, label in self.target_options
            ],
            'primary_metric_options': [
                {'key': key, 'label': label}
                for key, label in self.primary_metric_options
            ],
            'capabilities': {
                'supports_profit_simulation': self.supports_profit_simulation,
                'supports_public_pages': self.supports_public_pages,
                'supports_prompt_assistant': self.supports_prompt_assistant,
                'overview_mode': self.overview_mode,
                'consensus_fields': list(self.consensus_fields),
                'consensus_window_unit': self.consensus_window_unit
            }
        }

    def baseline_for(self, field_key: str) -> float:
        """该字段的随机基准命中率（百分比，0-100）。未声明则回退到 33.33（三选一）。"""
        for k, v in self.consensus_baselines:
            if k == field_key:
                return float(v)
        return 33.33


LOTTERY_DEFINITIONS = {
    'pc28': LotteryDefinition(
        lottery_type='pc28',
        label='PC28',
        target_options=(
            ('number', '单点'),
            ('big_small', '大/小'),
            ('odd_even', '单/双'),
            ('combo', '组合投注')
        ),
        primary_metric_options=(
            ('combo', '组合投注'),
            ('number', '单点'),
            ('big_small', '大/小'),
            ('odd_even', '单/双'),
            ('double_group', '组合分组统计'),
            ('kill_group', '排除统计')
        ),
        supports_profit_simulation=True,
        supports_public_pages=True,
        supports_prompt_assistant=True,
        overview_mode='pc28',
        # 共识分析仅做 combo（实测大小单双区分度极低、单点共识极少）
        consensus_fields=('combo',),
        consensus_baselines=(('combo', 25.0),),
        # PC28 方案区分度小，需要大样本压噪声
        consensus_min_sample_for_weight=200,
        consensus_window_unit='issues'
    ),
    'jingcai_football': LotteryDefinition(
        lottery_type='jingcai_football',
        label='竞彩足球',
        target_options=(
            ('spf', '胜平负'),
            ('rqspf', '让球胜平负')
        ),
        primary_metric_options=(
            ('spf', '胜平负'),
            ('rqspf', '让球胜平负')
        ),
        supports_profit_simulation=True,
        supports_public_pages=True,
        supports_prompt_assistant=True,
        overview_mode='jingcai_football',
        consensus_fields=('spf', 'rqspf'),
        consensus_baselines=(('spf', 33.33), ('rqspf', 33.33)),
        consensus_min_sample_for_weight=30,
        consensus_window_unit='days'
    )
}


def normalize_lottery_type(value) -> str:
    text = str(value or '').strip().lower()
    if text in LOTTERY_DEFINITIONS:
        return text
    return 'pc28'


def get_lottery_definition(lottery_type: str) -> LotteryDefinition:
    normalized = normalize_lottery_type(lottery_type)
    return LOTTERY_DEFINITIONS[normalized]


def list_lottery_catalog() -> list[dict]:
    return [
        LOTTERY_DEFINITIONS[key].to_catalog_item()
        for key in ('pc28', 'jingcai_football')
    ]


def normalize_prediction_targets(lottery_type: str, targets) -> list[str]:
    normalized = normalize_lottery_type(lottery_type)
    if normalized == 'jingcai_football':
        return football_utils.normalize_target_list(targets)
    return pc28_utils.normalize_target_list(targets)


def normalize_primary_metric(lottery_type: str, value) -> str:
    normalized = normalize_lottery_type(lottery_type)
    if normalized == 'jingcai_football':
        return football_utils.normalize_primary_metric(value)
    return pc28_utils.normalize_primary_metric(value)


def normalize_profit_metric(lottery_type: str, value) -> str:
    normalized = normalize_lottery_type(lottery_type)
    if normalized == 'jingcai_football':
        return football_utils.normalize_profit_metric(value)
    return pc28_utils.normalize_profit_metric(value)


def normalize_profit_rule(lottery_type: str, value) -> str:
    normalized = normalize_lottery_type(lottery_type)
    if normalized == 'jingcai_football':
        return football_utils.normalize_profit_rule(value)
    return pc28_utils.normalize_profit_rule(value)


def get_target_label(lottery_type: str, target: str) -> str:
    normalized = normalize_lottery_type(lottery_type)
    if normalized == 'jingcai_football':
        return football_utils.TARGET_LABELS.get(target, target)
    return pc28_utils.TARGET_LABELS.get(target, target)


def supports_profit_simulation(lottery_type: str) -> bool:
    return get_lottery_definition(lottery_type).supports_profit_simulation


def supports_public_pages(lottery_type: str) -> bool:
    return get_lottery_definition(lottery_type).supports_public_pages


def supports_prompt_assistant(lottery_type: str) -> bool:
    return get_lottery_definition(lottery_type).supports_prompt_assistant
