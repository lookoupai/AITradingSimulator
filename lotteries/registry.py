"""
彩种能力目录
"""
from __future__ import annotations

from dataclasses import dataclass

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
                'overview_mode': self.overview_mode
            }
        }


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
        overview_mode='pc28'
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
        overview_mode='jingcai_football'
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
