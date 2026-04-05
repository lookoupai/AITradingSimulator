"""
PC28 收益模拟服务
"""
from __future__ import annotations

import json
from typing import Optional

from utils.pc28 import (
    DEFAULT_PROFIT_RULE_ID,
    TARGET_LABELS,
    is_pc28_baozi,
    is_pc28_pair,
    is_pc28_straight,
    normalize_profit_rule,
    parse_pc28_triplet
)
from utils.timezone import (
    format_beijing_time,
    get_current_beijing_time,
    get_pc28_day_window,
    parse_beijing_time
)


SUPPORTED_SIMULATION_METRICS = ('big_small', 'odd_even', 'combo', 'number')
DEFAULT_ODDS_PROFILE = 'regular'
STAKE_AMOUNT = 10.0

ODDS_PROFILE_LABELS = {
    'regular': '常规盘',
    'abc': 'ABC赔率'
}

COMBO_GROUPS = {
    '小单 / 大双': {'combos': {'小单', '大双'}},
    '大单 / 小双': {'combos': {'大单', '小双'}}
}

HIGH_NUMBER_ODDS = {
    0: 1000.0,
    1: 300.0,
    2: 141.0,
    3: 72.0,
    4: 55.0,
    5: 42.0,
    6: 28.0,
    7: 22.0,
    8: 18.0,
    9: 15.0,
    10: 14.0,
    11: 13.0,
    12: 12.0,
    13: 12.0,
    14: 12.0,
    15: 12.0,
    16: 13.0,
    17: 14.0,
    18: 15.0,
    19: 18.0,
    20: 22.0,
    21: 28.0,
    22: 42.0,
    23: 55.0,
    24: 72.0,
    25: 141.0,
    26: 300.0,
    27: 1000.0
}

NETDISK_NUMBER_ODDS = {
    0: 888.0,
    1: 280.0,
    2: 138.0,
    3: 58.0,
    4: 48.0,
    5: 32.0,
    6: 25.0,
    7: 20.0,
    8: 17.0,
    9: 15.0,
    10: 14.0,
    11: 13.0,
    12: 12.0,
    13: 12.0,
    14: 12.0,
    15: 12.0,
    16: 13.0,
    17: 14.0,
    18: 15.0,
    19: 17.0,
    20: 20.0,
    21: 25.0,
    22: 32.0,
    23: 48.0,
    24: 58.0,
    25: 138.0,
    26: 280.0,
    27: 888.0
}

PROFIT_RULES = {
    'pc28_netdisk': {
        'label': '加拿大28网盘',
        'metrics': {
            'big_small': {
                'regular': {'kind': 'basic', 'odds': 1.98, 'refund_policy': 'none'},
                'abc': {'kind': 'basic', 'odds': 1.98, 'refund_policy': 'none'}
            },
            'odd_even': {
                'regular': {'kind': 'basic', 'odds': 1.98, 'refund_policy': 'none'},
                'abc': {'kind': 'basic', 'odds': 1.98, 'refund_policy': 'none'}
            },
            'combo': {
                'regular': {
                    'kind': 'combo_group',
                    'refund_policy': 'none',
                    'group_odds': {
                        '小单 / 大双': 3.6,
                        '大单 / 小双': 4.2
                    }
                },
                'abc': {
                    'kind': 'combo_group',
                    'refund_policy': 'none',
                    'group_odds': {
                        '小单 / 大双': 4.8,
                        '大单 / 小双': 3.1
                    }
                }
            },
            'number': {
                'regular': {'kind': 'number_map', 'odds_map': NETDISK_NUMBER_ODDS},
                'abc': {'kind': 'fixed_number', 'odds': 9.8}
            }
        }
    },
    'pc28_high': {
        'label': '加拿大28高倍',
        'metrics': {
            'big_small': {
                'regular': {'kind': 'basic', 'odds': 2.846, 'refund_policy': 'special_on_hit'},
                'abc': {'kind': 'basic', 'odds': 1.98, 'refund_policy': 'none'}
            },
            'odd_even': {
                'regular': {'kind': 'basic', 'odds': 2.846, 'refund_policy': 'special_on_hit'},
                'abc': {'kind': 'basic', 'odds': 1.98, 'refund_policy': 'none'}
            },
            'combo': {
                'regular': {
                    'kind': 'combo_group',
                    'refund_policy': 'special_on_hit',
                    'group_odds': {
                        '小单 / 大双': 6.78,
                        '大单 / 小双': 6.33
                    }
                },
                'abc': {
                    'kind': 'combo_group',
                    'refund_policy': 'none',
                    'group_odds': {
                        '小单 / 大双': 4.9,
                        '大单 / 小双': 3.1
                    }
                }
            },
            'number': {
                'regular': {'kind': 'number_map', 'odds_map': HIGH_NUMBER_ODDS},
                'abc': {'kind': 'fixed_number', 'odds': 9.9}
            }
        }
    }
}


class ProfitSimulator:
    def __init__(self, db):
        self.db = db

    def get_available_metrics(self, predictor: dict) -> list[str]:
        targets = predictor.get('prediction_targets') or []
        metrics = [metric for metric in targets if metric in SUPPORTED_SIMULATION_METRICS]
        profit_default_metric = predictor.get('profit_default_metric')
        primary_metric = predictor.get('primary_metric')

        if profit_default_metric in metrics:
            metrics = [profit_default_metric] + [metric for metric in metrics if metric != profit_default_metric]
        elif primary_metric in {'double_group', 'kill_group'} and 'combo' in metrics:
            metrics = ['combo'] + [metric for metric in metrics if metric != 'combo']
        elif primary_metric in metrics:
            metrics = [primary_metric] + [metric for metric in metrics if metric != primary_metric]

        return metrics

    def get_default_metric(self, predictor: dict) -> Optional[str]:
        metrics = self.get_available_metrics(predictor)
        return metrics[0] if metrics else None

    def get_metric_options(self, predictor: dict) -> list[dict]:
        return [
            {
                'key': metric,
                'label': TARGET_LABELS.get(metric, metric)
            }
            for metric in self.get_available_metrics(predictor)
        ]

    def get_rule_options(self) -> list[dict]:
        return [
            {
                'key': key,
                'label': config['label']
            }
            for key, config in PROFIT_RULES.items()
        ]

    def get_rule_label(self, profit_rule_id: Optional[str]) -> str:
        normalized = normalize_profit_rule(profit_rule_id)
        return PROFIT_RULES.get(normalized, PROFIT_RULES[DEFAULT_PROFIT_RULE_ID])['label']

    def get_default_rule_id(self, predictor: dict | None = None) -> str:
        if predictor and predictor.get('profit_rule_id'):
            return normalize_profit_rule(predictor.get('profit_rule_id'))
        return DEFAULT_PROFIT_RULE_ID

    def get_odds_profile_options(self) -> list[dict]:
        return [
            {
                'key': key,
                'label': label
            }
            for key, label in ODDS_PROFILE_LABELS.items()
        ]

    def build_today_simulation(
        self,
        predictor_id: int,
        requested_metric: Optional[str] = None,
        profit_rule_id: Optional[str] = None,
        odds_profile: str = DEFAULT_ODDS_PROFILE,
        include_records: bool = True
    ) -> dict:
        predictor = self.db.get_predictor(predictor_id, include_secret=False)
        if not predictor:
            raise ValueError('预测方案不存在')

        available_metrics = self.get_available_metrics(predictor)
        if not available_metrics:
            raise ValueError('当前方案没有可用于收益模拟的玩法')

        effective_metric = self._resolve_metric(predictor, requested_metric, available_metrics)
        effective_rule_id = self._resolve_rule_id(predictor, profit_rule_id)
        normalized_odds_profile = self._resolve_odds_profile(odds_profile)
        period = get_pc28_day_window(get_current_beijing_time())

        predictions = self.db.get_recent_predictions(predictor_id, limit=None)
        settled_predictions = [item for item in predictions if item.get('status') == 'settled']
        draws_by_issue = self.db.get_draws_by_issues('pc28', [item['issue_no'] for item in settled_predictions])

        records: list[dict] = []
        cumulative_profit = 0.0
        hit_count = 0
        refund_count = 0
        miss_count = 0
        skipped_count = 0

        for prediction in sorted(settled_predictions, key=lambda item: int(item['issue_no'])):
            draw = draws_by_issue.get(prediction['issue_no'])
            if not draw:
                skipped_count += 1
                continue

            open_time = parse_beijing_time(draw.get('open_time'))
            if not open_time:
                skipped_count += 1
                continue

            if not (period['start'] <= open_time < period['end_exclusive']):
                continue

            record = self._build_record(
                prediction,
                draw,
                effective_metric,
                effective_rule_id,
                normalized_odds_profile
            )
            if not record:
                skipped_count += 1
                continue

            cumulative_profit += record['net_profit']
            record['cumulative_profit'] = round(cumulative_profit, 2)

            if record['result_type'] == 'hit':
                hit_count += 1
            elif record['result_type'] == 'refund':
                refund_count += 1
            else:
                miss_count += 1

            if include_records:
                records.append(record)

        bet_count = hit_count + refund_count + miss_count
        total_stake = round(bet_count * STAKE_AMOUNT, 2)
        total_payout = round(sum(item['payout_amount'] for item in records), 2) if include_records else round(
            self._recalculate_total_payout(
                settled_predictions,
                draws_by_issue,
                period,
                effective_metric,
                effective_rule_id,
                normalized_odds_profile
            ),
            2
        )

        net_profit = round(total_payout - total_stake, 2)
        roi_percentage = round(net_profit / total_stake * 100, 2) if total_stake else 0.0
        average_profit = round(net_profit / bet_count, 2) if bet_count else 0.0

        return {
            'metric': effective_metric,
            'metric_label': TARGET_LABELS.get(effective_metric, effective_metric),
            'profit_rule_id': effective_rule_id,
            'profit_rule_label': self.get_rule_label(effective_rule_id),
            'odds_profile': normalized_odds_profile,
            'odds_profile_label': ODDS_PROFILE_LABELS[normalized_odds_profile],
            'profit_rules': self.get_rule_options(),
            'odds_profiles': self.get_odds_profile_options(),
            'stake_amount': STAKE_AMOUNT,
            'available_metrics': self.get_metric_options(predictor),
            'default_metric': self.get_default_metric(predictor),
            'default_profit_rule_id': self.get_default_rule_id(predictor),
            'period': {
                'mode': 'pc28_day',
                'label': '按PC28盘日',
                'start_time': format_beijing_time(period['start']),
                'end_time': format_beijing_time(period['end_exclusive']),
                'timezone': 'Asia/Shanghai',
                'is_dst': period['is_dst'],
                'boundary_hour': period['boundary_hour']
            },
            'summary': {
                'bet_count': bet_count,
                'hit_count': hit_count,
                'refund_count': refund_count,
                'miss_count': miss_count,
                'skipped_count': skipped_count,
                'total_stake': total_stake,
                'total_payout': total_payout,
                'net_profit': net_profit,
                'roi_percentage': roi_percentage,
                'average_profit': average_profit
            },
            'records': records
        }

    def _resolve_metric(self, predictor: dict, requested_metric: Optional[str], available_metrics: list[str]) -> str:
        metric = str(requested_metric or '').strip().lower()
        if metric in available_metrics:
            return metric
        return self.get_default_metric(predictor) or available_metrics[0]

    def _resolve_rule_id(self, predictor: dict, requested_rule_id: Optional[str]) -> str:
        if requested_rule_id:
            return normalize_profit_rule(requested_rule_id)
        return self.get_default_rule_id(predictor)

    def _resolve_odds_profile(self, odds_profile: Optional[str]) -> str:
        text = str(odds_profile or '').strip().lower()
        if text in ODDS_PROFILE_LABELS:
            return text
        return DEFAULT_ODDS_PROFILE

    def _build_record(
        self,
        prediction: dict,
        draw: dict,
        metric: str,
        profit_rule_id: str,
        odds_profile: str
    ) -> Optional[dict]:
        profile = self._get_metric_profile(profit_rule_id, metric, odds_profile)
        if not profile:
            return None

        special_flags = self._build_special_flags(draw)
        open_time = parse_beijing_time(draw.get('open_time'))
        if not open_time:
            return None

        predicted_value = None
        actual_value = None
        ticket_label = '--'
        result_type = 'miss'
        refund_reason = None
        odds = 0.0

        if metric == 'big_small':
            predicted_value = prediction.get('prediction_big_small')
            actual_value = draw.get('big_small')
            if not predicted_value or not actual_value:
                return None
            odds = float(profile['odds'])
            ticket_label = predicted_value
            result_type, refund_reason = self._resolve_refund_aware_result(
                predicted_value == actual_value,
                special_flags,
                profile.get('refund_policy', 'none')
            )
        elif metric == 'odd_even':
            predicted_value = prediction.get('prediction_odd_even')
            actual_value = draw.get('odd_even')
            if not predicted_value or not actual_value:
                return None
            odds = float(profile['odds'])
            ticket_label = predicted_value
            result_type, refund_reason = self._resolve_refund_aware_result(
                predicted_value == actual_value,
                special_flags,
                profile.get('refund_policy', 'none')
            )
        elif metric == 'combo':
            predicted_value = prediction.get('prediction_combo')
            actual_value = draw.get('combo')
            if not predicted_value or not actual_value:
                return None
            odds = self._combo_odds_for_value(predicted_value, profile.get('group_odds') or {})
            if odds is None:
                return None
            ticket_label = predicted_value
            result_type, refund_reason = self._resolve_refund_aware_result(
                predicted_value == actual_value,
                special_flags,
                profile.get('refund_policy', 'none')
            )
        elif metric == 'number':
            predicted_value = prediction.get('prediction_number')
            actual_value = draw.get('result_number')
            if predicted_value is None or actual_value is None:
                return None
            odds = self._resolve_number_odds(profile, int(predicted_value))
            if odds <= 0:
                return None
            ticket_label = str(predicted_value).zfill(2)
            result_type = 'hit' if int(predicted_value) == int(actual_value) else 'miss'
        else:
            return None

        payout_amount = self._resolve_payout_amount(result_type, odds)
        net_profit = round(payout_amount - STAKE_AMOUNT, 2)

        return {
            'issue_no': prediction['issue_no'],
            'open_time': format_beijing_time(open_time),
            'metric': metric,
            'metric_label': TARGET_LABELS.get(metric, metric),
            'profit_rule_id': profit_rule_id,
            'profit_rule_label': self.get_rule_label(profit_rule_id),
            'ticket_label': ticket_label,
            'predicted_value': self._display_value(metric, predicted_value),
            'actual_value': self._display_value(metric, actual_value),
            'actual_result_number': str(draw.get('result_number_text') or draw.get('result_number') or '--'),
            'result_type': result_type,
            'result_label': self._result_label(result_type),
            'refund_reason': refund_reason,
            'odds': odds,
            'stake_amount': STAKE_AMOUNT,
            'payout_amount': round(payout_amount, 2),
            'net_profit': net_profit
        }

    def _recalculate_total_payout(
        self,
        predictions: list[dict],
        draws_by_issue: dict[str, dict],
        period: dict,
        metric: str,
        profit_rule_id: str,
        odds_profile: str
    ) -> float:
        total_payout = 0.0
        for prediction in predictions:
            draw = draws_by_issue.get(prediction['issue_no'])
            if not draw:
                continue
            open_time = parse_beijing_time(draw.get('open_time'))
            if not open_time or not (period['start'] <= open_time < period['end_exclusive']):
                continue
            record = self._build_record(prediction, draw, metric, profit_rule_id, odds_profile)
            if record:
                total_payout += record['payout_amount']
        return total_payout

    def _get_metric_profile(self, profit_rule_id: str, metric: str, odds_profile: str) -> Optional[dict]:
        rule = PROFIT_RULES.get(profit_rule_id)
        if not rule:
            return None
        metric_profiles = rule.get('metrics', {}).get(metric, {})
        return metric_profiles.get(odds_profile) or metric_profiles.get(DEFAULT_ODDS_PROFILE)

    def _build_special_flags(self, draw: dict) -> dict:
        source_payload = {}
        try:
            source_payload = json.loads(draw.get('source_payload') or '{}')
        except Exception:
            source_payload = {}

        triplet = parse_pc28_triplet(source_payload.get('number'))
        result_number = int(draw.get('result_number') or 0)
        return {
            'is_special_sum': result_number in {13, 14},
            'is_pair': is_pc28_pair(triplet),
            'is_straight': is_pc28_straight(triplet),
            'is_baozi': is_pc28_baozi(triplet)
        }

    def _resolve_refund_aware_result(
        self,
        hit: bool,
        special_flags: dict,
        refund_policy: str
    ) -> tuple[str, Optional[str]]:
        if not hit:
            return 'miss', None

        if refund_policy != 'special_on_hit':
            return 'hit', None

        if special_flags.get('is_special_sum'):
            return 'refund', '13/14 退本金'
        if special_flags.get('is_baozi'):
            return 'refund', '豹子退本金'
        if special_flags.get('is_straight'):
            return 'refund', '顺子退本金'
        if special_flags.get('is_pair'):
            return 'refund', '对子退本金'
        return 'hit', None

    def _combo_odds_for_value(self, combo_value: str, group_odds: dict[str, float]) -> Optional[float]:
        for group_label, config in COMBO_GROUPS.items():
            if combo_value in config['combos']:
                return group_odds.get(group_label)
        return None

    def _resolve_number_odds(self, profile: dict, predicted_number: int) -> float:
        if profile.get('kind') == 'number_map':
            return float((profile.get('odds_map') or {}).get(predicted_number, 0.0))
        return float(profile.get('odds') or 0.0)

    def _resolve_payout_amount(self, result_type: str, odds: float) -> float:
        if result_type == 'hit':
            return STAKE_AMOUNT * odds
        if result_type == 'refund':
            return STAKE_AMOUNT
        return 0.0

    def _display_value(self, metric: str, value) -> str:
        if value is None:
            return '--'
        if metric == 'number':
            return str(value).zfill(2)
        return str(value)

    def _result_label(self, result_type: str) -> str:
        mapping = {
            'hit': '命中',
            'refund': '退本金',
            'miss': '未中'
        }
        return mapping.get(result_type, result_type)
