"""
通知规则评估引擎
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from lotteries.registry import get_target_label, normalize_lottery_type, normalize_primary_metric


PERFORMANCE_EVENT_TYPE = 'performance_threshold'
SUPPORTED_OPERATORS = {'lt', 'lte', 'gt', 'gte', 'eq'}
OPERATOR_LABELS = {
    'lt': '低于',
    'lte': '低于或等于',
    'gt': '高于',
    'gte': '高于或等于',
    'eq': '等于'
}


class NotificationRuleEngine:
    def __init__(self, db):
        self.db = db

    def normalize_filter(self, filters: dict | None, lottery_type: str = 'pc28') -> tuple[dict, list[str]]:
        errors: list[str] = []
        normalized_lottery_type = normalize_lottery_type(lottery_type)
        raw_filters = filters if isinstance(filters, dict) else {}
        raw_rules = raw_filters.get('rules')
        if not isinstance(raw_rules, list) or not raw_rules:
            raw_rules = [raw_filters] if raw_filters else [{}]

        rules = []
        for index, raw_rule in enumerate(raw_rules):
            rule, rule_errors = self.normalize_rule(raw_rule if isinstance(raw_rule, dict) else {}, normalized_lottery_type, index)
            errors.extend(rule_errors)
            rules.append(rule)

        return {
            'version': 1,
            'rules': rules
        }, errors

    def normalize_rule(self, raw_rule: dict, lottery_type: str = 'pc28', index: int = 0) -> tuple[dict, list[str]]:
        errors: list[str] = []
        normalized_lottery_type = normalize_lottery_type(lottery_type)
        metric = normalize_primary_metric(normalized_lottery_type, raw_rule.get('metric') or 'big_small')
        window_payload = raw_rule.get('window') if isinstance(raw_rule.get('window'), dict) else {}
        validity_payload = raw_rule.get('validity') if isinstance(raw_rule.get('validity'), dict) else {}
        trigger_payload = raw_rule.get('trigger') if isinstance(raw_rule.get('trigger'), dict) else {}
        cooldown_payload = raw_rule.get('cooldown') if isinstance(raw_rule.get('cooldown'), dict) else {}

        window_size = self._parse_int(window_payload.get('size', raw_rule.get('window_size')), 100, 1, 1000)
        min_sample_count = self._parse_int(validity_payload.get('min_sample_count'), window_size, 1, window_size)
        invalidate_missing_gte = self._parse_optional_int(validity_payload.get('invalidate_missing_gte'), 3, 1, 100000)
        max_total_missing = self._parse_optional_int(validity_payload.get('max_total_missing'), None, 0, 1000000)
        operator = str(trigger_payload.get('operator') or raw_rule.get('operator') or 'lt').strip().lower()
        if operator not in SUPPORTED_OPERATORS:
            operator = 'lt'
            errors.append('表现告警比较方式无效，已回退为“低于”')
        threshold = self._parse_float(trigger_payload.get('value', raw_rule.get('threshold')), 40.0, 0.0, 100.0)
        cooldown_issues = self._parse_int(cooldown_payload.get('issues'), 20, 0, 100000)
        rule_id = self._normalize_rule_id(raw_rule.get('id'), metric, window_size, operator, threshold, index)
        name = str(raw_rule.get('name') or '').strip()
        if not name:
            name = f'{get_target_label(normalized_lottery_type, metric)}最近{window_size}期{OPERATOR_LABELS[operator]}{self._format_number(threshold)}%'

        return {
            'id': rule_id,
            'name': name,
            'enabled': self._parse_bool(raw_rule.get('enabled'), True),
            'metric': metric,
            'window': {
                'type': 'settled_metric_samples',
                'size': window_size
            },
            'validity': {
                'min_sample_count': min_sample_count,
                'invalidate_missing_gte': invalidate_missing_gte,
                'max_total_missing': max_total_missing,
                'require_numeric_issue': self._parse_bool(validity_payload.get('require_numeric_issue'), True)
            },
            'trigger': {
                'field': 'hit_rate',
                'operator': operator,
                'value': threshold
            },
            'cooldown': {
                'issues': cooldown_issues
            }
        }, errors

    def evaluate_predictor(self, predictor: dict) -> list[dict]:
        predictor_id = int(predictor.get('id') or 0)
        if predictor_id <= 0:
            return []

        subscriptions = self.db.list_active_notification_subscriptions_by_predictor(
            predictor_id,
            event_type=PERFORMANCE_EVENT_TYPE
        )
        evaluations: list[dict] = []
        for subscription in subscriptions:
            normalized_filter, errors = self.normalize_filter(
                subscription.get('filter') or {},
                subscription.get('predictor_lottery_type') or predictor.get('lottery_type') or 'pc28'
            )
            if errors:
                evaluations.append({
                    'subscription': subscription,
                    'status': 'invalid_rule',
                    'error_message': '；'.join(errors)
                })
                continue

            for rule in normalized_filter.get('rules') or []:
                evaluations.append(self.evaluate_subscription_rule(subscription, predictor, rule))
        return evaluations

    def evaluate_subscription_rule(self, subscription: dict, predictor: dict, rule: dict) -> dict:
        rule_id = str(rule.get('id') or '').strip()
        if not rule_id:
            return {'subscription': subscription, 'rule': rule, 'status': 'invalid_rule', 'error_message': '规则 ID 不能为空'}
        if not rule.get('enabled', True):
            return {'subscription': subscription, 'rule': rule, 'status': 'disabled'}

        predictor_id = int(subscription.get('predictor_id') or predictor.get('id') or 0)
        metric = str(rule.get('metric') or 'big_small').strip()
        window_size = int((rule.get('window') or {}).get('size') or 100)
        samples = self.db.get_recent_prediction_metric_samples(predictor_id, metric, limit=window_size)
        snapshot = self._build_sample_snapshot(samples, rule)
        latest_issue = snapshot.get('latest_issue')
        state = self.db.get_notification_rule_state(int(subscription['id']), rule_id)

        base_result = {
            'subscription': subscription,
            'rule': rule,
            'state': state,
            'snapshot': snapshot,
            'latest_issue': latest_issue,
            'status': 'not_triggered'
        }

        validity_error = self._validate_snapshot(snapshot, rule)
        if validity_error:
            return {
                **base_result,
                'status': 'invalid_window',
                'error_message': validity_error,
                'state_payload': self._build_state_payload(subscription, rule, snapshot, 'invalid_window', state)
            }

        trigger = rule.get('trigger') or {}
        hit_rate = snapshot.get('hit_rate')
        threshold = float(trigger.get('value'))
        operator = str(trigger.get('operator') or 'lt')
        if not self._compare(hit_rate, operator, threshold):
            return {
                **base_result,
                'status': 'not_triggered',
                'state_payload': self._build_state_payload(subscription, rule, snapshot, 'not_triggered', state)
            }

        if not self._cooldown_allows(state, latest_issue, int((rule.get('cooldown') or {}).get('issues') or 0)):
            return {
                **base_result,
                'status': 'cooldown',
                'state_payload': self._build_state_payload(subscription, rule, snapshot, 'cooldown', state)
            }

        event_payload = self._build_event_payload(subscription, predictor, rule, snapshot)
        return {
            **base_result,
            'status': 'triggered',
            'record_key': event_payload['record_key'],
            'event_payload': event_payload,
            'state_payload': self._build_state_payload(subscription, rule, snapshot, 'triggered', state, triggered=True)
        }

    def mark_evaluated(self, evaluation: dict):
        payload = evaluation.get('state_payload')
        if payload:
            self.db.upsert_notification_rule_state(payload)

    def _build_sample_snapshot(self, samples: list[dict], rule: dict) -> dict:
        issue_numbers = []
        numeric_issue = True
        for sample in samples:
            try:
                issue_numbers.append(int(str(sample.get('issue_no') or '').strip()))
            except (TypeError, ValueError):
                numeric_issue = False
                break

        gaps = []
        if numeric_issue:
            for current_issue, previous_issue in zip(issue_numbers, issue_numbers[1:]):
                gaps.append(max(0, current_issue - previous_issue - 1))

        hit_count = sum(int(sample.get('hit') or 0) for sample in samples)
        sample_count = len(samples)
        hit_rate = round(hit_count / sample_count * 100, 2) if sample_count else None
        return {
            'sample_count': sample_count,
            'hit_count': hit_count,
            'hit_rate': hit_rate,
            'latest_issue': str(samples[0].get('issue_no')) if samples else None,
            'oldest_issue': str(samples[-1].get('issue_no')) if samples else None,
            'numeric_issue': numeric_issue,
            'max_missing_count': max(gaps) if gaps else 0,
            'total_missing_count': sum(gaps) if gaps else 0,
            'gaps': gaps[:20],
            'window_size': int((rule.get('window') or {}).get('size') or 100)
        }

    def _validate_snapshot(self, snapshot: dict, rule: dict) -> str | None:
        validity = rule.get('validity') or {}
        sample_count = int(snapshot.get('sample_count') or 0)
        min_sample_count = int(validity.get('min_sample_count') or 1)
        if sample_count < min_sample_count:
            return f'有效样本 {sample_count} 条，低于最小样本 {min_sample_count} 条'
        if validity.get('require_numeric_issue', True) and not snapshot.get('numeric_issue'):
            return '窗口内存在无法解析的期号'
        invalidate_missing_gte = validity.get('invalidate_missing_gte')
        if invalidate_missing_gte is not None and int(snapshot.get('max_missing_count') or 0) >= int(invalidate_missing_gte):
            return f"窗口内最大断档 {snapshot.get('max_missing_count')} 期，达到无效阈值 {invalidate_missing_gte} 期"
        max_total_missing = validity.get('max_total_missing')
        if max_total_missing is not None and int(snapshot.get('total_missing_count') or 0) > int(max_total_missing):
            return f"窗口内累计断档 {snapshot.get('total_missing_count')} 期，超过允许值 {max_total_missing} 期"
        return None

    def _build_event_payload(self, subscription: dict, predictor: dict, rule: dict, snapshot: dict) -> dict:
        trigger = rule.get('trigger') or {}
        operator = str(trigger.get('operator') or 'lt')
        threshold = float(trigger.get('value') or 0)
        metric = str(rule.get('metric') or 'big_small')
        lottery_type = normalize_lottery_type(predictor.get('lottery_type') or subscription.get('predictor_lottery_type') or 'pc28')
        latest_issue = str(snapshot.get('latest_issue') or '')
        rule_id = str(rule.get('id') or '')
        return {
            'event_type': PERFORMANCE_EVENT_TYPE,
            'record_key': f'{rule_id}:{latest_issue}',
            'lottery_type': lottery_type,
            'predictor_id': predictor.get('id') or subscription.get('predictor_id'),
            'predictor_name': predictor.get('name') or subscription.get('predictor_name'),
            'title': 'PC28 方案表现告警',
            'rule_id': rule_id,
            'rule_name': rule.get('name') or rule_id,
            'metric': metric,
            'metric_label': get_target_label(lottery_type, metric),
            'window_size': snapshot.get('window_size'),
            'sample_count': snapshot.get('sample_count'),
            'hit_count': snapshot.get('hit_count'),
            'hit_rate': snapshot.get('hit_rate'),
            'operator': operator,
            'operator_label': OPERATOR_LABELS.get(operator, operator),
            'threshold': threshold,
            'latest_issue': latest_issue,
            'oldest_issue': snapshot.get('oldest_issue'),
            'max_missing_count': snapshot.get('max_missing_count'),
            'total_missing_count': snapshot.get('total_missing_count')
        }

    def _build_state_payload(
        self,
        subscription: dict,
        rule: dict,
        snapshot: dict,
        status: str,
        state: dict | None,
        triggered: bool = False
    ) -> dict:
        latest_issue = snapshot.get('latest_issue')
        return {
            'subscription_id': int(subscription['id']),
            'rule_id': str(rule.get('id') or ''),
            'last_evaluated_issue': latest_issue,
            'last_triggered_issue': latest_issue if triggered else ((state or {}).get('last_triggered_issue')),
            'last_triggered_at': self._utc_now_str() if triggered else ((state or {}).get('last_triggered_at')),
            'last_status': status,
            'last_payload': {
                'rule': rule,
                'snapshot': snapshot,
                'status': status
            }
        }

    def _cooldown_allows(self, state: dict | None, latest_issue: str | None, cooldown_issues: int) -> bool:
        if not state or cooldown_issues <= 0:
            return True
        last_triggered_issue = state.get('last_triggered_issue')
        if not last_triggered_issue or not latest_issue:
            return True
        try:
            return int(str(latest_issue)) - int(str(last_triggered_issue)) >= int(cooldown_issues)
        except (TypeError, ValueError):
            return True

    def _compare(self, actual: Any, operator: str, expected: float) -> bool:
        if actual is None:
            return False
        value = float(actual)
        if operator == 'lt':
            return value < expected
        if operator == 'lte':
            return value <= expected
        if operator == 'gt':
            return value > expected
        if operator == 'gte':
            return value >= expected
        if operator == 'eq':
            return value == expected
        return False

    def _normalize_rule_id(self, raw_value: Any, metric: str, window_size: int, operator: str, threshold: float, index: int) -> str:
        raw_text = str(raw_value or '').strip()
        if not raw_text:
            raw_text = f'{metric}_recent{window_size}_{operator}_{self._format_number(threshold)}_{index + 1}'
        normalized = re.sub(r'[^a-zA-Z0-9_-]+', '_', raw_text).strip('_')
        return normalized[:80] or f'rule_{index + 1}'

    def _parse_bool(self, value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}

    def _parse_int(self, value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, min(maximum, parsed))

    def _parse_optional_int(self, value: Any, default: int | None, minimum: int, maximum: int) -> int | None:
        if value is None or value == '':
            return default
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return max(minimum, min(maximum, parsed))

    def _parse_float(self, value: Any, default: float, minimum: float, maximum: float) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, min(maximum, parsed))

    def _format_number(self, value: float) -> str:
        return f'{value:g}'

    def _utc_now_str(self) -> str:
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
