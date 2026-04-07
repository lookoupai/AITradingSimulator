"""
统一预测熔断保护
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


AI_FAILURE_GUARD_ENABLED_KEY = 'prediction_guard.ai_failure_guard_enabled'
AI_FAILURE_PAUSE_THRESHOLD_KEY = 'prediction_guard.ai_failure_pause_threshold'
DEFAULT_AI_FAILURE_GUARD_ENABLED = True
DEFAULT_AI_FAILURE_PAUSE_THRESHOLD = 3


@dataclass
class PredictionGuardSettings:
    enabled: bool = DEFAULT_AI_FAILURE_GUARD_ENABLED
    threshold: int = DEFAULT_AI_FAILURE_PAUSE_THRESHOLD

    def to_dict(self) -> dict:
        return {
            'enabled': bool(self.enabled),
            'threshold': int(self.threshold)
        }


class AIPredictionError(Exception):
    def __init__(self, message: str, category: str = 'ai_error'):
        super().__init__(message)
        self.category = category or 'ai_error'


class PredictionGuardService:
    def __init__(self, db):
        self.db = db

    def get_settings(self) -> dict:
        raw_settings = self.db.get_system_settings([
            AI_FAILURE_GUARD_ENABLED_KEY,
            AI_FAILURE_PAUSE_THRESHOLD_KEY
        ])
        enabled = self._parse_bool(
            raw_settings.get(AI_FAILURE_GUARD_ENABLED_KEY),
            DEFAULT_AI_FAILURE_GUARD_ENABLED
        )
        threshold = self._parse_int(
            raw_settings.get(AI_FAILURE_PAUSE_THRESHOLD_KEY),
            DEFAULT_AI_FAILURE_PAUSE_THRESHOLD
        )
        threshold = max(1, min(threshold, 20))
        return PredictionGuardSettings(enabled=enabled, threshold=threshold).to_dict()

    def update_settings(self, enabled: bool, threshold: int) -> dict:
        normalized_threshold = max(1, min(int(threshold), 20))
        self.db.set_system_settings({
            AI_FAILURE_GUARD_ENABLED_KEY: '1' if enabled else '0',
            AI_FAILURE_PAUSE_THRESHOLD_KEY: str(normalized_threshold)
        })
        return self.get_settings()

    def record_success(self, predictor_id: int) -> dict:
        state = self.db.get_predictor_runtime_state(predictor_id)
        if int(state.get('consecutive_ai_failures') or 0) == 0:
            return state

        self.db.update_predictor_runtime_state(predictor_id, {
            'consecutive_ai_failures': 0
        })
        return self.db.get_predictor_runtime_state(predictor_id)

    def record_ai_failure(self, predictor_id: int, error: Exception | str) -> dict:
        state = self.db.get_predictor_runtime_state(predictor_id)
        settings = self.get_settings()
        next_failures = int(state.get('consecutive_ai_failures') or 0) + 1
        normalized_error = self._normalize_error(error)
        payload = {
            'consecutive_ai_failures': next_failures,
            'last_ai_error_category': normalized_error.category,
            'last_ai_error_message': str(normalized_error),
            'last_ai_error_at': self._utc_now_str()
        }

        if settings['enabled'] and next_failures >= settings['threshold']:
            payload.update({
                'auto_paused': True,
                'auto_paused_at': self._utc_now_str(),
                'auto_pause_reason': f'AI 连续失败 {next_failures} 次，已自动暂停：{str(normalized_error)[:200]}'
            })

        self.db.update_predictor_runtime_state(predictor_id, payload)
        return self.db.get_predictor_runtime_state(predictor_id)

    def resume_predictor(self, predictor_id: int) -> dict:
        self.db.update_predictor_runtime_state(predictor_id, {
            'consecutive_ai_failures': 0,
            'auto_paused': False,
            'auto_paused_at': None,
            'auto_pause_reason': None
        })
        return self.db.get_predictor_runtime_state(predictor_id)

    def _normalize_error(self, error: Exception | str) -> AIPredictionError:
        if isinstance(error, AIPredictionError):
            return error
        return AIPredictionError(str(error))

    def _parse_bool(self, value, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}

    def _parse_int(self, value, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _utc_now_str(self) -> str:
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
