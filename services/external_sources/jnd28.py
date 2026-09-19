"""
JND（jnd-28.vip）接入插件。

实时采集使用 GET /api/ai-predict：一次返回目标期号与全部模型预测。
本期仅解析和值（scope=sum）的大小/单双/组合；号码集合、杀组合与分球玩法不解析。
"""
from __future__ import annotations

import json

import config
from services.external_sources.base import (
    EXTERNAL_TARGET_VALUES,
    SUPPORTED_TARGET_KEYS,
    ExternalModelSnapshot,
    ExternalSnapshot,
    ExternalSourceParseError,
    fetch_json,
)

PLUGIN_KEY = 'jnd28'
DISPLAY_NAME = 'JND 预测源'


class Jnd28Plugin:
    plugin_key = PLUGIN_KEY
    display_name = DISPLAY_NAME

    def validate_base_url(self, base_url: str) -> bool:
        text = str(base_url or '').strip().rstrip('/')
        if text.startswith('https://'):
            return True
        # 仅测试环境允许 http 回环地址
        return text.startswith('http://127.0.0.1') or text.startswith('http://localhost')

    def predict_url(self, base_url: str) -> str:
        return f"{str(base_url or '').strip().rstrip('/')}/api/ai-predict"

    def fetch_snapshot(self, base_url: str, timeout: float | None = None) -> ExternalSnapshot:
        resolved_timeout = float(timeout or config.EXTERNAL_SOURCE_REQUEST_TIMEOUT)
        max_bytes = int(config.EXTERNAL_SOURCE_MAX_RESPONSE_BYTES)
        data = fetch_json(self.predict_url(base_url), resolved_timeout, max_bytes)

        target_issue_no = str(data.get('draw_number') or '').strip()
        if not target_issue_no:
            raise ExternalSourceParseError('响应缺少目标期号 draw_number')

        models_raw = data.get('models')
        if not isinstance(models_raw, list):
            raise ExternalSourceParseError('响应缺少 models 列表')

        models: list[ExternalModelSnapshot] = []
        invalid_count = 0
        for model_raw in models_raw:
            model = self.parse_model(model_raw)
            if model is None:
                invalid_count += 1
                continue
            models.append(model)

        if not models:
            raise ExternalSourceParseError('models 中没有任何可解析的模型')

        return ExternalSnapshot(
            target_issue_no=target_issue_no,
            upstream_published_at=str(data.get('predict_time') or '').strip() or None,
            models=models,
            invalid_model_count=invalid_count,
            raw=json.dumps(data, ensure_ascii=False).encode('utf-8')
        )

    def parse_model(self, model_raw) -> ExternalModelSnapshot | None:
        if not isinstance(model_raw, dict):
            return None
        model_key = str(model_raw.get('model_type') or '').strip()
        display_name = str(model_raw.get('model_name') or '').strip()
        if not model_key or not display_name:
            return None
        if len(model_key) > 64 or len(display_name) > 100:
            return None

        targets = self._parse_sum_targets(model_raw)
        if not targets:
            return None

        confidence = None
        raw_confidence = model_raw.get('confidence')
        if isinstance(raw_confidence, (int, float)):
            confidence = round(float(raw_confidence), 4)

        return ExternalModelSnapshot(
            model_key=model_key,
            display_name=display_name,
            targets=targets,
            confidence=confidence,
            raw=model_raw
        )

    # JND items 的 category 名到本地目标键的映射
    CATEGORY_TARGET_MAP = {
        'big': 'big_small',
        'odd': 'odd_even',
        'group': 'combo'
    }

    def _parse_sum_targets(self, model_raw: dict) -> dict:
        """从 items 中提取 scope=sum 的大小/单双/组合，值必须在白名单内。"""
        items = model_raw.get('items')
        if not isinstance(items, list):
            return {}
        targets: dict = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get('scope') or '') != 'sum':
                continue
            target_key = self.CATEGORY_TARGET_MAP.get(str(item.get('category') or '').strip())
            if target_key not in SUPPORTED_TARGET_KEYS:
                continue
            value = str(item.get('value') or '').strip()
            if value in EXTERNAL_TARGET_VALUES[target_key]:
                targets[target_key] = value
        return targets


JND28_PLUGIN = Jnd28Plugin()
