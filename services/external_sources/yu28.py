"""
与28开放平台（yu28.top）接入插件。

- 目录：GET /api/dt 公开算法大厅（按支持分类分页拉取）＋两个固定 AI 模型（ai_dx/ai_ds）。
- 预测：按需逐个算法取 GET /api/sf?id=（predict + predictNbr），AI 模型走 /api/dx、/api/ds 的待开行。
- 鉴权：请求头 X-Api-Key（个人 Key 禁止放 URL，文档硬性规则）。

值域映射：
- big_small 算法 predict ∈ {大, 小}；odd_even ∈ {单, 双}；直接进入白名单校验。
- combo_predict 算法 predict 形如 "大+双"，去掉加号后必须是 {大单,大双,小单,小双}。
- kill_group / double_group 分类不是本平台支持的预测目标，目录阶段即排除。
"""
from __future__ import annotations

import json
import re

import config
from services.external_sources.base import (
    EXTERNAL_TARGET_VALUES,
    ExternalModelSnapshot,
    ExternalSnapshot,
    ExternalSourceError,
    ExternalSourceParseError,
    ExternalSourceRequestError,
    fetch_json,
)

PLUGIN_KEY = 'yu28'
DISPLAY_NAME = '与28开放平台（yu28.top）'

SUPPORTED_CATEGORIES = ('big_small', 'odd_even', 'combo_predict')
# 大厅分类 → 本地预测目标键
CATEGORY_TO_TARGET = {
    'big_small': 'big_small',
    'odd_even': 'odd_even',
    'combo_predict': 'combo'
}
FIXED_AI_MODELS = {
    'ai_dx': {'endpoint': '/api/dx.json', 'target': 'big_small', 'label': '人工智能·大小'},
    'ai_ds': {'endpoint': '/api/ds.json', 'target': 'odd_even', 'label': '人工智能·单双'},
}
HALL_ID_PATTERN = re.compile(r'^\d{1,9}$')
COMBO_CLEAN_PATTERN = re.compile(r'\s+')


class Yu28Plugin:
    plugin_key = PLUGIN_KEY
    display_name = DISPLAY_NAME
    # 采集按绑定模型逐个请求（与 JND 整批相反）
    fetches_per_model = True
    requires_api_key = True

    def validate_base_url(self, base_url: str) -> bool:
        text = str(base_url or '').strip().rstrip('/')
        if text.startswith('https://'):
            return True
        return text.startswith('http://127.0.0.1') or text.startswith('http://localhost')

    def _headers(self, api_key: str | None) -> dict:
        key = str(api_key or '').strip()
        if not key:
            raise ExternalSourceRequestError('缺少 API Key：请在来源配置中填写 yu28_ 开头的 Key')
        return {'X-Api-Key': key}

    def _get(self, base_url: str, path: str, api_key: str | None, timeout: float) -> dict:
        url = f"{str(base_url or '').strip().rstrip('/')}{path}"
        return fetch_json(
            url,
            timeout,
            int(config.EXTERNAL_SOURCE_MAX_RESPONSE_BYTES),
            headers=self._headers(api_key)
        )

    # ============ 目录 ============

    def probe(self, base_url: str, api_key: str | None = None, timeout: float | None = None) -> dict:
        """管理端“测试连接”：验证 Key 与连通性（拉目录）＋试取固定 AI 模型验证预测链路。"""
        resolved_timeout = float(timeout or config.EXTERNAL_SOURCE_REQUEST_TIMEOUT)
        catalog = self.fetch_catalog(base_url, api_key=api_key, timeout=resolved_timeout)
        snapshot = self.fetch_snapshot(
            base_url,
            timeout=resolved_timeout,
            api_key=api_key,
            model_keys=['ai_dx', 'ai_ds']
        )
        return {
            'target_issue_no': snapshot.target_issue_no,
            'upstream_published_at': snapshot.upstream_published_at,
            'model_count': len(catalog),
            'sample_model_count': len(snapshot.models),
            'invalid_model_count': snapshot.invalid_model_count
        }

    def fetch_catalog(self, base_url: str, api_key: str | None = None, timeout: float | None = None) -> list[dict]:
        resolved_timeout = float(timeout or config.EXTERNAL_SOURCE_REQUEST_TIMEOUT)
        pages = max(1, min(int(getattr(config, 'EXTERNAL_SOURCE_YU28_CATALOG_PAGES', 2)), 10))
        records = []

        for model_key, meta in FIXED_AI_MODELS.items():
            records.append({
                'model_key': model_key,
                'display_name': meta['label'],
                'supported_targets': [meta['target']]
            })

        seen_ids = set()
        for category in SUPPORTED_CATEGORIES:
            for page in range(1, pages + 1):
                data = self._get(
                    base_url,
                    f"/api/dt.json?category={category}&sort=rate_desc&window=20&page={page}&pageSize=50",
                    api_key,
                    resolved_timeout
                )
                items = data.get('items')
                if not isinstance(items, list):
                    break
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    algorithm_id = str(item.get('id') or '').strip()
                    if not HALL_ID_PATTERN.match(algorithm_id) or algorithm_id in seen_ids:
                        continue
                    seen_ids.add(algorithm_id)
                    records.append({
                        'model_key': algorithm_id,
                        'display_name': self._display_name(item, category),
                        'supported_targets': [CATEGORY_TO_TARGET[category]],
                    })
                if len(items) < 50:
                    break
        return records

    def _display_name(self, item: dict, category: str) -> str:
        name = str(item.get('name') or '').strip() or f'算法 {item.get("id")}'
        hit20 = str(item.get('hit20') or '').strip()
        label = f'{name}（{category}）'
        if hit20:
            if category == 'combo_predict':
                label = f'{label} 近20期 {hit20}（分项口径）'
            else:
                label = f'{label} 近20期 {hit20}'
        return label[:100]

    # ============ 预测 ============

    def fetch_snapshot(
        self,
        base_url: str,
        timeout: float | None = None,
        api_key: str | None = None,
        model_keys: list[str] | None = None
    ) -> ExternalSnapshot:
        resolved_timeout = float(timeout or config.EXTERNAL_SOURCE_REQUEST_TIMEOUT)
        keys = [str(key or '').strip() for key in (model_keys or [])]
        keys = [key for key in keys if key]
        if not keys:
            raise ExternalSourceParseError('yu28 来源必须指定要采集的算法列表')
        if self.requires_api_key and not str(api_key or '').strip():
            raise ExternalSourceRequestError('缺少 API Key：请在来源配置中填写 yu28_ 开头的 Key')

        models: list[ExternalModelSnapshot] = []
        invalid_count = 0
        target_issue: str | None = None

        for model_key in keys:
            try:
                model = self._fetch_model(base_url, api_key, resolved_timeout, model_key)
            except ExternalSourceError:
                invalid_count += 1
                continue
            if model is None:
                invalid_count += 1
                continue
            if target_issue is None:
                target_issue = model.target_issue
            elif model.target_issue != target_issue:
                # 同一批次内目标期号必须一致，异常算法隔离
                invalid_count += 1
                continue
            models.append(model.snapshot)

        if target_issue is None:
            raise ExternalSourceParseError('本次采集没有取得任何有效预测（算法全部不可用）')
        if not models:
            raise ExternalSourceParseError('本次采集没有可用的模型预测')

        # 原始载荷保留各模型的标准化切片，供批次留档与采用阶段反解析
        raw_payload = json.dumps(
            {'models': [model.raw for model in models]},
            ensure_ascii=False
        ).encode('utf-8')

        return ExternalSnapshot(
            target_issue_no=target_issue,
            upstream_published_at=None,
            models=models,
            invalid_model_count=invalid_count,
            raw=raw_payload
        )

    def parse_model(self, model_raw):
        """从批次留档的模型切片恢复标准化快照（与 _fetch_* 产出的 raw 结构对应）。"""
        if not isinstance(model_raw, dict):
            return None
        model_key = str(model_raw.get('model_key') or '').strip()
        if not model_key:
            return None
        category = str(model_raw.get('category') or '').strip()
        if category in CATEGORY_TO_TARGET:
            target_key = CATEGORY_TO_TARGET[category]
            value = self._normalize_predict(category, str(model_raw.get('predict') or ''))
            if value is None:
                return None
            return ExternalModelSnapshot(
                model_key=model_key,
                display_name=self._display_name(model_raw, category),
                targets={target_key: value},
                confidence=self._hit_rate_confidence(category, model_raw),
                raw=model_raw
            )
        if model_key in FIXED_AI_MODELS:
            meta = FIXED_AI_MODELS[model_key]
            row = model_raw.get('source_row') or {}
            value = str(row.get('predict') or '').strip()
            if value not in EXTERNAL_TARGET_VALUES[meta['target']]:
                return None
            return ExternalModelSnapshot(
                model_key=model_key,
                display_name=meta['label'],
                targets={meta['target']: value},
                confidence=None,
                raw=model_raw
            )
        return None

    def _fetch_model(self, base_url: str, api_key: str | None, timeout: float, model_key: str):
        if model_key in FIXED_AI_MODELS:
            return self._fetch_ai_model(base_url, api_key, timeout, model_key)
        if HALL_ID_PATTERN.match(model_key):
            return self._fetch_hall_model(base_url, api_key, timeout, model_key)
        return None

    def _fetch_ai_model(self, base_url: str, api_key: str | None, timeout: float, model_key: str):
        meta = FIXED_AI_MODELS[model_key]
        try:
            data = self._get(base_url, f"{meta['endpoint']}?nbr=1", api_key, timeout)
        except ExternalSourceError:
            return None
        rows = data.get('data')
        if not isinstance(rows, list) or not rows:
            return None
        pending = next((row for row in rows if isinstance(row, dict) and row.get('num') is None), None)
        if not pending:
            return None
        issue_no = str(pending.get('nbr') or '').strip()
        value = str(pending.get('predict') or '').strip()
        target_key = meta['target']
        if not issue_no or value not in EXTERNAL_TARGET_VALUES[target_key]:
            return None
        return _ModelResult(
            target_issue=issue_no,
            snapshot=ExternalModelSnapshot(
                model_key=model_key,
                display_name=meta['label'],
                targets={target_key: value},
                confidence=None,
                raw={'model_key': model_key, 'source_row': pending}
            )
        )

    def _fetch_hall_model(self, base_url: str, api_key: str | None, timeout: float, model_key: str):
        try:
            data = self._get(base_url, f"/api/sf.json?id={model_key}", api_key, timeout)
        except ExternalSourceError:
            return None
        category = str(data.get('category') or '').strip()
        target_key = CATEGORY_TO_TARGET.get(category)
        if target_key is None:
            return None
        issue_no = str(data.get('predictNbr') or '').strip()
        raw_value = str(data.get('predict') or '').strip()
        value = self._normalize_predict(category, raw_value)
        if not issue_no or value is None:
            return None
        return _ModelResult(
            target_issue=issue_no,
            snapshot=ExternalModelSnapshot(
                model_key=model_key,
                display_name=self._display_name(data, category),
                targets={target_key: value},
                confidence=self._hit_rate_confidence(category, data),
                raw={
                    'model_key': model_key,
                    'category': category,
                    'name': data.get('name'),
                    'author': data.get('author'),
                    'hit20': data.get('hit20'),
                    'hit100': data.get('hit100'),
                    'streak': data.get('streak'),
                    'miss': data.get('miss'),
                    'predict': raw_value,
                    'predictNbr': issue_no
                }
            )
        )

    def _normalize_predict(self, category: str, raw_value: str):
        """大厅 predict 值归一到本地白名单；白名单外返回 None（跳过该目标）。"""
        target_key = CATEGORY_TO_TARGET.get(category)
        if target_key is None:
            return None
        if category == 'combo_predict':
            cleaned = COMBO_CLEAN_PATTERN.sub('', raw_value).replace('+', '')
            return cleaned if cleaned in EXTERNAL_TARGET_VALUES[target_key] else None
        return raw_value if raw_value in EXTERNAL_TARGET_VALUES[target_key] else None

    def _hit_rate_confidence(self, category: str, data: dict):
        """把上游自报的近 20 期命中率换算为 0-100 的展示评分；解析失败返回 None。

        实测（2026-09-19，100 期逐期核对零偏差）：yu28 组合算法的 hit 是"大小或单双
        任一对就算中"的宽松口径（基线 75%），与本平台精确组合结算（基线 25%）不可比，
        因此组合类算法不给评分，避免与本地命中率混淆；大小/单双为精确口径，可保留。
        """
        if category == 'combo_predict':
            return None
        hit20 = str(data.get('hit20') or '').strip()
        if not hit20 or '/' not in hit20:
            return None
        numerator, _, denominator = hit20.partition('/')
        try:
            total = float(denominator)
            hit = float(numerator)
        except ValueError:
            return None
        if total <= 0 or hit < 0 or hit > total:
            return None
        return round(hit / total * 100.0, 2)


class _ModelResult:
    def __init__(self, target_issue: str, snapshot: ExternalModelSnapshot):
        self.target_issue = target_issue
        self.snapshot = snapshot


YU28_PLUGIN = Yu28Plugin()
