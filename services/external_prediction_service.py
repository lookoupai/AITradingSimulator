"""
外部预测源采集、采用与结算辅助服务。

职责边界：
- 采集（collect_source/refresh_catalog）：网络请求 → 标准化 → 批次留档（按内容指纹去重）。
- 采用（adopt_prediction_for_predictor）：把已留档批次中的模型快照固化为用户方案的 predictions 行。
- 结算辅助（prepare_external_settlement_payload）：结算前回溯校验"采集时是否已开奖"。
"""
from __future__ import annotations

import hashlib
import json
import zlib
from datetime import datetime, timedelta

import config
from services.external_sources import get_plugin
from services.external_sources.base import (
    ExternalSourceError,
    parse_iso_timestamp,
)
from utils.pc28 import next_issue_no
from utils.predictor_engine import normalize_engine_type
from utils.timezone import get_current_utc_time_str

BEIJING_OFFSET = timedelta(hours=8)


def parse_time_or_none(value) -> datetime | None:
    """解析数据库存储的时间字符串（UTC，空格分隔或 ISO 均可）。"""
    return parse_iso_timestamp(value)


def is_external_predictor(predictor: dict | None) -> bool:
    return normalize_engine_type((predictor or {}).get('engine_type')) == 'external'


def compute_snapshot_fingerprint(target_issue_no: str, models: list[dict]) -> str:
    """决策内容指纹：只含期号与各模型的大小/单双/组合决策，不含 confidence 等易抖动元数据。"""
    digest = hashlib.sha256()
    digest.update(str(target_issue_no).encode('utf-8'))
    for model in sorted(models, key=lambda item: str(item.get('model_key') or '')):
        digest.update(b'\x1e')
        digest.update(str(model.get('model_key') or '').encode('utf-8'))
        for key in ('big_small', 'odd_even', 'combo'):
            digest.update(b'\x1f')
            digest.update(str(model.get('targets', {}).get(key) or '').encode('utf-8'))
    return digest.hexdigest()


def collect_source(db, source: dict) -> dict:
    """执行一次采集：请求 → 校验 → 批次留档 → 更新来源状态。不抛网络异常，返回结果描述。"""
    source_id = int(source['id'])
    plugin = get_plugin(source.get('plugin_key'))
    fetched_at = get_current_utc_time_str()
    model_keys = []
    if getattr(plugin, 'fetches_per_model', False):
        # 按需来源（如 yu28）：只请求启用且被启用方案绑定的算法，避免无谓请求
        model_keys = db.list_bound_enabled_external_model_keys(source_id)
        if not model_keys:
            return {'ok': True, 'skipped': True, 'reason': '没有启用且被方案绑定的算法，跳过采集'}
    try:
        snapshot = plugin.fetch_snapshot(
            source.get('base_url'),
            api_key=source.get('api_key'),
            model_keys=model_keys or None
        )
    except ExternalSourceError as exc:
        failures = int(source.get('consecutive_failures') or 0) + 1
        db.update_external_source(source_id, {
            'last_attempt_at': fetched_at,
            'last_error': str(exc),
            'last_error_at': fetched_at,
            'consecutive_failures': failures
        })
        return {'ok': False, 'error': str(exc), 'fetched_at': fetched_at, 'consecutive_failures': failures}

    model_records = [
        {
            'model_key': model.model_key,
            'display_name': model.display_name,
            'supported_targets': model.supported_targets,
            'targets': dict(model.targets)
        }
        for model in snapshot.models
    ]
    fingerprint = compute_snapshot_fingerprint(snapshot.target_issue_no, model_records)
    raw_compressed = zlib.compress(snapshot.raw or b'')
    batch_id, created = db.record_external_batch(
        source_id,
        {
            'target_issue_no': snapshot.target_issue_no,
            'upstream_published_at': snapshot.upstream_published_at,
            'model_count': len(snapshot.models),
            'invalid_model_count': snapshot.invalid_model_count
        },
        raw_compressed,
        fingerprint,
        fetched_at
    )
    db.update_external_source(source_id, {
        'last_attempt_at': fetched_at,
        'last_success_at': fetched_at,
        'last_error': None,
        'last_target_issue': snapshot.target_issue_no,
        'last_model_count': len(snapshot.models),
        'consecutive_failures': 0
    })
    return {
        'ok': True,
        'fetched_at': fetched_at,
        'batch_id': batch_id,
        'batch_created': created,
        'target_issue_no': snapshot.target_issue_no,
        'model_count': len(snapshot.models),
        'invalid_model_count': snapshot.invalid_model_count,
        'upstream_published_at': snapshot.upstream_published_at,
        'model_records': model_records
    }


def refresh_catalog(db, source: dict) -> dict:
    """刷新模型目录（不自动开放任何模型）。有独立目录接口的插件（如 yu28 算法大厅）走目录接口，
    其余插件从一次采集快照提取目录。"""
    source_id = int(source['id'])
    plugin = get_plugin(source.get('plugin_key'))
    if hasattr(plugin, 'fetch_catalog'):
        fetched_at = get_current_utc_time_str()
        try:
            records = plugin.fetch_catalog(source.get('base_url'), api_key=source.get('api_key'))
        except ExternalSourceError as exc:
            return {'ok': False, 'error': str(exc), 'fetched_at': fetched_at}
        summary = db.upsert_external_models(source_id, records, fetched_at)
        db.update_external_source(source_id, {
            'last_attempt_at': fetched_at,
            'last_success_at': fetched_at,
            'last_error': None,
            'consecutive_failures': 0
        })
        return {
            'ok': True,
            'fetched_at': fetched_at,
            'catalog': summary,
            'target_issue_no': None,
            'model_count': summary.get('total', 0),
            'invalid_model_count': 0
        }
    result = collect_source(db, source)
    if not result.get('ok'):
        return result
    summary = db.upsert_external_models(
        source_id,
        result.get('model_records') or [],
        result.get('fetched_at')
    )
    return {**result, 'catalog': summary}


def _models_from_raw_payload(db, batch: dict) -> list[dict]:
    """解压批次原始载荷并返回标准化模型决策列表。"""
    raw_payload = batch.get('raw_payload') or b''
    if isinstance(raw_payload, str):
        try:
            raw_payload = raw_payload.encode('latin-1')
        except UnicodeEncodeError:
            raw_payload = raw_payload.encode('utf-8', errors='ignore')
    try:
        raw_data = json.loads(zlib.decompress(bytes(raw_payload)).decode('utf-8'))
    except (zlib.error, UnicodeDecodeError, json.JSONDecodeError):
        return []
    plugin = None
    try:
        plugin = get_plugin(_source_plugin_key(db, batch.get('source_id')))
    except ExternalSourceError:
        plugin = None
    if plugin is None:
        return []
    models = []
    for model_raw in raw_data.get('models') or []:
        parsed = plugin.parse_model(model_raw)
        if parsed is None:
            continue
        models.append({
            'model_key': parsed.model_key,
            'display_name': parsed.display_name,
            'targets': dict(parsed.targets),
            'confidence': parsed.confidence,
            'raw': parsed.raw,
            'upstream_published_at': batch.get('upstream_published_at'),
            'fetched_at': batch.get('fetched_at'),
            'fingerprint': batch.get('fingerprint')
        })
    return models


def _source_plugin_key(db, source_id) -> str:
    source = db.get_external_source(int(source_id or 0)) or {}
    return str(source.get('plugin_key') or '')


def resolve_next_issue_no(db) -> str | None:
    """本地认可的下一期：最新已开奖期号 + 1。"""
    draws = db.get_recent_draws('pc28', limit=1)
    if not draws:
        return None
    return next_issue_no(draws[0].get('issue_no'))


def adopt_prediction_for_predictor(db, predictor: dict, next_issue_no_value: str | None = None) -> dict:
    """
    为单个外部方案生成本期 predictions 行（首个有效版本固定，不被后续版本覆盖）。

    返回 {'status': 'created'|'exists'|'waiting'|'skipped', 'issue_no': ..., 'reason': ...}
    """
    source_id = int(predictor.get('external_source_id') or 0)
    model_key = str(predictor.get('external_model_key') or '').strip()
    if source_id <= 0 or not model_key:
        return {'status': 'skipped', 'reason': '方案未绑定外部来源或模型'}

    source = db.get_external_source(source_id)
    if not source or not source.get('enabled'):
        return {'status': 'skipped', 'reason': '外部来源已停用'}
    model = db.get_external_model(source_id, model_key)
    if not model or not model.get('enabled'):
        return {'status': 'skipped', 'reason': '模型未开放或已下架'}

    issue_no = str(next_issue_no_value or '').strip()
    if not issue_no:
        return {'status': 'waiting', 'reason': '本地下一期期号尚未确定'}

    existing = db.get_prediction_by_issue(predictor['id'], issue_no)
    if existing:
        return {'status': 'exists', 'issue_no': issue_no}

    draw = db.get_draw_by_issue('pc28', issue_no)
    if draw:
        return {'status': 'skipped', 'issue_no': issue_no, 'reason': '该期已开奖'}

    batches = db.get_external_batches(source_id, issue_no=issue_no, limit=5, ascending=True)
    if not batches:
        return {'status': 'waiting', 'issue_no': issue_no, 'reason': '来源尚未发布该期预测'}

    # 依次检查各版本批次：首个包含所选模型的版本即采用版本（上游修订/绑定变更产生的后续版本兜底）
    batch = None
    model_snapshot = None
    for candidate in batches:
        items = _models_from_raw_payload(db, candidate)
        found = next((item for item in items if item['model_key'] == model_key), None)
        if found:
            batch = candidate
            model_snapshot = found
            break
    if not batch or not model_snapshot:
        return {'status': 'skipped', 'issue_no': issue_no, 'reason': '各版本批次中均不存在所选模型'}

    targets = {key: value for key, value in model_snapshot['targets'].items() if value}
    if not targets:
        return {'status': 'skipped', 'issue_no': issue_no, 'reason': '该模型未提供支持的目标值'}

    requested_targets = [
        target for target in _parse_targets(predictor.get('prediction_targets'))
        if target in targets
    ]
    if not requested_targets:
        return {'status': 'skipped', 'issue_no': issue_no, 'reason': '方案目标与模型提供目标无交集'}

    fingerprint = str(model_snapshot.get('fingerprint') or batch.get('fingerprint') or '')
    db.insert_external_prediction({
        'batch_id': batch['id'],
        'source_id': source_id,
        'model_key': model_key,
        'lottery_type': 'pc28',
        'issue_no': issue_no,
        'upstream_published_at': model_snapshot.get('upstream_published_at'),
        'fetched_at': model_snapshot.get('fetched_at') or batch.get('fetched_at'),
        'prediction_big_small': targets.get('big_small'),
        'prediction_odd_even': targets.get('odd_even'),
        'prediction_combo': targets.get('combo'),
        'confidence': model_snapshot.get('confidence'),
        'model_payload': json.dumps(model_snapshot.get('raw') or {}, ensure_ascii=False),
        'fingerprint': fingerprint
    })

    source_name = str(source.get('name') or '').strip() or '外部来源'
    model_name = str(model.get('display_name') or model_key).strip()
    payload = {
        'predictor_id': predictor['id'],
        'lottery_type': 'pc28',
        'issue_no': issue_no,
        'requested_targets': requested_targets,
        'prediction_big_small': targets.get('big_small') if 'big_small' in requested_targets else None,
        'prediction_odd_even': targets.get('odd_even') if 'odd_even' in requested_targets else None,
        'prediction_combo': targets.get('combo') if 'combo' in requested_targets else None,
        'confidence': model_snapshot.get('confidence'),
        'reasoning_summary': f'外部来源 {source_name}·{model_name}（期号 {issue_no}）',
        'raw_response': json.dumps(model_snapshot.get('raw') or {}, ensure_ascii=False),
        'prompt_snapshot': None,
        'status': 'pending',
        'external_source_id': source_id,
        'external_model_key': model_key,
        'external_fetched_at': model_snapshot.get('fetched_at') or batch.get('fetched_at')
    }
    db.upsert_prediction(payload)
    return {'status': 'created', 'issue_no': issue_no}


def prepare_external_settlement_payload(db, prediction: dict, draw: dict) -> dict | None:
    """
    外部方案结算前的回溯资格校验：采集时间（UTC）晚于该期开奖时间（open_time 为北京时间）
    时，标记 expired 并返回 payload；正常时返回 None（由通用结算逻辑继续）。
    """
    if not is_external_prediction_row(prediction):
        return None
    fetched_at = parse_iso_timestamp((prediction or {}).get('external_fetched_at'))
    open_time = parse_iso_timestamp((draw or {}).get('open_time'))
    if fetched_at and open_time:
        open_time_utc = open_time - BEIJING_OFFSET
        if fetched_at >= open_time_utc:
            return {
                **prediction,
                'status': 'expired',
                'error_message': '采集时该期已开奖，不计入实时统计',
                'settled_at': get_current_utc_time_str()
            }
    return None


def is_external_prediction_row(prediction: dict | None) -> bool:
    return bool((prediction or {}).get('external_source_id'))


def evaluate_external_export_validity(db, predictor: dict, prediction: dict | None) -> bool:
    """external execution 导出的当期有效性门槛；不满足时返回 False（导出 items: []）。"""
    if not prediction:
        return False
    if str(prediction.get('status') or '') != 'pending':
        return False
    issue_no = str(prediction.get('issue_no') or '').strip()
    if not issue_no:
        return False
    if db.get_draw_by_issue('pc28', issue_no):
        return False
    next_issue = resolve_next_issue_no(db)
    if not next_issue or issue_no != next_issue:
        return False
    if not predictor.get('enabled'):
        return False
    source = db.get_external_source(int(predictor.get('external_source_id') or 0))
    if not source or not source.get('enabled'):
        return False
    model = db.get_external_model(source['id'], str(predictor.get('external_model_key') or ''))
    if not model or not model.get('enabled'):
        return False
    return True


def cleanup_expired_batches(db) -> int:
    """删除超过原始载荷保留期的批次。"""
    cutoff_dt = datetime.utcnow() - timedelta(days=config.EXTERNAL_SOURCE_RAW_RETENTION_DAYS)
    return db.cleanup_external_batches_before(cutoff_dt.strftime('%Y-%m-%d %H:%M:%S'))


def _parse_targets(value) -> list[str]:
    if isinstance(value, list):
        return [str(item or '').strip() for item in value if str(item or '').strip()]
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(item or '').strip() for item in parsed if str(item or '').strip()]
    except (TypeError, json.JSONDecodeError):
        pass
    return []
