from __future__ import annotations

import json
import os
import threading
import time
import uuid
import math
from datetime import datetime

import requests
from flask import Flask, jsonify, redirect, render_template, request, url_for, has_request_context
from flask_cors import CORS

import config
from ai_trader import AIPredictor
from database import Database
from lotteries.registry import (
    get_lottery_definition,
    get_target_label,
    list_lottery_catalog,
    normalize_lottery_type,
    normalize_prediction_targets,
    normalize_primary_metric as normalize_lottery_primary_metric,
    normalize_profit_metric as normalize_lottery_profit_metric,
    normalize_profit_rule as normalize_lottery_profit_rule,
    supports_profit_simulation,
    supports_prompt_assistant,
    supports_public_pages
)
from services.jingcai_football_service import JingcaiFootballService
from services.algorithm_backtester import backtest_jingcai_user_algorithm
from services.algorithm_chat_service import generate_algorithm_draft
from services.consensus_analysis_service import build_consensus_analysis, build_export_envelope
from services.consensus_chat_service import chat_consensus_analysis
from services.algorithm_definition_validator import validate_algorithm_definition
from services.algorithm_executor import predict_jingcai_with_user_algorithm
from services.algorithm_templates import (
    apply_algorithm_adjustment,
    build_backtest_adjustment_suggestions,
    list_algorithm_templates
)
from services.bet_strategy import BET_MODE_LABELS, build_bet_strategy, build_bet_strategy_label
from services.lottery_runtime import LotteryRuntime
from services.notification_rule_engine import NotificationRuleEngine, PERFORMANCE_EVENT_TYPE
from services.notification_service import NotificationService
from services.pc28_service import PC28Service
from services.profit_simulator import DEFAULT_ODDS_PROFILE, ProfitSimulator
from services.prediction_engine import PredictionEngine
from services.prediction_guard import PredictionGuardService
from utils import jingcai_football as football_utils
from utils.prompt_assistant import analyze_prompt, build_external_prompt_template, build_optimizer_prompt, get_prompt_placeholder_catalog
from utils.auth import (
    admin_required,
    clear_current_user,
    get_current_user_id,
    get_current_user_is_admin,
    hash_password,
    login_required,
    set_current_user_with_role,
    verify_password
)
from utils.pc28 import (
    DEFAULT_PROFIT_RULE_ID,
    derive_double_group,
    derive_kill_group,
    mask_api_key,
    normalize_api_mode,
    normalize_injection_mode,
    normalize_share_level
)
from utils.predictor_engine import (
    get_user_algorithm_id,
    get_algorithm_label,
    get_algorithm_description,
    get_engine_type_label,
    is_user_algorithm_key,
    normalize_algorithm_key,
    normalize_engine_type,
    resolve_execution_description,
    resolve_execution_label
)
from utils.timezone import get_current_beijing_time_str, utc_to_beijing
from utils.logger import get_logger


app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'pc28-predictor-secret')
CORS(app, supports_credentials=True)

APP_VERSION = str(int(time.time()))
MAX_USER_ALGORITHMS_PER_USER = int(getattr(config, 'MAX_USER_ALGORITHMS_PER_USER', 50))
USER_ALGORITHM_BACKTEST_COOLDOWN_SECONDS = int(getattr(config, 'USER_ALGORITHM_BACKTEST_COOLDOWN_SECONDS', 0))
_user_algorithm_backtest_last_run_at: dict[int, float] = {}
PUBLIC_LOTTERY_PAGE_CONFIG = {
    'pc28': {
        'slug': 'pc28',
        'title': '加拿大28 AI 预测平台',
        'heading': '加拿大28 AI 预测',
        'description': '查看加拿大28最新开奖、公开方案榜单与命中表现，面向单点、大/小、单/双和组合投注做公开展示。'
    },
    'jingcai_football': {
        'slug': 'jingcai-football',
        'title': '竞彩足球 AI 预测平台',
        'heading': '竞彩足球 AI 预测',
        'description': '查看竞彩足球当前批次、公开方案榜单与胜平负 / 让球胜平负近期表现，适合作为独立公开落地页。'
    }
}

db = Database(config.DATABASE_PATH)
prediction_guard = PredictionGuardService(db)
notification_service = NotificationService(db)
notification_rule_engine = NotificationRuleEngine(db)
pc28_service = PC28Service()
jingcai_football_service = JingcaiFootballService(prediction_guard=prediction_guard, notification_service=notification_service)
prediction_engine = PredictionEngine(
    db,
    pc28_service,
    prediction_guard=prediction_guard,
    notification_service=notification_service,
    notification_rule_engine=notification_rule_engine
)
lottery_runtime = LotteryRuntime(
    db,
    prediction_engine,
    {
        'jingcai_football': jingcai_football_service
    }
)
profit_simulator = ProfitSimulator(db)

_init_lock = threading.Lock()
_app_initialized = False
_scheduler_started = False
_notification_worker_started = False
_scheduler_owner_id = f'{os.getpid()}-{uuid.uuid4().hex}'
AUTO_PREDICTION_SCHEDULER = 'lottery-auto-prediction'
JINGCAI_HISTORY_BACKFILL_SCHEDULER = 'jingcai-history-backfill'
NOTIFICATION_DELIVERY_SCHEDULER = 'notification-delivery-worker'
NOTIFICATION_CHANNEL_LABELS = {
    'telegram': 'Telegram'
}
NOTIFICATION_STATUS_LABELS = {
    'active': '启用',
    'disabled': '停用'
}
NOTIFICATION_EVENT_LABELS = {
    'prediction_created': '预测生成',
    PERFORMANCE_EVENT_TYPE: '表现告警'
}
NOTIFICATION_DELIVERY_MODE_LABELS = {
    'notify_only': '仅通知',
    'follow_bet': '通知 + 下注策略'
}
NOTIFICATION_SENDER_MODE_LABELS = {
    'platform': '平台机器人',
    'user_sender': '我的机器人'
}
runtime_logger = get_logger('runtime')


@app.context_processor
def inject_version():
    return {'app_version': APP_VERSION}


@app.after_request
def after_request(response):
    if request.endpoint == 'static':
        response.headers['Cache-Control'] = 'public, max-age=300'
    elif request.path.startswith('/api/'):
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    elif response.content_type.startswith('text/html'):
        response.headers['Cache-Control'] = 'no-cache, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
    return response


def initialize_application():
    global _app_initialized, _scheduler_started, _notification_worker_started

    with _init_lock:
        if not _app_initialized:
            db.init_db()
            _app_initialized = True

        if config.AUTO_PREDICTION and not _scheduler_started:
            scheduler_thread = threading.Thread(target=prediction_loop, daemon=True)
            scheduler_thread.start()
            _scheduler_started = True

        if config.NOTIFICATION_WORKER_ENABLED and not _notification_worker_started:
            notification_thread = threading.Thread(target=notification_delivery_loop, daemon=True)
            notification_thread.start()
            _notification_worker_started = True


def _log_runtime_event(level: str, event: str, **fields):
    payload = {
        'event': event,
        'time_beijing': get_current_beijing_time_str(),
        **fields
    }
    getattr(runtime_logger, level)(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _snapshot_pc28_draw_state() -> dict:
    recent_draws = db.get_recent_draws('pc28', limit=2)
    latest_draw = recent_draws[0] if recent_draws else None
    previous_draw = recent_draws[1] if len(recent_draws) > 1 else None
    next_issue = None
    latest_issue = str((latest_draw or {}).get('issue_no') or '').strip()
    if latest_issue.isdigit():
        next_issue = str(int(latest_issue) + 1).zfill(len(latest_issue))
    return {
        'latest_draw_issue_no': latest_draw.get('issue_no') if latest_draw else None,
        'latest_draw_open_time': latest_draw.get('open_time') if latest_draw else None,
        'previous_draw_issue_no': previous_draw.get('issue_no') if previous_draw else None,
        'next_issue_guess': next_issue
    }


def _run_scheduled_jingcai_history_backfill() -> dict | None:
    if not bool(getattr(config, 'JINGCAI_HISTORY_BACKFILL_ENABLED', True)):
        return None

    lock_result = db.try_acquire_scheduler_with_details(
        JINGCAI_HISTORY_BACKFILL_SCHEDULER,
        _scheduler_owner_id,
        stale_after_seconds=max(int(getattr(config, 'JINGCAI_HISTORY_BACKFILL_INTERVAL_SECONDS', 86400)), 3600)
    )
    if not lock_result.get('acquired'):
        return {
            'skipped': True,
            'reason': 'lock_not_acquired',
            'current_owner_id': lock_result.get('current_owner_id'),
            'error': lock_result.get('error')
        }

    db.heartbeat_scheduler(JINGCAI_HISTORY_BACKFILL_SCHEDULER, _scheduler_owner_id)
    result = jingcai_football_service.run_scheduled_history_backfill(
        db,
        lookback_days=max(1, int(getattr(config, 'JINGCAI_HISTORY_BACKFILL_LOOKBACK_DAYS', 7))),
        include_details=bool(getattr(config, 'JINGCAI_HISTORY_BACKFILL_INCLUDE_DETAILS', True)),
        max_days=max(1, int(getattr(config, 'JINGCAI_HISTORY_BACKFILL_MAX_DAYS', 31)))
    )
    db.heartbeat_scheduler(JINGCAI_HISTORY_BACKFILL_SCHEDULER, _scheduler_owner_id)
    return result


def prediction_loop():
    _log_runtime_event(
        'info',
        'prediction_loop_started',
        process_id=os.getpid(),
        scheduler_name=AUTO_PREDICTION_SCHEDULER
    )
    scheduler_name = AUTO_PREDICTION_SCHEDULER
    stale_after_seconds = max(config.PREDICTION_POLL_INTERVAL * 3, 60)
    last_cycle_at = {
        'pc28': 0.0,
        'jingcai_football': 0.0
    }
    last_retention_maintenance_at = 0.0
    last_vacuum_at = 0.0
    last_jingcai_backfill_at = 0.0

    while config.AUTO_PREDICTION:
        try:
            loop_started_at = time.monotonic()
            lock_result = db.try_acquire_scheduler_with_details(
                scheduler_name,
                _scheduler_owner_id,
                stale_after_seconds=stale_after_seconds
            )
            acquired = bool(lock_result.get('acquired'))
            if acquired:
                db.heartbeat_scheduler(scheduler_name, _scheduler_owner_id)
                now_monotonic = time.monotonic()
                total_settled = 0
                total_predictions = []
                pc28_cycle_summary = {
                    'ran': False,
                    'duration_ms': 0,
                    'settled_count': 0,
                    'prediction_count': 0
                }
                jingcai_cycle_summary = {
                    'ran': False,
                    'duration_ms': 0,
                    'settled_count': 0,
                    'prediction_count': 0
                }

                if now_monotonic - last_cycle_at['pc28'] >= max(config.PREDICTION_POLL_INTERVAL, 5):
                    pc28_started_at = time.monotonic()
                    pc28_result = lottery_runtime.run_pc28_cycle()
                    pc28_snapshot = _snapshot_pc28_draw_state()
                    total_settled += int(pc28_result.get('settled_count') or 0)
                    total_predictions.extend(pc28_result.get('predictions') or [])
                    last_cycle_at['pc28'] = now_monotonic
                    pc28_cycle_summary = {
                        'ran': True,
                        'duration_ms': int((time.monotonic() - pc28_started_at) * 1000),
                        'settled_count': int(pc28_result.get('settled_count') or 0),
                        'prediction_count': len(pc28_result.get('predictions') or []),
                        **pc28_snapshot
                    }
                    _log_runtime_event(
                        'info',
                        'pc28_cycle_completed',
                        scheduler_name=scheduler_name,
                        owner_id=_scheduler_owner_id,
                        **pc28_cycle_summary
                    )

                jingcai_plan = jingcai_football_service.get_scheduler_plan(db)
                if now_monotonic - last_cycle_at['jingcai_football'] >= max(int(jingcai_plan.get('interval_seconds') or 0), 5):
                    jingcai_started_at = time.monotonic()
                    jingcai_result = lottery_runtime.run_lottery_cycle('jingcai_football')
                    total_settled += int(jingcai_result.get('settled_count') or 0)
                    total_predictions.extend(jingcai_result.get('predictions') or [])
                    last_cycle_at['jingcai_football'] = now_monotonic
                    jingcai_cycle_summary = {
                        'ran': True,
                        'duration_ms': int((time.monotonic() - jingcai_started_at) * 1000),
                        'settled_count': int(jingcai_result.get('settled_count') or 0),
                        'prediction_count': len(jingcai_result.get('predictions') or []),
                        'mode': jingcai_plan.get('mode'),
                        'interval_seconds': int(jingcai_plan.get('interval_seconds') or 0)
                    }
                    _log_runtime_event(
                        'info',
                        'jingcai_cycle_completed',
                        scheduler_name=scheduler_name,
                        owner_id=_scheduler_owner_id,
                        **jingcai_cycle_summary
                    )

                result = {
                    'settled_count': total_settled,
                    'predictions': total_predictions
                }

                maintenance_interval = max(300, int(config.PC28_ARCHIVE_MAINTENANCE_INTERVAL))
                if now_monotonic - last_retention_maintenance_at >= maintenance_interval:
                    maintenance_result = db.run_pc28_data_retention_maintenance(
                        config.PC28_PREDICTION_RETENTION_DAYS,
                        config.PC28_DRAW_RETENTION_DAYS
                    )
                    last_retention_maintenance_at = now_monotonic

                    deleted_prediction_rows = int(maintenance_result.get('deleted_prediction_rows') or 0)
                    deleted_draw_rows = int(maintenance_result.get('deleted_draw_rows') or 0)
                    if deleted_prediction_rows or deleted_draw_rows:
                        _log_runtime_event(
                            'info',
                            'pc28_retention_maintenance',
                            scheduler_name=scheduler_name,
                            owner_id=_scheduler_owner_id,
                            deleted_prediction_rows=deleted_prediction_rows,
                            deleted_draw_rows=deleted_draw_rows,
                            prediction_cutoff_date=maintenance_result.get('prediction_cutoff_date'),
                            draw_cutoff_date=maintenance_result.get('draw_cutoff_date')
                        )
                        vacuum_interval = max(3600, int(config.PC28_ARCHIVE_VACUUM_INTERVAL))
                        if now_monotonic - last_vacuum_at >= vacuum_interval:
                            db.vacuum()
                            last_vacuum_at = now_monotonic
                            _log_runtime_event(
                                'info',
                                'pc28_retention_vacuum_completed',
                                scheduler_name=scheduler_name,
                                owner_id=_scheduler_owner_id
                            )

                backfill_interval = max(3600, int(getattr(config, 'JINGCAI_HISTORY_BACKFILL_INTERVAL_SECONDS', 86400)))
                if now_monotonic - last_jingcai_backfill_at >= backfill_interval:
                    backfill_started_at = time.monotonic()
                    try:
                        backfill_result = _run_scheduled_jingcai_history_backfill()
                        if backfill_result:
                            result_payload = backfill_result.get('result') or {}
                            _log_runtime_event(
                                'info',
                                'jingcai_history_backfill_completed',
                                scheduler_name=JINGCAI_HISTORY_BACKFILL_SCHEDULER,
                                owner_id=_scheduler_owner_id,
                                skipped=bool(backfill_result.get('skipped')),
                                match_count=int(result_payload.get('match_count') or 0),
                                detail_count=int(result_payload.get('detail_count') or 0),
                                duration_ms=int((time.monotonic() - backfill_started_at) * 1000)
                            )
                    except Exception as exc:
                        _log_runtime_event(
                            'warning',
                            'jingcai_history_backfill_failed',
                            scheduler_name=JINGCAI_HISTORY_BACKFILL_SCHEDULER,
                            owner_id=_scheduler_owner_id,
                            error=str(exc),
                            duration_ms=int((time.monotonic() - backfill_started_at) * 1000)
                        )
                    finally:
                        last_jingcai_backfill_at = now_monotonic

                db.heartbeat_scheduler(scheduler_name, _scheduler_owner_id)
                total_duration_ms = int((time.monotonic() - loop_started_at) * 1000)
                cycle_level = 'warning' if total_duration_ms >= config.PREDICTION_POLL_INTERVAL * 1000 else 'info'
                _log_runtime_event(
                    cycle_level,
                    'scheduler_cycle_completed',
                    scheduler_name=scheduler_name,
                    owner_id=_scheduler_owner_id,
                    settled_count=result['settled_count'],
                    prediction_count=len(result['predictions']),
                    total_duration_ms=total_duration_ms,
                    pc28_ran=pc28_cycle_summary.get('ran'),
                    pc28_duration_ms=pc28_cycle_summary.get('duration_ms'),
                    pc28_latest_draw_issue_no=pc28_cycle_summary.get('latest_draw_issue_no'),
                    pc28_next_issue_guess=pc28_cycle_summary.get('next_issue_guess'),
                    jingcai_ran=jingcai_cycle_summary.get('ran'),
                    jingcai_duration_ms=jingcai_cycle_summary.get('duration_ms'),
                    jingcai_mode=jingcai_plan.get('mode'),
                    jingcai_interval_seconds=int(jingcai_plan.get('interval_seconds') or 0)
                )
            else:
                log_level = 'warning' if lock_result.get('error') else 'debug'
                _log_runtime_event(
                    log_level,
                    'scheduler_lock_skipped',
                    scheduler_name=scheduler_name,
                    owner_id=_scheduler_owner_id,
                    current_owner_id=lock_result.get('current_owner_id'),
                    current_heartbeat_at=lock_result.get('current_heartbeat_at'),
                    is_stale=lock_result.get('is_stale'),
                    error=lock_result.get('error')
                )

            loop_tick_seconds = max(5, min(config.PREDICTION_POLL_INTERVAL, config.JINGCAI_NEAR_MATCH_INTERVAL))
            time.sleep(loop_tick_seconds)
        except Exception as exc:
            runtime_logger.exception(
                json.dumps(
                    {
                        'event': 'prediction_loop_error',
                        'time_beijing': get_current_beijing_time_str(),
                        'scheduler_name': scheduler_name,
                        'owner_id': _scheduler_owner_id,
                        'error': str(exc)
                    },
                    ensure_ascii=False,
                    sort_keys=True
                )
            )
            time.sleep(10)


def notification_delivery_loop():
    _log_runtime_event(
        'info',
        'notification_loop_started',
        process_id=os.getpid(),
        scheduler_name=NOTIFICATION_DELIVERY_SCHEDULER
    )
    scheduler_name = NOTIFICATION_DELIVERY_SCHEDULER
    stale_after_seconds = max(int(config.NOTIFICATION_RETRY_MAX_SECONDS), 60)
    while True:
        try:
            lock_result = db.try_acquire_scheduler_with_details(
                scheduler_name,
                _scheduler_owner_id,
                stale_after_seconds=stale_after_seconds
            )
            acquired = bool(lock_result.get('acquired'))
            if acquired:
                db.heartbeat_scheduler(scheduler_name, _scheduler_owner_id)
                result = notification_service.process_delivery_jobs(limit=config.NOTIFICATION_WORKER_BATCH_SIZE)
                db.heartbeat_scheduler(scheduler_name, _scheduler_owner_id)
                if int(result.get('processed_count') or 0) > 0:
                    _log_runtime_event(
                        'info',
                        'notification_delivery_completed',
                        scheduler_name=scheduler_name,
                        owner_id=_scheduler_owner_id,
                        processed_count=int(result.get('processed_count') or 0),
                        delivered_count=int(result.get('delivered_count') or 0),
                        retrying_count=int(result.get('retrying_count') or 0),
                        failed_count=int(result.get('failed_count') or 0)
                    )
            time.sleep(max(0.2, float(config.NOTIFICATION_WORKER_POLL_INTERVAL)))
        except Exception as exc:
            runtime_logger.exception(
                json.dumps(
                    {
                        'event': 'notification_loop_error',
                        'time_beijing': get_current_beijing_time_str(),
                        'scheduler_name': scheduler_name,
                        'owner_id': _scheduler_owner_id,
                        'error': str(exc)
                    },
                    ensure_ascii=False,
                    sort_keys=True
                )
            )
            time.sleep(2)


def _resolve_predictor_runtime_status(predictor: dict) -> tuple[str, str]:
    enabled = bool(predictor.get('enabled'))
    auto_paused = bool(predictor.get('auto_paused'))
    if not enabled:
        return 'manual_disabled', '手动停用'
    if auto_paused:
        return 'auto_paused', '自动暂停'
    return 'enabled', '启用中'


def _serialize_predictor(predictor: dict) -> dict:
    lottery_type = normalize_lottery_type(predictor.get('lottery_type'))
    lottery_definition = get_lottery_definition(lottery_type)
    share_level = predictor.get('share_level') or ('records' if predictor.get('share_predictions') else 'stats_only')
    public_links = _build_public_links(predictor['id'])
    simulation_metrics = profit_simulator.get_metric_options(predictor) if supports_profit_simulation(lottery_type) else []
    default_simulation_metric = profit_simulator.get_default_metric(predictor) if supports_profit_simulation(lottery_type) else None
    default_profit_rule_id = profit_simulator.get_default_rule_id(predictor) if supports_profit_simulation(lottery_type) else ''
    default_profit_period_key = profit_simulator.get_default_period_key(predictor) if supports_profit_simulation(lottery_type) else ''
    runtime_status, runtime_status_label = _resolve_predictor_runtime_status(predictor)
    engine_type = normalize_engine_type(predictor.get('engine_type'))
    algorithm_key = normalize_algorithm_key(lottery_type, engine_type, predictor.get('algorithm_key'))
    algorithm_label = get_algorithm_label(lottery_type, engine_type, algorithm_key)
    algorithm_description = get_algorithm_description(lottery_type, engine_type, algorithm_key)
    user_algorithm = predictor.get('user_algorithm') if is_user_algorithm_key(algorithm_key) else None
    if user_algorithm:
        algorithm_label = user_algorithm.get('name') or algorithm_label
        algorithm_description = user_algorithm.get('description') or algorithm_description
    data = {
        'id': predictor['id'],
        'user_id': predictor['user_id'],
        'name': predictor['name'],
        'lottery_type': lottery_type,
        'lottery_label': lottery_definition.label,
        'api_url': predictor['api_url'],
        'model_name': predictor['model_name'],
        'engine_type': engine_type,
        'engine_type_label': get_engine_type_label(engine_type),
        'algorithm_key': algorithm_key,
        'algorithm_source': 'user' if is_user_algorithm_key(algorithm_key) else ('builtin' if engine_type == 'machine' else ''),
        'user_algorithm_id': get_user_algorithm_id(algorithm_key) if is_user_algorithm_key(algorithm_key) else None,
        'user_algorithm_name': user_algorithm.get('name') if user_algorithm else '',
        'user_algorithm_status': user_algorithm.get('status') if user_algorithm else '',
        'algorithm_label': algorithm_label,
        'algorithm_description': algorithm_description,
        'execution_label': resolve_execution_label({
            **predictor,
            'engine_type': engine_type,
            'algorithm_key': algorithm_key,
            'algorithm_label': algorithm_label
        }),
        'execution_description': resolve_execution_description({
            **predictor,
            'engine_type': engine_type,
            'algorithm_key': algorithm_key,
            'algorithm_label': algorithm_label
        }),
        'api_mode': predictor.get('api_mode') or 'auto',
        'primary_metric': predictor.get('primary_metric') or (lottery_definition.primary_metric_options[0][0] if lottery_definition.primary_metric_options else ''),
        'primary_metric_label': get_target_label(lottery_type, predictor.get('primary_metric') or ''),
        'profit_default_metric': predictor.get('profit_default_metric') or default_simulation_metric,
        'profit_rule_id': predictor.get('profit_rule_id') or default_profit_rule_id,
        'profit_rule_label': profit_simulator.get_rule_label(predictor.get('profit_rule_id') or default_profit_rule_id, predictor) if supports_profit_simulation(lottery_type) else '',
        'share_level': share_level,
        'share_level_label': _share_level_label(share_level),
        'share_predictions': share_level != 'stats_only',
        'prediction_method': predictor.get('prediction_method') or '',
        'system_prompt': predictor.get('system_prompt') or '',
        'data_injection_mode': predictor.get('data_injection_mode') or 'summary',
        'user_algorithm_fallback_strategy': predictor.get('user_algorithm_fallback_strategy') or 'fail',
        'prediction_targets': normalize_prediction_targets(lottery_type, predictor.get('prediction_targets')),
        'target_options': lottery_definition.to_catalog_item()['target_options'],
        'primary_metric_options': lottery_definition.to_catalog_item()['primary_metric_options'],
        'capabilities': lottery_definition.to_catalog_item()['capabilities'],
        'simulation_metrics': simulation_metrics,
        'default_simulation_metric': default_simulation_metric,
        'profit_rule_options': profit_simulator.get_rule_options(predictor) if supports_profit_simulation(lottery_type) else [],
        'profit_period_options': profit_simulator.get_period_options(predictor) if supports_profit_simulation(lottery_type) else [],
        'default_profit_period_key': default_profit_period_key,
        'odds_profiles': profit_simulator.get_odds_profile_options(predictor) if supports_profit_simulation(lottery_type) else [],
        'history_window': predictor.get('history_window'),
        'temperature': predictor.get('temperature'),
        'enabled': bool(predictor.get('enabled')),
        'auto_paused': bool(predictor.get('auto_paused')),
        'runtime_status': runtime_status,
        'runtime_status_label': runtime_status_label,
        'can_auto_run': bool(predictor.get('enabled')) and not bool(predictor.get('auto_paused')),
        'consecutive_ai_failures': int(predictor.get('consecutive_ai_failures') or 0),
        'auto_paused_at': utc_to_beijing(predictor['auto_paused_at']) if predictor.get('auto_paused_at') else None,
        'auto_pause_reason': predictor.get('auto_pause_reason'),
        'last_ai_error_category': predictor.get('last_ai_error_category'),
        'last_ai_error_message': predictor.get('last_ai_error_message'),
        'last_ai_error_at': utc_to_beijing(predictor['last_ai_error_at']) if predictor.get('last_ai_error_at') else None,
        'created_at': utc_to_beijing(predictor['created_at']) if predictor.get('created_at') else None,
        'updated_at': utc_to_beijing(predictor['updated_at']) if predictor.get('updated_at') else None,
        'masked_api_key': mask_api_key(predictor.get('api_key')),
        'has_api_key': bool(predictor.get('api_key')),
        'public_path': public_links['path'],
        'public_url': public_links['url'],
        'public_page_available': _is_predictor_publicly_available(predictor)
    }
    return data


def _serialize_prediction(prediction: dict, include_raw: bool = True) -> dict:
    lottery_type = normalize_lottery_type(prediction.get('lottery_type'))
    score_values = [
        prediction.get('hit_number'),
        prediction.get('hit_big_small'),
        prediction.get('hit_odd_even'),
        prediction.get('hit_combo')
    ]
    effective_scores = [value for value in score_values if value is not None]

    data = {
        **prediction,
        'requested_target_labels': [get_target_label(lottery_type, item) for item in prediction.get('requested_targets', [])],
        'created_at': utc_to_beijing(prediction['created_at']) if prediction.get('created_at') else None,
        'updated_at': utc_to_beijing(prediction['updated_at']) if prediction.get('updated_at') else None,
        'settled_at': utc_to_beijing(prediction['settled_at']) if prediction.get('settled_at') else None,
        'score_percentage': round(sum(effective_scores) / len(effective_scores) * 100, 2) if effective_scores else None
    }
    if not include_raw:
        data.pop('raw_response', None)
        data.pop('prompt_snapshot', None)
    return data


def _serialize_draw(draw: dict) -> dict:
    return {
        'issue_no': draw['issue_no'],
        'draw_date': draw.get('draw_date'),
        'draw_time': draw.get('draw_time'),
        'open_time': draw.get('open_time'),
        'result_number': draw['result_number'],
        'result_number_text': draw['result_number_text'],
        'big_small': draw['big_small'],
        'odd_even': draw['odd_even'],
        'combo': draw['combo']
    }


def _serialize_overview(overview: dict) -> dict:
    return {
        **overview,
        'latest_draw': _serialize_draw(overview['latest_draw']) if overview.get('latest_draw') else None,
        'recent_draws': [_serialize_draw(draw) for draw in overview.get('recent_draws', [])]
    }


def _serialize_lottery_event(event: dict) -> dict:
    meta_payload = event.get('meta_payload') or {}
    status = football_utils.normalize_match_status_code(
        event.get('status') or event.get('show_sell_status')
    )
    status_label = football_utils.normalize_match_status_label(
        status,
        event.get('status_label') or event.get('show_sell_status_label')
    )
    return {
        **event,
        'issue_no': event.get('issue_no') or event.get('match_no') or meta_payload.get('match_no') or meta_payload.get('match_no_value') or '',
        'status': status,
        'status_label': status_label,
        'show_sell_status': status,
        'show_sell_status_label': status_label,
        'created_at': utc_to_beijing(event['created_at']) if event.get('created_at') else None,
        'updated_at': utc_to_beijing(event['updated_at']) if event.get('updated_at') else None
    }


def _serialize_prediction_item_group(item: dict, include_raw: bool = True) -> dict:
    data = {
        **item,
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None,
        'updated_at': utc_to_beijing(item['updated_at']) if item.get('updated_at') else None,
        'settled_at': utc_to_beijing(item['settled_at']) if item.get('settled_at') else None,
        'predicted_spf_label': item.get('predicted_spf_label') or '--',
        'predicted_rqspf_label': item.get('predicted_rqspf_label') or '--',
        'actual_spf_label': item.get('actual_spf_label') or '--',
        'actual_rqspf_label': item.get('actual_rqspf_label') or '--'
    }
    if not include_raw:
        data.pop('raw_response', None)
        data.pop('run_raw_response', None)
    return data


def _serialize_prediction_run(run: dict | None) -> dict | None:
    if not run:
        return None
    return {
        **run,
        'created_at': utc_to_beijing(run['created_at']) if run.get('created_at') else None,
        'updated_at': utc_to_beijing(run['updated_at']) if run.get('updated_at') else None,
        'settled_at': utc_to_beijing(run['settled_at']) if run.get('settled_at') else None,
        'requested_target_labels': [get_target_label('jingcai_football', item) for item in run.get('requested_targets', [])],
        'items': [_serialize_prediction_item_group(item) for item in run.get('items', [])]
    }


def _build_jingcai_external_prompt_template(predictor_payload: dict) -> str:
    targets = normalize_prediction_targets('jingcai_football', predictor_payload.get('prediction_targets'))
    target_labels = '、'.join(get_target_label('jingcai_football', item) for item in targets)
    return f"""你是一个资深提示词工程师。请为“AITradingSimulator”的竞彩足球预测功能编写一版可直接使用的自定义提示词。

当前方案配置：
- 彩种：竞彩足球
- 预测目标：{target_labels or '胜平负'}
- 主玩法：{get_target_label('jingcai_football', predictor_payload.get('primary_metric') or 'spf')}
- 历史窗口：最近 {predictor_payload.get('history_window') or 60} 场已开奖比赛
- 数据注入模式：{'原始模式' if predictor_payload.get('data_injection_mode') == 'raw' else '摘要模式'}

编写要求：
1. 最终提示词必须面向中国体育彩票竞彩足球，不要写成 PC28 或通用聊天助手。
2. 只能围绕待售比赛、赔率、近期赛果做分析。
3. 输出必须是 JSON，不要 Markdown，不要额外解释。
4. JSON 结构只允许包含：batch_key、predictions，其中 predictions 是数组。
5. 每个 prediction 项必须包含：event_key、match_no、predicted_spf、predicted_rqspf、confidence、reasoning_summary。
6. predicted_spf 和 predicted_rqspf 只能输出 "3"、"1"、"0" 或 null。
7. 不要编造不存在的比赛，不要输出平台未提供的 event_key。
8. 如果某个目标没有足够把握，可以输出 null，但不要整场留空。

请按以下方式回复：
- 第一段：只输出最终可粘贴到项目里的提示词正文。
- 空一行后，第二段：用 2-4 句简短说明你的设计重点。

我当前已有提示词：
{predictor_payload.get('system_prompt') or '（暂无）'}"""


def _serialize_public_prediction(prediction: dict) -> dict:
    if not prediction:
        return None

    data = _serialize_prediction(prediction)
    data.pop('raw_response', None)
    data.pop('prompt_snapshot', None)
    data.pop('error_message', None)
    return data


def _serialize_public_prediction_with_level(prediction: dict, share_level: str) -> dict:
    data = _serialize_public_prediction(prediction)
    if not data:
        return None

    if share_level != 'analysis':
        data['reasoning_summary'] = ''

    return data


def _build_pc28_execution_signal_view(predictor: dict, prediction: dict) -> dict | None:
    if not predictor or not prediction:
        return None
    if normalize_lottery_type(prediction.get('lottery_type')) != 'pc28':
        return None

    published_at = prediction.get('updated_at') or prediction.get('created_at')
    predictor_name = str(predictor.get('name') or '').strip() or f"predictor-{predictor.get('id')}"
    share_level = predictor.get('share_level') or ('records' if predictor.get('share_predictions') else 'stats_only')

    signal_items = []
    mappings = [
        ('big_small', prediction.get('prediction_big_small')),
        ('odd_even', prediction.get('prediction_odd_even')),
        ('combo', prediction.get('prediction_combo')),
    ]
    if prediction.get('prediction_number') is not None:
        mappings.insert(0, ('number', prediction.get('prediction_number')))

    for bet_type, bet_value in mappings:
        if bet_value in {None, ''}:
            continue
        item = {
            'bet_type': bet_type,
            'bet_value': bet_value,
            'confidence': prediction.get('confidence'),
            'normalized_payload': {
                'requested_targets': prediction.get('requested_targets') or [],
                'predictor_id': predictor.get('id'),
                'primary_metric': predictor.get('primary_metric'),
                'share_level': share_level,
                'profit_rule_id': predictor.get('profit_rule_id') or DEFAULT_PROFIT_RULE_ID,
                'odds_profile': DEFAULT_ODDS_PROFILE
            }
        }
        if bet_type != 'number':
            item['message_text'] = f'{bet_value}10'
        signal_items.append(item)

    if not signal_items:
        return None

    issue_no = str(prediction.get('issue_no') or '').strip()
    signal_id = f"pc28-predictor-{predictor.get('id')}-{issue_no}"
    return {
        'schema_version': '1.0',
        'signal_id': signal_id,
        'source_type': 'ai_trading_simulator',
        'source_ref': {
            'platform': 'AITradingSimulator',
            'predictor_id': predictor.get('id'),
            'predictor_name': predictor_name,
            'share_level': share_level
        },
        'lottery_type': 'pc28',
        'issue_no': issue_no,
        'published_at': published_at,
        'signals': signal_items
    }


def _build_pc28_analysis_signal_view(predictor: dict, prediction: dict) -> dict | None:
    if not predictor or not prediction:
        return None
    if normalize_lottery_type(prediction.get('lottery_type')) != 'pc28':
        return None

    published_at = prediction.get('updated_at') or prediction.get('created_at')
    predictor_name = str(predictor.get('name') or '').strip() or f"predictor-{predictor.get('id')}"
    issue_no = str(prediction.get('issue_no') or '').strip()
    signal_id = f"pc28-predictor-{predictor.get('id')}-{issue_no}"
    stats = db.get_predictor_stats(predictor['id'])
    metric_stats = (stats.get('metrics') or {}).get(predictor.get('primary_metric') or 'big_small', {})

    return {
        'schema_version': '1.0',
        'signal_id': signal_id,
        'lottery_type': 'pc28',
        'issue_no': issue_no,
        'published_at': published_at,
        'predictor': {
            'predictor_id': predictor.get('id'),
            'predictor_name': predictor_name,
            'prediction_method': predictor.get('prediction_method') or '自定义策略',
            'prediction_targets': predictor.get('prediction_targets') or [],
            'primary_metric': predictor.get('primary_metric'),
            'history_window': predictor.get('history_window'),
            'profit_rule_id': predictor.get('profit_rule_id')
        },
        'prediction': {
            'prediction_number': prediction.get('prediction_number'),
            'prediction_big_small': prediction.get('prediction_big_small'),
            'prediction_odd_even': prediction.get('prediction_odd_even'),
            'prediction_combo': prediction.get('prediction_combo'),
            'confidence': prediction.get('confidence'),
            'reasoning_summary': prediction.get('reasoning_summary') or ''
        },
        'performance': {
            'recent_20_hit_rate': (metric_stats.get('recent_20') or {}).get('hit_rate'),
            'recent_100_hit_rate': (metric_stats.get('recent_100') or {}).get('hit_rate'),
            'overall_hit_rate': (metric_stats.get('overall') or {}).get('hit_rate'),
            'settled_predictions': stats.get('settled_predictions')
        },
        'context': {
            'history_window': predictor.get('history_window'),
            'primary_metric': predictor.get('primary_metric'),
            'profit_rule_id': predictor.get('profit_rule_id')
        },
        'raw': {
            'prompt_snapshot': prediction.get('prompt_snapshot') or '',
            'raw_response': prediction.get('raw_response') or ''
        }
    }


def _build_pc28_performance_export_view(predictor: dict) -> dict | None:
    if not predictor:
        return None
    if normalize_lottery_type(predictor.get('lottery_type')) != 'pc28':
        return None

    predictor_name = str(predictor.get('name') or '').strip() or f"predictor-{predictor.get('id')}"
    stats = db.get_predictor_stats(predictor['id'])
    metrics = {}
    for metric_key in ['big_small', 'odd_even', 'combo']:
        metric_stats = (stats.get('metrics') or {}).get(metric_key) or {}
        metric_streaks = (stats.get('metric_streaks') or {}).get(metric_key) or {}
        recent_20 = metric_stats.get('recent_20') or {}
        recent_50 = metric_stats.get('recent_50') or {}
        recent_100 = metric_stats.get('recent_100') or {}
        metrics[metric_key] = {
            'label': metric_stats.get('label') or metric_key,
            'recent_20': {
                'hit_rate': recent_20.get('hit_rate'),
                'sample_count': int(recent_20.get('sample_count') or 0),
                'hit_count': int(recent_20.get('hit_count') or 0),
                'ratio_text': recent_20.get('ratio_text') or '--',
            },
            'recent_50': {
                'hit_rate': recent_50.get('hit_rate'),
                'sample_count': int(recent_50.get('sample_count') or 0),
                'hit_count': int(recent_50.get('hit_count') or 0),
                'ratio_text': recent_50.get('ratio_text') or '--',
            },
            'recent_100': {
                'hit_rate': recent_100.get('hit_rate'),
                'sample_count': int(recent_100.get('sample_count') or 0),
                'hit_count': int(recent_100.get('hit_count') or 0),
                'ratio_text': recent_100.get('ratio_text') or '--',
            },
            'streaks': {
                'current_hit_streak': int(metric_streaks.get('current_hit_streak') or 0),
                'current_miss_streak': int(metric_streaks.get('current_miss_streak') or 0),
                'recent_100_max_hit_streak': int(metric_streaks.get('recent_100_max_hit_streak') or 0),
                'recent_100_max_miss_streak': int(metric_streaks.get('recent_100_max_miss_streak') or 0),
                'historical_max_hit_streak': int(metric_streaks.get('historical_max_hit_streak') or 0),
                'historical_max_miss_streak': int(metric_streaks.get('historical_max_miss_streak') or 0),
            },
        }

    return {
        'schema_version': '1.0',
        'predictor_id': predictor.get('id'),
        'predictor_name': predictor_name,
        'lottery_type': 'pc28',
        'latest_settled_issue': stats.get('latest_settled_issue') or '',
        'settled_predictions': int(stats.get('settled_predictions') or 0),
        'metrics': metrics,
    }


def _serialize_profit_simulation(simulation: dict, include_records: bool = True) -> dict:
    payload = dict(simulation)
    if not include_records:
        payload['records'] = []
    return payload


def _format_jingcai_simulation_outcome(metric: str, outcome: str, meta_payload: dict) -> str:
    if not outcome:
        return '--'
    if metric != 'rqspf':
        return str(outcome)
    handicap_text = str(((meta_payload.get('rqspf') or {}).get('handicap_text') or '')).strip()
    if not handicap_text:
        return str(outcome)
    return f'{outcome}（让{handicap_text}）'


def _build_jingcai_simulation_ticket_label(issue_no: str, metric: str, outcome: str, meta_payload: dict) -> str:
    return f'{issue_no or "--"} {_format_jingcai_simulation_outcome(metric, outcome, meta_payload)}'


def _resolve_simulation_next_step(bet_mode: str, max_steps: int, current_step: int, result_type: str) -> int:
    if bet_mode == 'flat':
        return 1
    if result_type == 'hit':
        return 1
    if result_type == 'refund':
        return current_step
    if current_step >= max_steps:
        return 1
    return current_step + 1


def _apply_jingcai_simulation_sale_filter(predictor_id: int, simulation: dict) -> dict:
    metric = str(simulation.get('metric') or '').strip().lower()
    metric_mapping = {
        'spf': 'spf',
        'rqspf': 'rqspf',
        'spf_parlay': 'spf',
        'rqspf_parlay': 'rqspf'
    }
    base_metric = metric_mapping.get(metric)
    if not base_metric:
        return simulation

    play_mode = 'single' if metric in {'spf', 'rqspf'} else 'parlay'
    required_sell_status = ','.join(sorted(football_utils.allowed_sell_statuses(play_mode)))

    records = list(simulation.get('records') or [])
    if not records:
        payload = dict(simulation)
        payload['sell_status_filter'] = {
            'enabled': True,
            'base_metric': base_metric,
            'required_sell_status': required_sell_status,
            'filtered_out_count': 0,
            'kept_count': 0
        }
        return payload

    items = db.get_recent_prediction_items(
        predictor_id,
        lottery_type='jingcai_football',
        limit=4000
    )
    settled_items = [item for item in items if item.get('status') == 'settled' and item.get('event_key')]
    event_map = db.get_lottery_event_map(
        'jingcai_football',
        [item.get('event_key') for item in settled_items]
    ) if settled_items else {}

    sellable_single_keys: set[tuple[str, str]] = set()
    sellable_parlay_legs: set[tuple[str, str]] = set()
    for item in settled_items:
        event = event_map.get(item.get('event_key')) or {}
        meta_payload = event.get('meta_payload') or {}
        prediction_payload = item.get('prediction_payload') or {}
        actual_payload = item.get('actual_payload') or {}
        predicted_value = prediction_payload.get(base_metric)
        actual_value = actual_payload.get(base_metric)
        if not predicted_value or not actual_value:
            continue
        if not football_utils.is_metric_sellable(
            base_metric,
            meta_payload,
            predicted_value,
            play_mode=play_mode,
            allow_settled=True
        ):
            continue
        odds = football_utils.resolve_snapshot_odds(meta_payload, base_metric, predicted_value)
        if odds is None or odds <= 0:
            continue

        issue_no = item.get('issue_no') or '--'
        event_time = event.get('event_time') or '--'
        ticket_label = _build_jingcai_simulation_ticket_label(issue_no, base_metric, predicted_value, meta_payload)
        sellable_single_keys.add((ticket_label, event_time))
        run_key = str(item.get('run_key') or '').strip()
        if run_key:
            sellable_parlay_legs.add((run_key, ticket_label))

    filtered_records = []
    for record in records:
        ticket_label = str(record.get('ticket_label') or '').strip()
        open_time = str(record.get('open_time') or '--').strip() or '--'
        if metric in {'spf', 'rqspf'}:
            if (ticket_label, open_time) in sellable_single_keys:
                filtered_records.append(record)
            continue

        run_key = str(record.get('issue_no') or '').strip()
        leg_labels = [item.strip() for item in ticket_label.split(' + ') if item.strip()]
        if run_key and len(leg_labels) >= 2 and all((run_key, leg) in sellable_parlay_legs for leg in leg_labels):
            filtered_records.append(record)

    bet_mode = str(simulation.get('bet_mode') or 'flat').strip().lower()
    bet_config = simulation.get('bet_config') or {}
    try:
        base_stake = float(bet_config.get('base_stake') or simulation.get('stake_amount') or 10.0)
    except (TypeError, ValueError):
        base_stake = 10.0
    try:
        multiplier = float(bet_config.get('multiplier') or 2.0)
    except (TypeError, ValueError):
        multiplier = 2.0
    try:
        max_steps = int(bet_config.get('max_steps') or 6)
    except (TypeError, ValueError):
        max_steps = 6
    max_steps = max(1, max_steps)

    recalculated_records = []
    cumulative_profit = 0.0
    total_stake = 0.0
    total_payout = 0.0
    hit_count = 0
    refund_count = 0
    miss_count = 0
    current_step = 1
    open_times: list[str] = []

    for record in filtered_records:
        if bet_mode == 'flat':
            stake_amount = round(base_stake, 2)
        else:
            stake_amount = round(base_stake * (multiplier ** (current_step - 1)), 2)

        try:
            odds = float(record.get('odds') or 0.0)
        except (TypeError, ValueError):
            odds = 0.0
        result_type = str(record.get('result_type') or 'miss').strip().lower() or 'miss'
        if result_type == 'hit':
            payout_amount = round(stake_amount * odds, 2)
            hit_count += 1
        elif result_type == 'refund':
            payout_amount = round(stake_amount, 2)
            refund_count += 1
        else:
            payout_amount = 0.0
            miss_count += 1

        net_profit = round(payout_amount - stake_amount, 2)
        cumulative_profit = round(cumulative_profit + net_profit, 2)
        total_stake = round(total_stake + stake_amount, 2)
        total_payout = round(total_payout + payout_amount, 2)

        updated_record = {
            **record,
            'bet_step': current_step,
            'bet_step_label': '均注' if bet_mode == 'flat' else f'第 {current_step} 手',
            'stake_amount': stake_amount,
            'payout_amount': payout_amount,
            'net_profit': net_profit,
            'cumulative_profit': cumulative_profit
        }
        recalculated_records.append(updated_record)

        current_step = _resolve_simulation_next_step(bet_mode, max_steps, current_step, result_type)
        open_time = str(record.get('open_time') or '').strip()
        if open_time and open_time != '--':
            open_times.append(open_time)

    filtered_out_count = len(records) - len(recalculated_records)
    bet_count = hit_count + refund_count + miss_count
    net_profit = round(total_payout - total_stake, 2)
    roi_percentage = round(net_profit / total_stake * 100, 2) if total_stake else 0.0
    average_profit = round(net_profit / bet_count, 2) if bet_count else 0.0

    original_summary = simulation.get('summary') or {}
    try:
        original_skipped_count = int(original_summary.get('skipped_count') or 0)
    except (TypeError, ValueError):
        original_skipped_count = 0

    period = dict(simulation.get('period') or {})
    if open_times:
        period['start_time'] = open_times[0]
        period['end_time'] = open_times[-1]
    else:
        period['start_time'] = '--'
        period['end_time'] = '--'

    payload = dict(simulation)
    payload['records'] = recalculated_records
    payload['period'] = period
    payload['summary'] = {
        'bet_count': bet_count,
        'hit_count': hit_count,
        'refund_count': refund_count,
        'miss_count': miss_count,
        'skipped_count': original_skipped_count + filtered_out_count,
        'total_stake': round(total_stake, 2),
        'total_payout': round(total_payout, 2),
        'net_profit': net_profit,
        'roi_percentage': roi_percentage,
        'average_profit': average_profit
    }
    payload['sell_status_filter'] = {
        'enabled': True,
        'base_metric': base_metric,
        'required_sell_status': required_sell_status,
        'filtered_out_count': filtered_out_count,
        'kept_count': len(recalculated_records)
    }
    return payload


def _serialize_admin_user(item: dict) -> dict:
    return {
        'id': item['id'],
        'username': item['username'],
        'email': item.get('email'),
        'is_admin': bool(item.get('is_admin')),
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None,
        'predictor_count': int(item.get('predictor_count') or 0),
        'enabled_predictor_count': int(item.get('enabled_predictor_count') or 0),
        'latest_predictor_update': utc_to_beijing(item['latest_predictor_update']) if item.get('latest_predictor_update') else None
    }


def _serialize_admin_predictor(item: dict) -> dict:
    lottery_type = normalize_lottery_type(item.get('lottery_type'))
    default_profit_rule_id = profit_simulator.get_default_rule_id(lottery_type=lottery_type) if supports_profit_simulation(lottery_type) else ''
    runtime_status, runtime_status_label = _resolve_predictor_runtime_status(item)
    engine_type = normalize_engine_type(item.get('engine_type'))
    algorithm_key = normalize_algorithm_key(lottery_type, engine_type, item.get('algorithm_key'))
    algorithm_label = get_algorithm_label(lottery_type, engine_type, algorithm_key)
    algorithm_description = get_algorithm_description(lottery_type, engine_type, algorithm_key)
    user_algorithm = item.get('user_algorithm') if is_user_algorithm_key(algorithm_key) else None
    if user_algorithm:
        algorithm_label = user_algorithm.get('name') or algorithm_label
        algorithm_description = user_algorithm.get('description') or algorithm_description
    return {
        'id': item['id'],
        'user_id': item['user_id'],
        'username': item.get('username'),
        'name': item['name'],
        'model_name': item.get('model_name'),
        'engine_type': engine_type,
        'engine_type_label': get_engine_type_label(engine_type),
        'algorithm_key': algorithm_key,
        'algorithm_source': 'user' if is_user_algorithm_key(algorithm_key) else ('builtin' if engine_type == 'machine' else ''),
        'user_algorithm_id': get_user_algorithm_id(algorithm_key) if is_user_algorithm_key(algorithm_key) else None,
        'user_algorithm_name': user_algorithm.get('name') if user_algorithm else '',
        'user_algorithm_status': user_algorithm.get('status') if user_algorithm else '',
        'algorithm_label': algorithm_label,
        'algorithm_description': algorithm_description,
        'execution_label': resolve_execution_label({
            **item,
            'engine_type': engine_type,
            'algorithm_key': algorithm_key,
            'algorithm_label': algorithm_label
        }),
        'execution_description': resolve_execution_description({
            **item,
            'engine_type': engine_type,
            'algorithm_key': algorithm_key,
            'algorithm_label': algorithm_label
        }),
        'primary_metric': item.get('primary_metric'),
        'primary_metric_label': get_target_label(lottery_type, item.get('primary_metric')),
        'profit_default_metric': item.get('profit_default_metric') or item.get('primary_metric'),
        'profit_default_metric_label': get_target_label(lottery_type, item.get('profit_default_metric') or item.get('primary_metric')),
        'profit_rule_id': item.get('profit_rule_id') or default_profit_rule_id,
        'profit_rule_label': profit_simulator.get_rule_label(item.get('profit_rule_id') or default_profit_rule_id, lottery_type) if supports_profit_simulation(lottery_type) else '',
        'lottery_type': lottery_type,
        'lottery_label': get_lottery_definition(lottery_type).label,
        'share_level': item.get('share_level'),
        'enabled': bool(item.get('enabled')),
        'auto_paused': bool(item.get('auto_paused')),
        'runtime_status': runtime_status,
        'runtime_status_label': runtime_status_label,
        'consecutive_ai_failures': int(item.get('consecutive_ai_failures') or 0),
        'auto_paused_at': utc_to_beijing(item['auto_paused_at']) if item.get('auto_paused_at') else None,
        'auto_pause_reason': item.get('auto_pause_reason'),
        'last_ai_error_message': item.get('last_ai_error_message'),
        'last_ai_error_at': utc_to_beijing(item['last_ai_error_at']) if item.get('last_ai_error_at') else None,
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None,
        'updated_at': utc_to_beijing(item['updated_at']) if item.get('updated_at') else None,
        'prediction_count': int(item.get('prediction_count') or 0),
        'failed_prediction_count': int(item.get('failed_prediction_count') or 0),
        'latest_issue_no': item.get('latest_issue_no'),
        'latest_prediction_update': utc_to_beijing(item['latest_prediction_update']) if item.get('latest_prediction_update') else None
    }


def _serialize_admin_failure(item: dict) -> dict:
    lottery_type = normalize_lottery_type(item.get('lottery_type'))
    return {
        'issue_no': item.get('issue_no'),
        'status': item.get('status'),
        'error_message': item.get('error_message'),
        'updated_at': utc_to_beijing(item['updated_at']) if item.get('updated_at') else None,
        'predictor_id': item.get('predictor_id'),
        'predictor_name': item.get('predictor_name'),
        'username': item.get('username'),
        'lottery_type': lottery_type,
        'lottery_label': get_lottery_definition(lottery_type).label
    }


def _serialize_jingcai_backfill_job(item: dict | None) -> dict:
    if not item:
        return {}
    return {
        'id': item.get('id'),
        'trigger_source': item.get('trigger_source') or '',
        'status': item.get('status') or '',
        'start_date': item.get('start_date') or '',
        'end_date': item.get('end_date') or '',
        'include_details': bool(item.get('include_details')),
        'requested_days': int(item.get('requested_days') or 0),
        'match_count': int(item.get('match_count') or 0),
        'detail_count': int(item.get('detail_count') or 0),
        'error_message': item.get('error_message') or '',
        'result': item.get('result') or {},
        'started_at': utc_to_beijing(item['started_at']) if item.get('started_at') else None,
        'finished_at': utc_to_beijing(item['finished_at']) if item.get('finished_at') else None,
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None,
        'updated_at': utc_to_beijing(item['updated_at']) if item.get('updated_at') else None
    }


def _build_admin_dashboard_data() -> dict:
    summary = db.get_admin_summary_counts()
    users = db.get_admin_users_overview()
    predictors = db.get_admin_predictors_overview()
    failed_predictions = db.get_recent_failed_predictions(limit=20)
    scheduler = db.get_scheduler_snapshot(AUTO_PREDICTION_SCHEDULER)
    backfill_scheduler = db.get_scheduler_snapshot(JINGCAI_HISTORY_BACKFILL_SCHEDULER)
    jingcai_data_health = db.build_jingcai_data_health()
    guard_settings = prediction_guard.get_settings()
    notification_settings = notification_service.get_settings()

    scheduler_data = {
        'name': AUTO_PREDICTION_SCHEDULER,
        'auto_prediction_enabled': config.AUTO_PREDICTION,
        'poll_interval_seconds': config.PREDICTION_POLL_INTERVAL,
        'pc28_prediction_retention_days': config.PC28_PREDICTION_RETENTION_DAYS,
        'pc28_draw_retention_days': config.PC28_DRAW_RETENTION_DAYS,
        'pc28_archive_maintenance_interval_seconds': config.PC28_ARCHIVE_MAINTENANCE_INTERVAL,
        'pc28_archive_vacuum_interval_seconds': config.PC28_ARCHIVE_VACUUM_INTERVAL,
        'owner_id': scheduler.get('owner_id') if scheduler else None,
        'heartbeat_at': utc_to_beijing(scheduler['heartbeat_at']) if scheduler and scheduler.get('heartbeat_at') else None,
        'seconds_since_heartbeat': None
    }

    if scheduler and scheduler.get('heartbeat_at'):
        try:
            heartbeat_at = datetime.strptime(str(scheduler['heartbeat_at']), '%Y-%m-%d %H:%M:%S')
            scheduler_data['seconds_since_heartbeat'] = max(0, int((datetime.utcnow() - heartbeat_at).total_seconds()))
        except Exception:
            scheduler_data['seconds_since_heartbeat'] = None

    backfill_scheduler_data = {
        'name': JINGCAI_HISTORY_BACKFILL_SCHEDULER,
        'enabled': bool(getattr(config, 'JINGCAI_HISTORY_BACKFILL_ENABLED', True)),
        'interval_seconds': int(getattr(config, 'JINGCAI_HISTORY_BACKFILL_INTERVAL_SECONDS', 86400)),
        'lookback_days': int(getattr(config, 'JINGCAI_HISTORY_BACKFILL_LOOKBACK_DAYS', 7)),
        'include_details': bool(getattr(config, 'JINGCAI_HISTORY_BACKFILL_INCLUDE_DETAILS', True)),
        'owner_id': backfill_scheduler.get('owner_id') if backfill_scheduler else None,
        'heartbeat_at': utc_to_beijing(backfill_scheduler['heartbeat_at']) if backfill_scheduler and backfill_scheduler.get('heartbeat_at') else None,
        'seconds_since_heartbeat': None
    }
    if backfill_scheduler and backfill_scheduler.get('heartbeat_at'):
        try:
            heartbeat_at = datetime.strptime(str(backfill_scheduler['heartbeat_at']), '%Y-%m-%d %H:%M:%S')
            backfill_scheduler_data['seconds_since_heartbeat'] = max(0, int((datetime.utcnow() - heartbeat_at).total_seconds()))
        except Exception:
            backfill_scheduler_data['seconds_since_heartbeat'] = None

    return {
        'summary': {
            'total_users': int(summary.get('total_users') or 0),
            'admin_users': int(summary.get('admin_users') or 0),
            'total_predictors': int(summary.get('total_predictors') or 0),
            'enabled_predictors': int(summary.get('enabled_predictors') or 0),
            'auto_paused_predictors': int(summary.get('auto_paused_predictors') or 0),
            'shared_predictors': int(summary.get('shared_predictors') or 0),
            'total_predictions': int(summary.get('total_predictions') or 0),
            'pending_predictions': int(summary.get('pending_predictions') or 0),
            'failed_predictions': int(summary.get('failed_predictions') or 0),
            'settled_predictions': int(summary.get('settled_predictions') or 0),
            'total_draws': int(summary.get('total_draws') or 0)
        },
        'scheduler': scheduler_data,
        'jingcai_data_health': {
            **jingcai_data_health,
            'recent_jobs': [_serialize_jingcai_backfill_job(item) for item in jingcai_data_health.get('recent_jobs', [])],
            'scheduler': backfill_scheduler_data
        },
        'prediction_guard': guard_settings,
        'notification_settings': {
            **notification_settings,
            'telegram_bot_token_masked': mask_api_key(notification_settings.get('telegram_bot_token'))
        },
        'users': [_serialize_admin_user(item) for item in users],
        'predictors': [_serialize_admin_predictor(item) for item in predictors],
        'recent_failures': [_serialize_admin_failure(item) for item in failed_predictions]
    }


def _serialize_bet_profile(item: dict) -> dict:
    lottery_type = normalize_lottery_type(item.get('lottery_type'))
    bet_strategy = build_bet_strategy(
        bet_mode=item.get('mode'),
        base_stake=item.get('base_stake'),
        multiplier=item.get('multiplier'),
        max_steps=item.get('max_steps'),
        refund_action=item.get('refund_action'),
        cap_action=item.get('cap_action')
    )
    return {
        'id': item['id'],
        'user_id': item['user_id'],
        'name': item.get('name') or '',
        'lottery_type': lottery_type,
        'lottery_label': get_lottery_definition(lottery_type).label,
        'mode': bet_strategy['mode'],
        'mode_label': bet_strategy['mode_label'],
        'base_stake': bet_strategy['base_stake'],
        'multiplier': bet_strategy['multiplier'],
        'max_steps': bet_strategy['max_steps'],
        'refund_action': bet_strategy['refund_action'],
        'refund_action_label': bet_strategy['refund_action_label'],
        'cap_action': bet_strategy['cap_action'],
        'cap_action_label': bet_strategy['cap_action_label'],
        'strategy_label': build_bet_strategy_label(bet_strategy),
        'enabled': bool(item.get('enabled')),
        'is_default': bool(item.get('is_default')),
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None,
        'updated_at': utc_to_beijing(item['updated_at']) if item.get('updated_at') else None
    }


def _serialize_notification_sender_account(item: dict) -> dict:
    channel_type = str(item.get('channel_type') or 'telegram').strip().lower()
    status = str(item.get('status') or 'active').strip().lower()
    return {
        'id': item['id'],
        'user_id': item['user_id'],
        'channel_type': channel_type,
        'channel_label': NOTIFICATION_CHANNEL_LABELS.get(channel_type, channel_type),
        'sender_name': item.get('sender_name') or item.get('bot_name') or '',
        'bot_name': item.get('bot_name') or '',
        'status': status,
        'status_label': NOTIFICATION_STATUS_LABELS.get(status, status),
        'is_default': bool(item.get('is_default')),
        'last_verified_at': utc_to_beijing(item['last_verified_at']) if item.get('last_verified_at') else None,
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None,
        'updated_at': utc_to_beijing(item['updated_at']) if item.get('updated_at') else None
    }


def _serialize_notification_endpoint(item: dict) -> dict:
    channel_type = str(item.get('channel_type') or 'telegram').strip().lower()
    status = str(item.get('status') or 'active').strip().lower()
    return {
        'id': item['id'],
        'user_id': item['user_id'],
        'channel_type': channel_type,
        'channel_label': NOTIFICATION_CHANNEL_LABELS.get(channel_type, channel_type),
        'endpoint_key': item.get('endpoint_key') or '',
        'endpoint_label': item.get('endpoint_label') or item.get('endpoint_key') or '',
        'config': item.get('config') or {},
        'status': status,
        'status_label': NOTIFICATION_STATUS_LABELS.get(status, status),
        'is_default': bool(item.get('is_default')),
        'last_verified_at': utc_to_beijing(item['last_verified_at']) if item.get('last_verified_at') else None,
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None,
        'updated_at': utc_to_beijing(item['updated_at']) if item.get('updated_at') else None
    }


def _serialize_notification_subscription(item: dict) -> dict:
    lottery_type = normalize_lottery_type(item.get('predictor_lottery_type'))
    event_type = str(item.get('event_type') or 'prediction_created').strip().lower()
    delivery_mode = str(item.get('delivery_mode') or 'notify_only').strip().lower()
    return {
        'id': item['id'],
        'user_id': item['user_id'],
        'predictor_id': int(item.get('predictor_id') or 0),
        'predictor_name': item.get('predictor_name') or '',
        'lottery_type': lottery_type,
        'lottery_label': get_lottery_definition(lottery_type).label,
        'endpoint_id': int(item.get('endpoint_id') or 0),
        'channel_type': item.get('channel_type') or 'telegram',
        'channel_label': NOTIFICATION_CHANNEL_LABELS.get(item.get('channel_type') or 'telegram', item.get('channel_type') or 'telegram'),
        'endpoint_key': item.get('endpoint_key') or '',
        'endpoint_label': item.get('endpoint_label') or item.get('endpoint_key') or '',
        'sender_mode': item.get('sender_mode') or 'platform',
        'sender_mode_label': NOTIFICATION_SENDER_MODE_LABELS.get(item.get('sender_mode') or 'platform', item.get('sender_mode') or 'platform'),
        'sender_account_id': item.get('sender_account_id'),
        'sender_account_name': item.get('sender_account_name') or '',
        'sender_bot_name': item.get('sender_bot_name') or '',
        'bet_profile_id': item.get('bet_profile_id'),
        'bet_profile_name': item.get('bet_profile_name') or '',
        'bet_profile_mode': item.get('bet_profile_mode'),
        'bet_profile_base_stake': item.get('bet_profile_base_stake'),
        'event_type': event_type,
        'event_label': NOTIFICATION_EVENT_LABELS.get(event_type, event_type),
        'delivery_mode': delivery_mode,
        'delivery_mode_label': NOTIFICATION_DELIVERY_MODE_LABELS.get(delivery_mode, delivery_mode),
        'filter': item.get('filter') or {},
        'enabled': bool(item.get('enabled')),
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None,
        'updated_at': utc_to_beijing(item['updated_at']) if item.get('updated_at') else None
    }


def _serialize_notification_delivery(item: dict) -> dict:
    status = str(item.get('status') or 'pending').strip().lower()
    can_retry = status in {'failed', 'skipped'} and (item.get('channel_type') or '') == 'telegram'
    return {
        'id': item['id'],
        'subscription_id': int(item.get('subscription_id') or 0),
        'predictor_id': int(item.get('predictor_id') or 0),
        'predictor_name': item.get('predictor_name') or '',
        'endpoint_id': int(item.get('endpoint_id') or 0),
        'channel_type': item.get('channel_type') or 'telegram',
        'channel_label': NOTIFICATION_CHANNEL_LABELS.get(item.get('channel_type') or 'telegram', item.get('channel_type') or 'telegram'),
        'endpoint_label': item.get('endpoint_label') or '',
        'sender_mode': item.get('sender_mode') or 'platform',
        'sender_mode_label': NOTIFICATION_SENDER_MODE_LABELS.get(item.get('sender_mode') or 'platform', item.get('sender_mode') or 'platform'),
        'sender_account_id': item.get('sender_account_id'),
        'sender_account_name': item.get('sender_account_name') or '',
        'event_type': item.get('event_type') or 'prediction_created',
        'event_label': NOTIFICATION_EVENT_LABELS.get(item.get('event_type') or 'prediction_created', item.get('event_type') or 'prediction_created'),
        'record_key': item.get('record_key') or '',
        'status': status,
        'status_label': status,
        'can_retry': can_retry,
        'payload': item.get('payload') or {},
        'error_message': item.get('error_message'),
        'sent_at': utc_to_beijing(item['sent_at']) if item.get('sent_at') else None,
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None,
        'updated_at': utc_to_beijing(item['updated_at']) if item.get('updated_at') else None
    }


def _share_level_label(share_level: str) -> str:
    mapping = {
        'stats_only': '只公开统计',
        'records': '公开统计 + 预测记录',
        'analysis': '公开统计 + 预测记录 + 分析说明'
    }
    return mapping.get(share_level, share_level)


def _build_public_links(predictor_id: int) -> dict:
    path = f'/public/predictors/{predictor_id}'
    url = path

    if has_request_context():
        path = url_for('public_predictor_page', predictor_id=predictor_id)
        url = url_for('public_predictor_page', predictor_id=predictor_id, _external=True)

    return {
        'path': path,
        'url': url
    }


def _is_predictor_publicly_available(predictor: dict | None) -> bool:
    if not predictor:
        return False
    lottery_type = normalize_lottery_type(predictor.get('lottery_type'))
    return bool(predictor.get('enabled')) and not bool(predictor.get('auto_paused')) and supports_public_pages(lottery_type)


def _get_public_lottery_page_context(lottery_type: str) -> dict:
    normalized_lottery_type = normalize_lottery_type(lottery_type)
    page_config = PUBLIC_LOTTERY_PAGE_CONFIG.get(normalized_lottery_type)
    if not page_config:
        raise ValueError('暂不支持的彩种')
    alternate_items = [
        {
            'lottery_type': item_type,
            'label': get_lottery_definition(item_type).label,
            'path': f"/{item_config['slug']}",
            'is_current': item_type == normalized_lottery_type
        }
        for item_type, item_config in PUBLIC_LOTTERY_PAGE_CONFIG.items()
    ]
    canonical_url = ''
    if has_request_context():
        canonical_url = url_for('public_lottery_page', lottery_slug=page_config['slug'], _external=True)
    return {
        'lottery_type': normalized_lottery_type,
        'lottery_label': get_lottery_definition(normalized_lottery_type).label,
        'page_title': page_config['title'],
        'page_heading': page_config['heading'],
        'page_description': page_config['description'],
        'canonical_path': f"/{page_config['slug']}",
        'canonical_url': canonical_url,
        'alternate_items': alternate_items
    }


def _build_public_predictor_rankings(sort_by: str = 'recent100', metric: str = 'combo', limit: int = 10, lottery_type: str = 'pc28') -> list[dict]:
    normalized_lottery_type = normalize_lottery_type(lottery_type)
    predictors = [
        item for item in db.get_all_predictors(include_secret=False)
        if _is_predictor_publicly_available(item) and normalize_lottery_type(item.get('lottery_type')) == normalized_lottery_type
    ]
    ranked_items = []

    for predictor in predictors:
        if normalized_lottery_type == 'jingcai_football':
            stats = jingcai_football_service.build_predictor_stats(
                db,
                predictor['id'],
                predictor.get('primary_metric') or 'spf'
            )
        else:
            stats = db.get_predictor_stats(predictor['id'])
        metric_stats = (stats.get('metrics') or {}).get(metric) or {}
        recent20 = metric_stats.get('recent_20') or {}
        recent100 = metric_stats.get('recent_100') or {}
        overall = metric_stats.get('overall') or {}
        if normalized_lottery_type == 'jingcai_football':
            streaks = (stats.get('metric_streaks') or {}).get(metric) or {}
        else:
            settled_rows = [row for row in db.get_recent_predictions(predictor['id'], limit=None) if row['status'] == 'settled']
            streaks = db._build_streak_stats(settled_rows, metric)
        user = db.get_user_by_id(predictor['user_id'])

        ranked_items.append({
            'predictor_id': predictor['id'],
            'predictor_name': predictor['name'],
            'username': user['username'] if user else 'unknown',
            'model_name': predictor['model_name'],
            'share_level': predictor.get('share_level') or ('records' if predictor.get('share_predictions') else 'stats_only'),
            'share_level_label': _share_level_label(predictor.get('share_level') or ('records' if predictor.get('share_predictions') else 'stats_only')),
            'share_predictions': bool(predictor.get('share_predictions')),
            'lottery_type': normalized_lottery_type,
            'lottery_label': get_lottery_definition(normalized_lottery_type).label,
            'primary_metric': predictor.get('primary_metric') or ('spf' if normalized_lottery_type == 'jingcai_football' else 'combo'),
            'primary_metric_label': stats.get('primary_metric_label') or get_target_label(normalized_lottery_type, predictor.get('primary_metric') or ''),
            'metric': metric,
            'metric_label': metric_stats.get('label') or get_target_label(normalized_lottery_type, metric),
            'recent_20': recent20,
            'recent_100': recent100,
            'overall': overall,
            'current_hit_streak': streaks.get('current_hit_streak', 0),
            'current_miss_streak': streaks.get('current_miss_streak', 0),
            'recent_100_max_hit_streak': streaks.get('recent_100_max_hit_streak', 0),
            'recent_100_max_miss_streak': streaks.get('recent_100_max_miss_streak', 0),
            'historical_max_hit_streak': streaks.get('historical_max_hit_streak', 0),
            'historical_max_miss_streak': streaks.get('historical_max_miss_streak', 0),
            'settled_predictions': stats.get('settled_predictions', 0)
        })

    def _rank_value(item: dict):
        if sort_by == 'recent20':
            return (
                item['recent_20'].get('hit_rate') or -1,
                item['recent_20'].get('hit_count') or -1
            )
        if sort_by == 'current_streak':
            return (
                item.get('current_hit_streak', 0),
                item['recent_20'].get('hit_rate') or -1
            )
        if sort_by == 'historical_streak':
            return (
                item.get('historical_max_hit_streak', 0),
                item['overall'].get('hit_rate') or -1
            )
        return (
            item['recent_100'].get('hit_rate') or -1,
            item['recent_100'].get('hit_count') or -1
        )

    ranked_items.sort(key=_rank_value, reverse=True)
    return ranked_items[:limit]


def _get_public_predictor_detail(predictor_id: int) -> dict:
    predictor = db.get_predictor(predictor_id, include_secret=False)
    if not _is_predictor_publicly_available(predictor):
        return None
    lottery_type = normalize_lottery_type(predictor.get('lottery_type'))

    if lottery_type == 'jingcai_football':
        stats = jingcai_football_service.build_predictor_stats(
            db,
            predictor_id,
            predictor.get('primary_metric') or 'spf'
        )
    else:
        stats = db.get_predictor_stats(predictor_id)
    share_level = predictor.get('share_level') or ('records' if predictor.get('share_predictions') else 'stats_only')
    can_view_records = share_level in {'records', 'analysis'}
    can_view_analysis = share_level == 'analysis'
    if lottery_type == 'jingcai_football':
        raw_predictions = jingcai_football_service.get_recent_prediction_items(db, predictor_id, limit=20) if can_view_records else []
        predictions = [_serialize_prediction_item_group(item) for item in raw_predictions]
        recent_runs = db.get_recent_prediction_runs(predictor_id, lottery_type=lottery_type, limit=5) if can_view_records else []
        current_run = next((item for item in recent_runs if item.get('status') == 'pending'), None) or (recent_runs[0] if recent_runs else None)
        current_prediction = _serialize_prediction_run(jingcai_football_service.build_run_view_model(db, current_run)) if current_run else None
        latest_prediction = predictions[0] if predictions else None
    else:
        predictions = db.get_recent_predictions(predictor_id, limit=20) if can_view_records else []
        current_prediction = next((item for item in predictions if item['status'] == 'pending'), None) if predictions else None
        latest_prediction = predictions[0] if predictions else None
    user = db.get_user_by_id(predictor['user_id'])
    public_links = _build_public_links(predictor['id'])

    return {
        'predictor': {
            'id': predictor['id'],
            'name': predictor['name'],
            'username': user['username'] if user else 'unknown',
            'lottery_type': lottery_type,
            'lottery_label': get_lottery_definition(lottery_type).label,
            'model_name': predictor['model_name'],
            'primary_metric': predictor.get('primary_metric') or ('spf' if lottery_type == 'jingcai_football' else 'combo'),
            'primary_metric_label': stats.get('primary_metric_label') or get_target_label(lottery_type, predictor.get('primary_metric') or ''),
            'profit_default_metric': predictor.get('profit_default_metric') or (profit_simulator.get_default_metric(predictor) if supports_profit_simulation(lottery_type) else ''),
            'profit_rule_id': predictor.get('profit_rule_id') or (profit_simulator.get_default_rule_id(predictor) if supports_profit_simulation(lottery_type) else ''),
            'profit_rule_label': profit_simulator.get_rule_label(predictor.get('profit_rule_id') or profit_simulator.get_default_rule_id(predictor), predictor) if supports_profit_simulation(lottery_type) else '',
            'prediction_method': predictor.get('prediction_method') or '自定义策略',
            'prediction_targets': predictor.get('prediction_targets') or [],
            'simulation_metrics': profit_simulator.get_metric_options(predictor) if supports_profit_simulation(lottery_type) else [],
            'default_simulation_metric': profit_simulator.get_default_metric(predictor) if supports_profit_simulation(lottery_type) else None,
            'profit_rule_options': profit_simulator.get_rule_options(predictor) if supports_profit_simulation(lottery_type) else [],
            'profit_period_options': profit_simulator.get_period_options(predictor) if supports_profit_simulation(lottery_type) else [],
            'default_profit_period_key': profit_simulator.get_default_period_key(predictor) if supports_profit_simulation(lottery_type) else '',
            'odds_profiles': profit_simulator.get_odds_profile_options(predictor) if supports_profit_simulation(lottery_type) else [],
            'capabilities': get_lottery_definition(lottery_type).to_catalog_item()['capabilities'],
            'history_window': predictor.get('history_window'),
            'share_level': share_level,
            'share_level_label': _share_level_label(share_level),
            'share_predictions': can_view_records,
            'can_view_records': can_view_records,
            'can_view_analysis': can_view_analysis,
            'can_subscribe_public': can_view_records,
            'public_path': public_links['path'],
            'public_url': public_links['url']
        },
        'stats': stats,
        'current_prediction': _serialize_public_prediction_with_level(current_prediction, share_level) if lottery_type == 'pc28' else current_prediction,
        'latest_prediction': _serialize_public_prediction_with_level(latest_prediction, share_level) if lottery_type == 'pc28' else latest_prediction,
        'recent_predictions': [_serialize_public_prediction_with_level(item, share_level) for item in predictions] if lottery_type == 'pc28' else predictions
}


def _can_user_subscribe_to_predictor(user_id: int, predictor: dict | None) -> bool:
    if not predictor:
        return False
    predictor_user_id = int(predictor.get('user_id') or 0)
    if predictor_user_id == int(user_id):
        return True
    if not _is_predictor_publicly_available(predictor):
        return False
    share_level = predictor.get('share_level') or ('records' if predictor.get('share_predictions') else 'stats_only')
    return share_level in {'records', 'analysis'}


def _parse_bool(value, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def _parse_optional_int(value):
    if value in {None, ''}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_positive_int(value, default: int, minimum: int = 1, maximum: int = 500) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _build_pagination_payload(page: int, page_size: int, total: int) -> dict:
    safe_total = max(0, int(total or 0))
    safe_page_size = max(1, int(page_size or 1))
    total_pages = max(1, math.ceil(safe_total / safe_page_size)) if safe_total else 1
    safe_page = max(1, min(int(page or 1), total_pages))
    return {
        'page': safe_page,
        'page_size': safe_page_size,
        'total': safe_total,
        'total_pages': total_pages,
        'has_prev': safe_page > 1,
        'has_next': safe_page < total_pages
    }


def _prediction_matches_outcome_filter(prediction: dict, outcome: str) -> bool:
    normalized = str(outcome or 'all').strip().lower()
    if normalized == 'all':
        return True
    lottery_type = normalize_lottery_type(prediction.get('lottery_type'))
    hit_values: list[int] = []
    if lottery_type == 'jingcai_football':
        hit_payload = prediction.get('hit_payload') or {}
        for key in ('spf', 'rqspf'):
            value = hit_payload.get(key)
            if value is not None:
                hit_values.append(int(value))
    else:
        for key in ('hit_number', 'hit_big_small', 'hit_odd_even', 'hit_combo'):
            value = prediction.get(key)
            if value is not None:
                hit_values.append(int(value))
        predicted_combo = prediction.get('prediction_combo')
        actual_combo = prediction.get('actual_combo')
        predicted_group = derive_double_group(predicted_combo)
        actual_group = derive_double_group(actual_combo)
        if predicted_group and actual_group:
            hit_values.append(1 if predicted_group == actual_group else 0)
        kill_group = derive_kill_group(predicted_combo)
        if kill_group and actual_combo:
            hit_values.append(1 if actual_combo != kill_group else 0)
    if not hit_values:
        return False
    if normalized == 'hit':
        return any(value == 1 for value in hit_values)
    if normalized == 'miss':
        return all(value == 0 for value in hit_values)
    return True


def _validate_bet_profile_payload(data: dict, existing_profile: dict | None = None) -> tuple[dict, list[str]]:
    errors = []
    lottery_type = normalize_lottery_type(data.get('lottery_type') or (existing_profile.get('lottery_type') if existing_profile else 'pc28'))
    fallback_name = existing_profile.get('name') if existing_profile else ''
    fallback_enabled = existing_profile.get('enabled') if existing_profile else True
    fallback_is_default = existing_profile.get('is_default') if existing_profile else False
    strategy = build_bet_strategy(
        bet_mode=data.get('mode') or (existing_profile.get('mode') if existing_profile else None),
        base_stake=data.get('base_stake', existing_profile.get('base_stake') if existing_profile else None),
        multiplier=data.get('multiplier', existing_profile.get('multiplier') if existing_profile else None),
        max_steps=data.get('max_steps', existing_profile.get('max_steps') if existing_profile else None),
        refund_action=data.get('refund_action') or (existing_profile.get('refund_action') if existing_profile else None),
        cap_action=data.get('cap_action') or (existing_profile.get('cap_action') if existing_profile else None)
    )
    name = str(data.get('name') or fallback_name).strip()
    enabled = _parse_bool(data.get('enabled'), bool(fallback_enabled))
    is_default = _parse_bool(data.get('is_default'), bool(fallback_is_default))

    if not name:
        errors.append('下注方案名称不能为空')

    payload = {
        'name': name,
        'lottery_type': lottery_type,
        'mode': strategy['mode'],
        'base_stake': strategy['base_stake'],
        'multiplier': strategy['multiplier'],
        'max_steps': strategy['max_steps'],
        'refund_action': strategy['refund_action'],
        'cap_action': strategy['cap_action'],
        'enabled': enabled,
        'is_default': is_default
    }
    return payload, errors


def _validate_notification_endpoint_payload(data: dict, existing_endpoint: dict | None = None) -> tuple[dict, list[str]]:
    errors = []
    channel_type = str(data.get('channel_type') or (existing_endpoint.get('channel_type') if existing_endpoint else 'telegram')).strip().lower()
    endpoint_key = str(data.get('endpoint_key') or (existing_endpoint.get('endpoint_key') if existing_endpoint else '')).strip()
    endpoint_label = str(data.get('endpoint_label') or (existing_endpoint.get('endpoint_label') if existing_endpoint else endpoint_key)).strip()
    config_payload = data.get('config', existing_endpoint.get('config') if existing_endpoint else {})
    config_payload = config_payload if isinstance(config_payload, dict) else {}
    status = str(data.get('status') or (existing_endpoint.get('status') if existing_endpoint else 'active')).strip().lower()
    is_default = _parse_bool(data.get('is_default'), bool(existing_endpoint.get('is_default')) if existing_endpoint else False)

    if channel_type not in NOTIFICATION_CHANNEL_LABELS:
        errors.append('当前仅支持 Telegram 通知接收端')
    if not endpoint_key:
        errors.append('接收端标识不能为空')
    if status not in NOTIFICATION_STATUS_LABELS:
        status = 'active'

    payload = {
        'channel_type': channel_type,
        'endpoint_key': endpoint_key,
        'endpoint_label': endpoint_label or endpoint_key,
        'config': config_payload,
        'status': status,
        'is_default': is_default
    }
    return payload, errors


def _validate_notification_sender_account_payload(data: dict, existing_sender: dict | None = None) -> tuple[dict, list[str]]:
    errors = []
    channel_type = str(data.get('channel_type') or (existing_sender.get('channel_type') if existing_sender else 'telegram')).strip().lower()
    sender_name = str(data.get('sender_name') or (existing_sender.get('sender_name') if existing_sender else '')).strip()
    bot_name = str(data.get('bot_name') or (existing_sender.get('bot_name') if existing_sender else '')).strip()
    bot_token = str(data.get('bot_token') or '').strip()
    status = str(data.get('status') or (existing_sender.get('status') if existing_sender else 'active')).strip().lower()
    is_default = _parse_bool(data.get('is_default'), bool(existing_sender.get('is_default')) if existing_sender else False)

    if channel_type not in NOTIFICATION_CHANNEL_LABELS:
        errors.append('当前仅支持 Telegram 发送方')
    if not sender_name:
        errors.append('发送方名称不能为空')
    if existing_sender is None and not bot_token:
        errors.append('Bot Token 不能为空')
    if existing_sender is not None and not bot_token:
        bot_token = existing_sender.get('bot_token') or ''
    if status not in NOTIFICATION_STATUS_LABELS:
        status = 'active'

    payload = {
        'channel_type': channel_type,
        'sender_name': sender_name,
        'bot_name': bot_name,
        'bot_token': bot_token,
        'status': status,
        'is_default': is_default
    }
    return payload, errors


def _validate_notification_subscription_payload(
    user_id: int,
    data: dict,
    existing_subscription: dict | None = None
) -> tuple[dict, list[str]]:
    errors = []
    predictor_id = _parse_optional_int(data.get('predictor_id', existing_subscription.get('predictor_id') if existing_subscription else None))
    endpoint_id = _parse_optional_int(data.get('endpoint_id', existing_subscription.get('endpoint_id') if existing_subscription else None))
    sender_mode = str(data.get('sender_mode') or (existing_subscription.get('sender_mode') if existing_subscription else 'platform')).strip().lower()
    sender_account_id = _parse_optional_int(data.get('sender_account_id', existing_subscription.get('sender_account_id') if existing_subscription else None))
    bet_profile_id = _parse_optional_int(data.get('bet_profile_id', existing_subscription.get('bet_profile_id') if existing_subscription else None))
    event_type = str(data.get('event_type') or (existing_subscription.get('event_type') if existing_subscription else 'prediction_created')).strip().lower()
    delivery_mode = str(data.get('delivery_mode') or (existing_subscription.get('delivery_mode') if existing_subscription else 'notify_only')).strip().lower()
    filters = data.get('filter', existing_subscription.get('filter') if existing_subscription else {})
    filters = filters if isinstance(filters, dict) else {}
    enabled = _parse_bool(data.get('enabled'), bool(existing_subscription.get('enabled')) if existing_subscription else True)
    if event_type == PERFORMANCE_EVENT_TYPE:
        delivery_mode = 'notify_only'
        bet_profile_id = None

    predictor = db.get_predictor(predictor_id, include_secret=False) if predictor_id else None
    endpoint = db.get_notification_endpoint(endpoint_id) if endpoint_id else None
    sender_account = db.get_notification_sender_account(sender_account_id) if sender_account_id else None
    bet_profile = db.get_bet_profile(bet_profile_id) if bet_profile_id else None

    if not predictor_id or not predictor or not _can_user_subscribe_to_predictor(user_id, predictor):
        errors.append('订阅的预测方案不存在或无权访问')
    if not endpoint_id or not endpoint or int(endpoint.get('user_id') or 0) != int(user_id):
        errors.append('通知接收端不存在或无权访问')
    if sender_mode not in NOTIFICATION_SENDER_MODE_LABELS:
        sender_mode = 'platform'
    if sender_mode == 'user_sender':
        if not sender_account_id or not sender_account or int(sender_account.get('user_id') or 0) != int(user_id):
            errors.append('通知发送方不存在或无权访问')
        elif str(sender_account.get('status') or '').strip().lower() != 'active':
            errors.append('通知发送方未启用')
    else:
        sender_account_id = None
    if bet_profile_id and (not bet_profile or int(bet_profile.get('user_id') or 0) != int(user_id)):
        errors.append('下注策略不存在或无权访问')

    predictor_lottery_type = normalize_lottery_type(predictor.get('lottery_type')) if predictor else 'pc28'
    if bet_profile and normalize_lottery_type(bet_profile.get('lottery_type')) != predictor_lottery_type:
        errors.append('下注策略和彩种必须保持一致')
    if sender_account and str(sender_account.get('channel_type') or '').strip().lower() != str(endpoint.get('channel_type') or '').strip().lower():
        errors.append('通知发送方和接收端渠道必须一致')

    if event_type not in NOTIFICATION_EVENT_LABELS:
        errors.append('当前仅支持预测生成或表现告警通知事件')
    if delivery_mode not in NOTIFICATION_DELIVERY_MODE_LABELS:
        delivery_mode = 'notify_only'
    if delivery_mode == 'follow_bet' and not bet_profile_id:
        errors.append('启用下注策略模式时必须绑定下注策略')
    if event_type == PERFORMANCE_EVENT_TYPE:
        if predictor_lottery_type != 'pc28':
            errors.append('表现告警当前仅支持 PC28 方案')
        filters, rule_errors = notification_rule_engine.normalize_filter(filters, predictor_lottery_type)
        errors.extend(rule_errors)
    elif 'rules' in filters:
        filters = {
            key: value
            for key, value in filters.items()
            if key != 'rules'
        }

    payload = {
        'predictor_id': predictor_id,
        'endpoint_id': endpoint_id,
        'sender_mode': sender_mode,
        'sender_account_id': sender_account_id,
        'bet_profile_id': bet_profile_id,
        'event_type': event_type,
        'delivery_mode': delivery_mode,
        'filters': filters,
        'enabled': enabled
    }
    return payload, errors


def _validate_predictor_payload(
    data: dict,
    existing_predictor: dict | None = None,
    user_id: int | None = None
) -> tuple[dict, list[str]]:
    errors = []
    lottery_type = normalize_lottery_type(data.get('lottery_type') or (existing_predictor.get('lottery_type') if existing_predictor else 'pc28'))
    lottery_definition = get_lottery_definition(lottery_type)

    fallback_name = existing_predictor.get('name') if existing_predictor else ''
    fallback_engine_type = existing_predictor.get('engine_type') if existing_predictor else 'ai'
    fallback_algorithm_key = existing_predictor.get('algorithm_key') if existing_predictor else ''
    fallback_api_url = existing_predictor.get('api_url') if existing_predictor else ''
    fallback_model_name = existing_predictor.get('model_name') if existing_predictor else ''
    fallback_api_mode = existing_predictor.get('api_mode') if existing_predictor else 'auto'
    fallback_primary_metric = existing_predictor.get('primary_metric') if existing_predictor else lottery_definition.primary_metric_options[0][0]
    fallback_profit_default_metric = existing_predictor.get('profit_default_metric') if existing_predictor else fallback_primary_metric
    fallback_profit_rule_id = existing_predictor.get('profit_rule_id') if existing_predictor else DEFAULT_PROFIT_RULE_ID
    fallback_share_level = existing_predictor.get('share_level') if existing_predictor else ('records' if (existing_predictor and existing_predictor.get('share_predictions')) else 'stats_only')
    fallback_method = existing_predictor.get('prediction_method') if existing_predictor else ''
    fallback_prompt = existing_predictor.get('system_prompt') if existing_predictor else ''
    fallback_injection_mode = existing_predictor.get('data_injection_mode') if existing_predictor else 'summary'
    fallback_user_algorithm_fallback_strategy = existing_predictor.get('user_algorithm_fallback_strategy') if existing_predictor else 'fail'

    name = str(data.get('name') or fallback_name).strip()
    engine_type = normalize_engine_type(data.get('engine_type') or fallback_engine_type)
    algorithm_key = normalize_algorithm_key(
        lottery_type,
        engine_type,
        data.get('algorithm_key') or fallback_algorithm_key
    )
    api_key = str(data.get('api_key') or '').strip()
    api_url = str(data.get('api_url') or fallback_api_url).strip()
    model_name = str(data.get('model_name') or fallback_model_name).strip()
    api_mode = normalize_api_mode(data.get('api_mode') or fallback_api_mode)
    primary_metric = normalize_lottery_primary_metric(lottery_type, data.get('primary_metric') or fallback_primary_metric)
    profit_default_metric = normalize_lottery_profit_metric(lottery_type, data.get('profit_default_metric') or fallback_profit_default_metric)
    profit_rule_id = normalize_lottery_profit_rule(lottery_type, data.get('profit_rule_id') or fallback_profit_rule_id)
    share_level = normalize_share_level(data.get('share_level') or fallback_share_level)
    prediction_method = str(data.get('prediction_method') or fallback_method).strip()
    system_prompt = str(data.get('system_prompt') or fallback_prompt).strip()
    data_injection_mode = normalize_injection_mode(data.get('data_injection_mode') or fallback_injection_mode)
    user_algorithm_fallback_strategy = str(
        data.get('user_algorithm_fallback_strategy') or fallback_user_algorithm_fallback_strategy or 'fail'
    ).strip().lower()
    if user_algorithm_fallback_strategy not in {'fail', 'builtin_baseline', 'skip'}:
        user_algorithm_fallback_strategy = 'fail'
    history_window = data.get('history_window', existing_predictor.get('history_window') if existing_predictor else config.DEFAULT_HISTORY_WINDOW)
    temperature = data.get('temperature', existing_predictor.get('temperature') if existing_predictor else config.DEFAULT_PREDICTION_TEMPERATURE)
    enabled = _parse_bool(data.get('enabled'), existing_predictor.get('enabled') if existing_predictor else True)
    prediction_targets = normalize_prediction_targets(
        lottery_type,
        data.get('prediction_targets', existing_predictor.get('prediction_targets') if existing_predictor else None)
    )

    if not name:
        errors.append('方案名称不能为空')

    if existing_predictor is None and not api_key:
        if engine_type == 'ai':
            errors.append('API Key 不能为空')

    if existing_predictor is not None and not api_key:
        api_key = existing_predictor.get('api_key', '')

    if engine_type == 'ai' and not api_url:
        errors.append('API 地址不能为空')

    if engine_type == 'ai' and not model_name:
        errors.append('模型名称不能为空')
    if engine_type == 'machine' and not algorithm_key:
        errors.append('机器算法不能为空')
    if engine_type == 'machine' and is_user_algorithm_key(algorithm_key):
        user_algorithm_id = get_user_algorithm_id(algorithm_key)
        user_algorithm = db.get_user_algorithm_for_user(user_algorithm_id, user_id) if user_id and user_algorithm_id else None
        if not user_algorithm:
            errors.append('无权使用此用户算法')
        elif user_algorithm.get('lottery_type') != lottery_type:
            errors.append('用户算法彩种与预测方案彩种不一致')
        elif user_algorithm.get('status') != 'validated':
            errors.append('用户算法尚未通过校验，不能用于预测方案')
        elif lottery_type != 'jingcai_football':
            errors.append('当前仅支持竞彩足球用户算法绑定预测方案')

    if primary_metric not in prediction_targets:
        errors.append('主玩法必须包含在预测目标中')
    if lottery_type == 'pc28' and primary_metric in {'double_group', 'kill_group'} and 'combo' not in prediction_targets:
        errors.append('双组/杀组统计依赖组合预测，请勾选组合目标')
    if supports_profit_simulation(lottery_type) and profit_default_metric not in prediction_targets:
        errors.append('默认收益玩法必须包含在预测目标中')

    try:
        history_window = int(history_window)
    except (TypeError, ValueError):
        history_window = config.DEFAULT_HISTORY_WINDOW
    history_window = max(10, min(history_window, 200))

    try:
        temperature = float(temperature)
    except (TypeError, ValueError):
        temperature = config.DEFAULT_PREDICTION_TEMPERATURE
    temperature = max(0.0, min(temperature, 1.0))

    payload = {
        'name': name,
        'engine_type': engine_type,
        'algorithm_key': algorithm_key,
        'api_key': api_key,
        'api_url': api_url,
        'model_name': model_name,
        'api_mode': api_mode,
        'primary_metric': primary_metric,
        'profit_default_metric': profit_default_metric,
        'profit_rule_id': profit_rule_id,
        'share_level': share_level,
        'share_predictions': share_level != 'stats_only',
        'prediction_method': prediction_method or '自定义策略',
        'system_prompt': system_prompt,
        'data_injection_mode': data_injection_mode,
        'user_algorithm_fallback_strategy': user_algorithm_fallback_strategy,
        'prediction_targets': prediction_targets,
        'history_window': history_window,
        'temperature': temperature,
        'enabled': enabled,
        'lottery_type': lottery_type
    }
    return payload, errors


def _validate_user_algorithm_payload(data: dict, existing_algorithm: dict | None = None) -> tuple[dict, list[str]]:
    errors = []
    fallback_lottery_type = existing_algorithm.get('lottery_type') if existing_algorithm else 'pc28'
    fallback_name = existing_algorithm.get('name') if existing_algorithm else ''
    fallback_description = existing_algorithm.get('description') if existing_algorithm else ''
    fallback_definition = existing_algorithm.get('definition') if existing_algorithm else {}

    lottery_type = normalize_lottery_type(data.get('lottery_type') or fallback_lottery_type)
    name = str(data.get('name') or fallback_name).strip()
    description = str(data.get('description') or fallback_description).strip()
    definition = data.get('definition', fallback_definition)
    if isinstance(definition, str):
        try:
            definition = json.loads(definition)
        except json.JSONDecodeError:
            definition = {}
            errors.append('算法定义必须是有效 JSON')
    if not isinstance(definition, dict):
        definition = {}
        errors.append('算法定义必须是 JSON 对象')

    if not name:
        errors.append('算法名称不能为空')

    validation = validate_algorithm_definition(definition, lottery_type=lottery_type)
    status = 'validated' if validation['valid'] and not errors else 'draft'
    payload = {
        'lottery_type': lottery_type,
        'name': name,
        'description': description,
        'definition': validation['normalized_definition'] if validation['normalized_definition'] else definition,
        'validation': validation,
        'status': status,
        'change_summary': str(data.get('change_summary') or '').strip()
    }
    return payload, errors


def _serialize_user_algorithm(item: dict | None) -> dict:
    if not item:
        return {}
    active_version = int(item.get('active_version') or 1)
    active_version_row = db.get_user_algorithm_version_for_user(item['id'], item['user_id'], active_version)
    return {
        'id': item['id'],
        'key': item.get('key') or f"user:{item['id']}",
        'user_id': item['user_id'],
        'lottery_type': item['lottery_type'],
        'name': item['name'],
        'description': item.get('description') or '',
        'algorithm_type': item.get('algorithm_type') or 'dsl',
        'definition': item.get('definition') or {},
        'status': item.get('status') or 'draft',
        'active_version': active_version,
        'active_backtest': (active_version_row or {}).get('backtest') or {},
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None,
        'updated_at': utc_to_beijing(item['updated_at']) if item.get('updated_at') else None
    }


def _serialize_user_algorithm_version(item: dict | None, active_version: int | None = None) -> dict:
    if not item:
        return {}
    version = int(item.get('version') or 0)
    return {
        'id': item['id'],
        'algorithm_id': item['algorithm_id'],
        'version': version,
        'is_active': version == int(active_version or 0),
        'change_summary': item.get('change_summary') or '',
        'definition': item.get('definition') or {},
        'validation': item.get('validation') or {},
        'backtest': item.get('backtest') or {},
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None
    }


def _serialize_user_algorithm_execution_log(item: dict | None) -> dict:
    if not item:
        return {}
    return {
        'id': item['id'],
        'user_id': item.get('user_id'),
        'algorithm_id': item.get('algorithm_id'),
        'algorithm_version': item.get('algorithm_version'),
        'predictor_id': item.get('predictor_id'),
        'run_key': item.get('run_key') or '',
        'status': item.get('status') or '',
        'match_count': int(item.get('match_count') or 0),
        'prediction_count': int(item.get('prediction_count') or 0),
        'skip_count': int(item.get('skip_count') or 0),
        'duration_ms': int(item.get('duration_ms') or 0),
        'fallback_strategy': item.get('fallback_strategy') or 'fail',
        'fallback_used': bool(item.get('fallback_used')),
        'error_message': item.get('error_message') or '',
        'debug': item.get('debug') or {},
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None
    }


def _build_user_algorithm_version_comparison(versions: list[dict]) -> dict:
    ordered = sorted(versions or [], key=lambda item: int(item.get('version') or 0))
    rows = []
    previous_summary = None
    for item in ordered:
        summary = _summarize_user_algorithm_backtest(item.get('backtest') or {})
        row = {
            'version': int(item.get('version') or 0),
            'change_summary': item.get('change_summary') or '',
            'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None,
            'summary': summary,
            'delta_from_previous': _build_backtest_summary_delta(summary, previous_summary) if previous_summary else {}
        }
        rows.append(row)
        previous_summary = summary
    return {
        'rows': list(reversed(rows)),
        'best_version': _resolve_best_user_algorithm_version(rows),
        'baseline_version': rows[0]['version'] if rows else None
    }


def _summarize_user_algorithm_backtest(backtest: dict) -> dict:
    confidence = backtest.get('confidence_report') or {}
    data_quality = backtest.get('data_quality') or {}
    return {
        'sample_size': int(backtest.get('sample_size') or 0),
        'effective_sample_count': int(backtest.get('effective_sample_count') or 0),
        'prediction_count': int(backtest.get('prediction_count') or 0),
        'skip_rate': backtest.get('skip_rate'),
        'confidence_level': confidence.get('level') or 'unknown',
        'confidence_label': confidence.get('label') or '--',
        'confidence_score': confidence.get('score'),
        'field_completeness_rate': data_quality.get('field_completeness_rate'),
        'targets': {
            target: {
                'hit_rate': (backtest.get('hit_rate') or {}).get(target, {}).get('hit_rate'),
                'ratio_text': (backtest.get('hit_rate') or {}).get(target, {}).get('ratio_text'),
                'roi': (backtest.get('profit_summary') or {}).get(target, {}).get('roi'),
                'net_profit': (backtest.get('profit_summary') or {}).get(target, {}).get('net_profit'),
                'max_drawdown': (backtest.get('max_drawdown') or {}).get(target, {}).get('amount')
            }
            for target in (backtest.get('targets') or ['spf', 'rqspf'])
        }
    }


def _build_backtest_summary_delta(current: dict, previous: dict | None) -> dict:
    if not previous:
        return {}
    return {
        'sample_size': current.get('sample_size', 0) - previous.get('sample_size', 0),
        'effective_sample_count': current.get('effective_sample_count', 0) - previous.get('effective_sample_count', 0),
        'prediction_count': current.get('prediction_count', 0) - previous.get('prediction_count', 0),
        'skip_rate': _numeric_delta(current.get('skip_rate'), previous.get('skip_rate')),
        'confidence_score': _numeric_delta(current.get('confidence_score'), previous.get('confidence_score')),
        'field_completeness_rate': _numeric_delta(current.get('field_completeness_rate'), previous.get('field_completeness_rate')),
        'targets': {
            target: {
                'hit_rate': _numeric_delta(values.get('hit_rate'), (previous.get('targets') or {}).get(target, {}).get('hit_rate')),
                'roi': _numeric_delta(values.get('roi'), (previous.get('targets') or {}).get(target, {}).get('roi')),
                'net_profit': _numeric_delta(values.get('net_profit'), (previous.get('targets') or {}).get(target, {}).get('net_profit')),
                'max_drawdown': _numeric_delta(values.get('max_drawdown'), (previous.get('targets') or {}).get(target, {}).get('max_drawdown'))
            }
            for target, values in (current.get('targets') or {}).items()
        }
    }


def _resolve_best_user_algorithm_version(rows: list[dict]) -> dict | None:
    candidates = []
    for row in rows:
        summary = row.get('summary') or {}
        if summary.get('effective_sample_count', 0) < 20:
            continue
        target_values = summary.get('targets') or {}
        best_hit_rate = max(
            [
                value.get('hit_rate')
                for value in target_values.values()
                if value.get('hit_rate') is not None
            ] or [None]
        )
        if best_hit_rate is None:
            continue
        candidates.append((summary.get('confidence_score') or 0, best_hit_rate, -float(summary.get('skip_rate') or 0), row))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[:3], reverse=True)
    best = candidates[0][3]
    return {
        'version': best.get('version'),
        'reason': '按可信度评分、最高命中率和较低跳过率综合选择。'
    }


def _serialize_bound_user_algorithm_predictor(predictor: dict) -> dict:
    runtime_status, runtime_status_label = _resolve_predictor_runtime_status(predictor)
    return {
        'id': predictor.get('id'),
        'name': predictor.get('name') or '',
        'enabled': bool(predictor.get('enabled')),
        'auto_paused': bool(predictor.get('auto_paused')),
        'runtime_status': runtime_status,
        'runtime_status_label': runtime_status_label,
        'fallback_strategy': predictor.get('user_algorithm_fallback_strategy') or 'fail',
        'prediction_targets': normalize_prediction_targets('jingcai_football', predictor.get('prediction_targets')),
        'updated_at': utc_to_beijing(predictor['updated_at']) if predictor.get('updated_at') else None
    }


def _resolve_recent_user_algorithm_backtest(algorithm_id: int, user_id: int, active_version: int) -> dict:
    versions = db.get_user_algorithm_versions_for_user(algorithm_id, user_id)
    active_row = next((item for item in versions if int(item.get('version') or 0) == active_version), None)
    candidates = [active_row] if active_row else []
    candidates.extend(item for item in versions if item is not active_row)
    for item in candidates:
        backtest = (item or {}).get('backtest') or {}
        if backtest:
            return {
                'version': int(item.get('version') or 0),
                'backtest': backtest,
                'summary': _summarize_user_algorithm_backtest(backtest),
                'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None
            }
    return {
        'version': active_version,
        'backtest': {},
        'summary': _summarize_user_algorithm_backtest({}),
        'created_at': None
    }


def _build_user_algorithm_risk_summary(backtest: dict, logs: list[dict], bound_predictors: list[dict]) -> dict:
    confidence = backtest.get('confidence_report') or {}
    risk_flags = list(backtest.get('risk_flags') or [])
    failed_logs = [item for item in logs if item.get('status') in {'failed', 'fallback_succeeded'}]
    if failed_logs:
        risk_flags.append('最近执行出现失败或降级，请优先检查执行日志。')
    if not bound_predictors:
        risk_flags.append('当前算法还没有绑定预测方案，无法形成生产执行闭环。')
    if not backtest:
        risk_flags.append('当前版本暂无回测结果。')
    risk_flags = list(dict.fromkeys(risk_flags))

    level = confidence.get('level') or 'unknown'
    score = confidence.get('score')
    if failed_logs and level == 'high':
        level = 'medium'
    if not backtest or not bound_predictors or failed_logs:
        label = '需关注'
    else:
        label = confidence.get('label') or '待观察'

    return {
        'level': level,
        'label': label,
        'score': score,
        'flags': risk_flags,
        'recent_failure_count': len(failed_logs),
        'bound_predictor_count': len(bound_predictors)
    }


def _build_user_algorithm_ops_summary(algorithm: dict, user_id: int) -> dict:
    active_version = int(algorithm.get('active_version') or 1)
    versions = [
        _serialize_user_algorithm_version(item, active_version=active_version)
        for item in db.get_user_algorithm_versions_for_user(algorithm['id'], user_id)
    ]
    recent_backtest = _resolve_recent_user_algorithm_backtest(algorithm['id'], user_id, active_version)
    logs = [
        _serialize_user_algorithm_execution_log(item)
        for item in db.get_user_algorithm_execution_logs_for_user(algorithm['id'], user_id, limit=10)
    ]
    bound_predictors = [
        _serialize_bound_user_algorithm_predictor(item)
        for item in db.get_predictors_using_user_algorithm(user_id, algorithm['id'])
    ]
    return {
        'algorithm': _serialize_user_algorithm(algorithm),
        'versions': {
            'total': len(versions),
            'active_version': active_version,
            'recent': versions[:5]
        },
        'recent_backtest': recent_backtest,
        'recent_execution_logs': logs,
        'bound_predictors': bound_predictors,
        'risk_summary': _build_user_algorithm_risk_summary(
            recent_backtest.get('backtest') or {},
            logs,
            bound_predictors
        )
    }


def _build_compare_backtest_metrics(backtest: dict) -> dict:
    summary = _summarize_user_algorithm_backtest(backtest)
    return {
        'sample_size': summary['sample_size'],
        'effective_sample_count': summary['effective_sample_count'],
        'prediction_count': summary['prediction_count'],
        'skip_rate': summary['skip_rate'],
        'field_completeness_rate': summary['field_completeness_rate'],
        'targets': summary['targets'],
        'odds_interval_performance': backtest.get('odds_interval_performance') or {}
    }


def _build_odds_interval_delta(base_backtest: dict, candidate_backtest: dict) -> dict:
    result = {}
    base_map = base_backtest.get('odds_interval_performance') or {}
    candidate_map = candidate_backtest.get('odds_interval_performance') or {}
    for target in sorted(set(base_map) | set(candidate_map)):
        base_ranges = {item.get('range'): item for item in base_map.get(target, [])}
        candidate_ranges = {item.get('range'): item for item in candidate_map.get(target, [])}
        result[target] = [
            {
                'range': range_key,
                'sample_count': int((candidate_ranges.get(range_key) or {}).get('sample_count') or 0)
                - int((base_ranges.get(range_key) or {}).get('sample_count') or 0),
                'hit_rate': _numeric_delta(
                    (candidate_ranges.get(range_key) or {}).get('hit_rate'),
                    (base_ranges.get(range_key) or {}).get('hit_rate')
                )
            }
            for range_key in sorted(set(base_ranges) | set(candidate_ranges))
            if range_key
        ]
    return result


def _build_user_algorithm_compare_payload(
    base_version_row: dict,
    candidate_version_row: dict,
    base_backtest: dict,
    candidate_backtest: dict,
    filters: dict
) -> dict:
    base_metrics = _build_compare_backtest_metrics(base_backtest)
    candidate_metrics = _build_compare_backtest_metrics(candidate_backtest)
    return {
        'base_version': int(base_version_row.get('version') or 0),
        'candidate_version': int(candidate_version_row.get('version') or 0),
        'filters': filters,
        'base': base_metrics,
        'candidate': candidate_metrics,
        'delta': {
            **_build_backtest_summary_delta(candidate_metrics, base_metrics),
            'odds_interval_performance': _build_odds_interval_delta(base_backtest, candidate_backtest)
        }
    }


def _diagnosis_status(level: str, label: str, findings: list[str]) -> dict:
    return {
        'level': level,
        'label': label,
        'findings': findings or ['未触发明显风险。']
    }


def _build_user_algorithm_diagnosis(backtest: dict) -> dict:
    summary = _summarize_user_algorithm_backtest(backtest)
    sample_findings = list((backtest.get('confidence_report') or {}).get('reasons') or [])
    sample_level = 'ok'
    if summary['effective_sample_count'] < 20:
        sample_level = 'high_risk'
    elif summary['effective_sample_count'] < 50:
        sample_level = 'watch'

    skip_rate = summary.get('skip_rate')
    skip_findings = [
        f"{item.get('label') or key}: {item.get('count')}"
        for key, item in (backtest.get('skip_reason_stats') or {}).items()
    ]
    skip_level = 'ok'
    if skip_rate is not None and skip_rate >= 70:
        skip_level = 'high_risk'
    elif skip_rate is not None and skip_rate >= 50:
        skip_level = 'watch'

    odds_findings = []
    for target, intervals in (backtest.get('odds_interval_performance') or {}).items():
        active = [item for item in intervals if int(item.get('sample_count') or 0) > 0]
        if not active:
            odds_findings.append(f'{target.upper()} 暂无可分析赔率区间。')
            continue
        odds_findings.extend(
            f"{target.upper()} {item.get('range')}: {item.get('hit_rate') if item.get('hit_rate') is not None else '--'}% / {item.get('sample_count')} 场"
            for item in active
        )

    drawdown_findings = []
    drawdown_level = 'ok'
    for target, item in (backtest.get('max_drawdown') or {}).items():
        amount = item.get('amount')
        drawdown_findings.append(f"{target.upper()} 最大回撤 {amount if amount is not None else '--'}")
        if amount is not None and float(amount) >= 5:
            drawdown_level = 'watch'

    data_quality = backtest.get('data_quality') or {}
    completeness = data_quality.get('field_completeness_rate')
    data_quality_findings = [
        f"字段完整率 {completeness if completeness is not None else '--'}%",
        *[
            f'{field}: 缺失 {count} 场'
            for field, count in (data_quality.get('missing_field_stats') or {}).items()
        ]
    ]
    data_quality_level = 'ok'
    if completeness is None or completeness < 60:
        data_quality_level = 'high_risk'
    elif completeness < 80:
        data_quality_level = 'watch'

    actions = []
    if sample_level != 'ok':
        actions.append('先补齐更多已开奖历史样本，再判断算法优劣。')
    if skip_level != 'ok':
        actions.append('检查过滤条件和 min_confidence，优先降低无效跳过。')
    if data_quality_level != 'ok':
        actions.append('补齐近期战绩、赔率和伤停字段，避免评分退化为少数字段。')
    if drawdown_level != 'ok':
        actions.append('降低高波动赔率区间权重，或提高出手门槛。')
    if not actions:
        actions.append('保持当前 DSL 简洁，继续用同一筛选条件观察新版本表现。')

    return {
        'sample_quality': _diagnosis_status(sample_level, '样本质量', sample_findings),
        'skip_analysis': _diagnosis_status(skip_level, '跳过分析', skip_findings),
        'odds_analysis': _diagnosis_status('ok' if odds_findings else 'watch', '赔率区间', odds_findings),
        'drawdown_analysis': _diagnosis_status(drawdown_level, '回撤分析', drawdown_findings),
        'data_quality_analysis': _diagnosis_status(data_quality_level, '数据质量', data_quality_findings),
        'recommended_actions': actions
    }


def _numeric_delta(current, previous):
    if current is None or previous is None:
        return None
    try:
        return round(float(current) - float(previous), 2)
    except (TypeError, ValueError):
        return None


def _build_user_algorithm_backtest_filters(data: dict, definition: dict | None = None) -> dict:
    payload = data.get('filters') if isinstance(data.get('filters'), dict) else {}
    payload = {
        **payload,
        'start_date': data.get('start_date', payload.get('start_date')),
        'end_date': data.get('end_date', payload.get('end_date')),
        'recent_n': data.get('recent_n', payload.get('recent_n')),
        'leagues': data.get('leagues', data.get('league', payload.get('leagues'))),
        'market_type': data.get('market_type', data.get('target', payload.get('market_type'))),
        'targets': data.get('targets', payload.get('targets'))
    }
    if not payload.get('targets') and isinstance(definition, dict):
        payload['targets'] = definition.get('targets') or []
    return payload


def _check_user_algorithm_backtest_rate_limit(user_id: int) -> tuple[bool, int]:
    if USER_ALGORITHM_BACKTEST_COOLDOWN_SECONDS <= 0:
        return True, 0
    now = time.time()
    last_run_at = _user_algorithm_backtest_last_run_at.get(user_id, 0)
    wait_seconds = int(USER_ALGORITHM_BACKTEST_COOLDOWN_SECONDS - (now - last_run_at))
    if wait_seconds > 0:
        return False, wait_seconds
    _user_algorithm_backtest_last_run_at[user_id] = now
    return True, 0


def _build_user_algorithm_sample_matches() -> list[dict]:
    return [{
        'event_key': 'SAMPLE001',
        'match_no': '周五001',
        'league': '英超',
        'home_team': '样例主队',
        'away_team': '样例客队',
        'team1_id': 'sample-home',
        'team2_id': 'sample-away',
        'match_time': '2026-04-24 20:00',
        'spf_odds': {'胜': 1.62, '平': 3.75, '负': 5.20},
        'rqspf': {
            'handicap': -1,
            'handicap_text': '-1',
            'odds': {'胜': 2.95, '平': 3.35, '负': 2.10}
        },
        'detail_bundle': {
            'recent_form_team1': [
                {'team1': '样例主队', 'team2': '甲队', 'team1Id': 'sample-home', 'score1': '3', 'score2': '1'},
                {'team1': '乙队', 'team2': '样例主队', 'team2Id': 'sample-home', 'score1': '0', 'score2': '2'},
                {'team1': '样例主队', 'team2': '丙队', 'team1Id': 'sample-home', 'score1': '2', 'score2': '0'}
            ],
            'recent_form_team2': [
                {'team1': '样例客队', 'team2': '丁队', 'team1Id': 'sample-away', 'score1': '0', 'score2': '1'},
                {'team1': '戊队', 'team2': '样例客队', 'team2Id': 'sample-away', 'score1': '2', 'score2': '1'},
                {'team1': '样例客队', 'team2': '己队', 'team1Id': 'sample-away', 'score1': '1', 'score2': '1'}
            ],
            'team_table': {
                'team1': {'items': {'all': [{'position': '2', 'points': '68'}]}},
                'team2': {'items': {'all': [{'position': '9', 'points': '46'}]}}
            },
            'injury': {
                'team1': [],
                'team2': [{'playerShortName': '样例客队主力', 'typeCn': '伤'}]
            },
            'odds_snapshots': {
                'euro': {
                    'initial': {'win': '1.72', 'draw': '3.55', 'lose': '4.60'},
                    'current': {'win': '1.62', 'draw': '3.75', 'lose': '5.20'}
                }
            }
        }
    }]


def _get_predictor_dashboard_data(predictor_id: int) -> dict:
    predictor = db.get_predictor(predictor_id, include_secret=True)
    if not predictor:
        raise ValueError('预测方案不存在')

    if normalize_lottery_type(predictor.get('lottery_type')) == 'jingcai_football':
        dashboard = lottery_runtime.build_dashboard_data(predictor_id, _get_pc28_predictor_dashboard_data)
        return {
            'predictor': _serialize_predictor(dashboard['predictor']),
            'stats': dashboard.get('stats') or {},
            'current_prediction': _serialize_prediction_run(dashboard.get('current_prediction')),
            'latest_prediction': _serialize_prediction_run(dashboard.get('latest_prediction')),
            'recent_predictions': [_serialize_prediction_run(item) for item in dashboard.get('recent_predictions', [])],
            'recent_prediction_items': [_serialize_prediction_item_group(item) for item in dashboard.get('recent_prediction_items', [])],
            'recent_draws': [_serialize_lottery_event(item) for item in dashboard.get('recent_draws', [])],
            'overview': {
                **(dashboard.get('overview') or {}),
                'recent_events': [_serialize_lottery_event(item) for item in (dashboard.get('overview') or {}).get('recent_events', [])]
            }
        }

    return _get_pc28_predictor_dashboard_data(predictor_id)


def _get_predictor_history_data(
    predictor_id: int,
    tab: str = 'predictions',
    page: int = 1,
    page_size: int | None = None,
    status: str = 'all',
    outcome: str = 'all'
) -> dict:
    predictor = db.get_predictor(predictor_id, include_secret=True)
    if not predictor:
        raise ValueError('预测方案不存在')

    lottery_type = normalize_lottery_type(predictor.get('lottery_type'))
    serialized_predictor = _serialize_predictor(predictor)
    normalized_tab = str(tab or 'predictions').strip().lower()
    if normalized_tab not in {'predictions', 'draws', 'ai'}:
        normalized_tab = 'predictions'
    normalized_status = str(status or 'all').strip().lower()
    normalized_outcome = str(outcome or 'all').strip().lower()
    default_page_sizes = {'predictions': 50, 'draws': 50, 'ai': 20}
    safe_page_size = _parse_positive_int(page_size, default_page_sizes[normalized_tab], minimum=10, maximum=100)
    if lottery_type == 'jingcai_football':
        handler = lottery_runtime.get_handler(lottery_type)
        include_raw = normalized_tab == 'ai'
        if normalized_tab in {'predictions', 'ai'}:
            if normalized_tab == 'predictions':
                all_items = handler.get_recent_prediction_items(db, predictor_id, limit=None) if handler else []
                filtered_items = [
                    item for item in all_items
                    if (normalized_status == 'all' or item.get('status') == normalized_status)
                    and _prediction_matches_outcome_filter(item, normalized_outcome)
                ]
                pagination = _build_pagination_payload(page, safe_page_size, len(filtered_items))
                start = (pagination['page'] - 1) * pagination['page_size']
                prediction_items = filtered_items[start:start + pagination['page_size']]
            else:
                total = db.count_prediction_items(predictor_id, lottery_type=lottery_type)
                pagination = _build_pagination_payload(page, safe_page_size, total)
                offset = (pagination['page'] - 1) * pagination['page_size']
                prediction_items = handler.get_recent_prediction_items(db, predictor_id, limit=pagination['page_size'], offset=offset) if handler else []
        else:
            prediction_items = []
            pagination = _build_pagination_payload(page, safe_page_size, db.count_lottery_events(lottery_type))
        draws = (
            db.get_recent_lottery_events(
                lottery_type,
                limit=pagination['page_size'],
                offset=(pagination['page'] - 1) * pagination['page_size']
            )
            if normalized_tab == 'draws'
            else []
        )
        return {
            'predictor': serialized_predictor,
            'active_tab': normalized_tab,
            'filters': {
                'status': normalized_status,
                'outcome': normalized_outcome
            },
            'pagination': pagination,
            'recent_predictions': [_serialize_prediction_item_group(item, include_raw=include_raw) for item in prediction_items],
            'recent_draws': [_serialize_lottery_event(item) for item in draws]
        }

    include_raw = normalized_tab == 'ai'
    if normalized_tab in {'predictions', 'ai'}:
        if normalized_tab == 'predictions':
            all_predictions = db.get_recent_predictions(predictor_id, limit=None)
            filtered_predictions = [
                item for item in all_predictions
                if (normalized_status == 'all' or item.get('status') == normalized_status)
                and _prediction_matches_outcome_filter(item, normalized_outcome)
            ]
            pagination = _build_pagination_payload(page, safe_page_size, len(filtered_predictions))
            start = (pagination['page'] - 1) * pagination['page_size']
            recent_predictions = filtered_predictions[start:start + pagination['page_size']]
        else:
            total = db.count_predictions(predictor_id)
            pagination = _build_pagination_payload(page, safe_page_size, total)
            offset = (pagination['page'] - 1) * pagination['page_size']
            recent_predictions = db.get_recent_predictions(predictor_id, limit=pagination['page_size'], offset=offset)
    else:
        total = db.count_draws(lottery_type)
        pagination = _build_pagination_payload(page, safe_page_size, total)
        recent_predictions = []
    draws = (
        db.get_recent_draws(
            lottery_type,
            limit=pagination['page_size'],
            offset=(pagination['page'] - 1) * pagination['page_size']
        )
        if normalized_tab == 'draws'
        else []
    )
    return {
        'predictor': serialized_predictor,
        'active_tab': normalized_tab,
        'filters': {
            'status': normalized_status,
            'outcome': normalized_outcome
        },
        'pagination': pagination,
        'recent_predictions': [_serialize_prediction(item, include_raw=include_raw) for item in recent_predictions],
        'recent_draws': [_serialize_draw(draw) for draw in draws]
    }


def _get_pc28_predictor_dashboard_data(predictor_id: int) -> dict:
    predictor = db.get_predictor(predictor_id, include_secret=True)
    stats = db.get_predictor_stats(predictor_id)
    recent_predictions = db.get_recent_predictions(predictor_id, limit=100)
    current_prediction = next((item for item in recent_predictions if item['status'] == 'pending'), None)
    latest_prediction = recent_predictions[0] if recent_predictions else None
    draws = db.get_recent_draws('pc28', limit=20)

    if not draws:
        try:
            synced_draws = pc28_service.sync_recent_draws(db, limit=30)
            draws = synced_draws[:20]
        except Exception:
            draws = []

    try:
        overview = pc28_service.build_overview(history_limit=20)
        if overview.get('recent_draws'):
            db.upsert_draws('pc28', overview['recent_draws'])
    except Exception:
        latest_draw = draws[0] if draws else None
        overview = {
            'latest_draw': _serialize_draw(latest_draw) if latest_draw else None,
            'next_issue_no': None,
            'countdown': '--:--:--',
            'recent_draws': [_serialize_draw(draw) for draw in draws],
            'warning': '官方接口不可用，已回退本地缓存'
        }

    return {
        'predictor': _serialize_predictor(predictor),
        'stats': stats,
        'current_prediction': _serialize_prediction(current_prediction) if current_prediction else None,
        'latest_prediction': _serialize_prediction(latest_prediction) if latest_prediction else None,
        'recent_predictions': [_serialize_prediction(item) for item in recent_predictions],
        'recent_draws': [_serialize_draw(draw) for draw in draws],
        'overview': _serialize_overview(overview)
    }


def _resolve_predictor_form_context(user_id: int, data: dict) -> tuple[dict | None, dict]:
    predictor_id = data.get('predictor_id')
    existing = None
    if predictor_id:
        predictor_id = int(predictor_id)
        if not db.predictor_exists_for_user(predictor_id, user_id):
            raise PermissionError('无权访问此预测方案')
        existing = db.get_predictor(predictor_id, include_secret=True)

    fallback_name = existing.get('name') if existing else ''
    fallback_engine_type = existing.get('engine_type') if existing else 'ai'
    fallback_algorithm_key = existing.get('algorithm_key') if existing else ''
    fallback_api_url = existing.get('api_url') if existing else ''
    fallback_model_name = existing.get('model_name') if existing else ''
    fallback_api_mode = existing.get('api_mode') if existing else 'auto'
    lottery_type = normalize_lottery_type(data.get('lottery_type') or (existing.get('lottery_type') if existing else 'pc28'))
    lottery_definition = get_lottery_definition(lottery_type)
    fallback_primary_metric = existing.get('primary_metric') if existing else lottery_definition.primary_metric_options[0][0]
    fallback_profit_default_metric = existing.get('profit_default_metric') if existing else fallback_primary_metric
    fallback_profit_rule_id = existing.get('profit_rule_id') if existing else DEFAULT_PROFIT_RULE_ID
    fallback_method = existing.get('prediction_method') if existing else ''
    fallback_prompt = existing.get('system_prompt') if existing else ''
    fallback_injection_mode = existing.get('data_injection_mode') if existing else 'summary'
    fallback_targets = existing.get('prediction_targets') if existing else None
    fallback_history_window = existing.get('history_window') if existing else config.DEFAULT_HISTORY_WINDOW

    history_window = data.get('history_window', fallback_history_window)
    try:
        history_window = int(history_window)
    except (TypeError, ValueError):
        history_window = config.DEFAULT_HISTORY_WINDOW
    history_window = max(10, min(history_window, 200))

    resolved = {
        'predictor_id': predictor_id,
        'name': str(data.get('name') or fallback_name).strip(),
        'lottery_type': lottery_type,
        'engine_type': normalize_engine_type(data.get('engine_type') or fallback_engine_type),
        'algorithm_key': normalize_algorithm_key(
            lottery_type,
            data.get('engine_type') or fallback_engine_type,
            data.get('algorithm_key') or fallback_algorithm_key
        ),
        'api_key': str(data.get('api_key') or '').strip() or (existing.get('api_key') if existing else ''),
        'api_url': str(data.get('api_url') or fallback_api_url).strip(),
        'model_name': str(data.get('model_name') or fallback_model_name).strip(),
        'api_mode': normalize_api_mode(data.get('api_mode') or fallback_api_mode),
        'primary_metric': normalize_lottery_primary_metric(lottery_type, data.get('primary_metric') or fallback_primary_metric),
        'profit_default_metric': normalize_lottery_profit_metric(lottery_type, data.get('profit_default_metric') or fallback_profit_default_metric),
        'profit_rule_id': normalize_lottery_profit_rule(lottery_type, data.get('profit_rule_id') or fallback_profit_rule_id),
        'prediction_method': str(data.get('prediction_method') or fallback_method).strip(),
        'system_prompt': str(data.get('system_prompt') or fallback_prompt).strip(),
        'data_injection_mode': normalize_injection_mode(data.get('data_injection_mode') or fallback_injection_mode),
        'prediction_targets': normalize_prediction_targets(lottery_type, data.get('prediction_targets', fallback_targets)),
        'history_window': history_window
    }
    if resolved['engine_type'] == 'machine' and is_user_algorithm_key(resolved['algorithm_key']):
        user_algorithm_id = get_user_algorithm_id(resolved['algorithm_key'])
        user_algorithm = db.get_user_algorithm_for_user(user_algorithm_id, user_id) if user_algorithm_id else None
        if not user_algorithm:
            raise PermissionError('无权使用此用户算法')
        if user_algorithm.get('lottery_type') != lottery_type:
            raise PermissionError('用户算法彩种与预测方案彩种不一致')
        if user_algorithm.get('status') != 'validated':
            raise PermissionError('用户算法尚未通过校验，不能用于预测方案')
        resolved['user_algorithm'] = user_algorithm

    return existing, resolved


@app.route('/image/<path:filename>')
def serve_image(filename):
    from flask import send_from_directory
    return send_from_directory('image', filename)


@app.route('/')
def index():
    lottery_cards = [
        {
            **_get_public_lottery_page_context(lottery_type),
            'capabilities': get_lottery_definition(lottery_type).to_catalog_item()['capabilities']
        }
        for lottery_type in PUBLIC_LOTTERY_PAGE_CONFIG.keys()
    ]
    return render_template('home.html', lottery_cards=lottery_cards)


@app.route('/<string:lottery_slug>')
def public_lottery_page(lottery_slug: str):
    slug_mapping = {
        item_config['slug']: lottery_type
        for lottery_type, item_config in PUBLIC_LOTTERY_PAGE_CONFIG.items()
    }
    lottery_type = slug_mapping.get(lottery_slug)
    if not lottery_type:
        return redirect('/')
    return render_template('public_lottery_home.html', **_get_public_lottery_page_context(lottery_type))


@app.route('/login')
def login_page():
    return render_template('login.html')


@app.route('/public/predictors/<int:predictor_id>')
def public_predictor_page(predictor_id: int):
    detail = _get_public_predictor_detail(predictor_id)
    if not detail:
        return render_template('public_predictor.html', predictor=None), 404
    return render_template('public_predictor.html', predictor=detail['predictor'])


@app.route('/dashboard')
def dashboard():
    if not get_current_user_id():
        return redirect('/login')
    return render_template('dashboard.html', prompt_placeholders=get_prompt_placeholder_catalog('all'))


@app.route('/settings')
def settings_page():
    if not get_current_user_id():
        return redirect('/login')
    return render_template('settings.html')


@app.route('/predictors/<int:predictor_id>/history')
def predictor_history_page(predictor_id: int):
    user_id = get_current_user_id()
    if not user_id:
        return redirect('/login')
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return redirect('/dashboard')

    predictor = db.get_predictor(predictor_id, include_secret=False)
    initial_tab = str(request.args.get('tab') or 'predictions').strip().lower()
    if initial_tab not in {'predictions', 'draws', 'ai'}:
        initial_tab = 'predictions'
    return render_template('predictor_history.html', predictor=_serialize_predictor(predictor), initial_tab=initial_tab)


@app.route('/admin')
def admin_page():
    user_id = get_current_user_id()
    if not user_id:
        return redirect('/login')
    if not get_current_user_is_admin():
        user = db.get_user_by_id(user_id)
        if user and user.get('is_admin'):
            set_current_user_with_role(user['id'], user['username'], True)
        else:
            return redirect('/dashboard')
    return render_template('admin.html')


@app.route('/consensus')
def consensus_page():
    """竞彩足球方案共识分析页面（用户视角，只看自己方案）"""
    if not get_current_user_id():
        return redirect('/login')
    return render_template('consensus.html', scope='user')


@app.route('/admin/consensus')
def admin_consensus_page():
    """竞彩足球方案共识分析页面（管理员视角，看全部方案）"""
    user_id = get_current_user_id()
    if not user_id:
        return redirect('/login')
    if not get_current_user_is_admin():
        user = db.get_user_by_id(user_id)
        if user and user.get('is_admin'):
            set_current_user_with_role(user['id'], user['username'], True)
        else:
            return redirect('/consensus')
    return render_template('consensus.html', scope='all')


@app.route('/api/health', methods=['GET'])
def healthcheck():
    return jsonify({
        'status': 'ok',
        'time': get_current_beijing_time_str(),
        'auto_prediction': config.AUTO_PREDICTION
    })


@app.route('/api/lotteries/catalog', methods=['GET'])
def get_lottery_catalog():
    return jsonify(list_lottery_catalog())


# ============ Authentication APIs ============


@app.route('/api/auth/register', methods=['POST'])
def register():
    data = request.get_json() or {}
    username = str(data.get('username') or '').strip()
    password = data.get('password')
    email = str(data.get('email') or '').strip() or None

    if not username or not password:
        return jsonify({'error': '用户名和密码不能为空'}), 400

    existing_user = db.get_user_by_username(username)
    if existing_user:
        return jsonify({'error': '用户名已存在'}), 400

    password_hash = hash_password(password)
    is_admin = db.count_users() == 0
    user_id = db.create_user(username, password_hash, email, is_admin=is_admin)
    set_current_user_with_role(user_id, username, is_admin=is_admin)

    return jsonify({
        'message': '注册成功',
        'user': {
            'id': user_id,
            'username': username,
            'email': email,
            'is_admin': is_admin
        }
    })


@app.route('/api/auth/login', methods=['POST'])
def login():
    data = request.get_json() or {}
    username = str(data.get('username') or '').strip()
    password = data.get('password')

    if not username or not password:
        return jsonify({'error': '用户名和密码不能为空'}), 400

    user = db.get_user_by_username(username)
    if not user or not verify_password(user['password_hash'], password):
        return jsonify({'error': '用户名或密码错误'}), 401

    set_current_user_with_role(user['id'], user['username'], bool(user.get('is_admin')))
    return jsonify({
        'message': '登录成功',
        'user': {
            'id': user['id'],
            'username': user['username'],
            'email': user.get('email'),
            'is_admin': bool(user.get('is_admin'))
        }
    })


@app.route('/api/auth/logout', methods=['POST'])
def logout():
    clear_current_user()
    return jsonify({'message': '登出成功'})


@app.route('/api/auth/linuxdo', methods=['GET'])
def linuxdo_oauth():
    import urllib.parse

    params = {
        'client_id': config.LINUXDO_CLIENT_ID,
        'redirect_uri': config.LINUXDO_REDIRECT_URI,
        'response_type': 'code',
        'scope': 'user'
    }
    auth_url = f"{config.LINUXDO_AUTHORIZE_URL}?{urllib.parse.urlencode(params)}"
    return redirect(auth_url)


@app.route('/api/auth/callback', methods=['GET'])
def linuxdo_callback():
    code = request.args.get('code')
    if not code:
        return jsonify({'error': '授权失败，未获取到授权码'}), 400

    try:
        token_data = {
            'client_id': config.LINUXDO_CLIENT_ID,
            'client_secret': config.LINUXDO_CLIENT_SECRET,
            'code': code,
            'grant_type': 'authorization_code',
            'redirect_uri': config.LINUXDO_REDIRECT_URI
        }
        token_response = requests.post(config.LINUXDO_TOKEN_URL, data=token_data, timeout=10)
        token_response.raise_for_status()
        access_token = token_response.json().get('access_token')
        if not access_token:
            return jsonify({'error': 'OAuth 授权失败，未获取到 access_token'}), 400

        userinfo_response = requests.get(
            config.LINUXDO_USERINFO_URL,
            headers={'Authorization': f'Bearer {access_token}'},
            timeout=10
        )
        userinfo_response.raise_for_status()
        userinfo = userinfo_response.json()

        trust_level = userinfo.get('trust_level', 0)
        if trust_level < config.LINUXDO_MIN_TRUST_LEVEL:
            return jsonify({
                'error': f'您的信任等级为 {trust_level}，需要达到 {config.LINUXDO_MIN_TRUST_LEVEL} 级才能登录'
            }), 403

        linuxdo_id = userinfo.get('id')
        email = userinfo.get('email', '')
        if not linuxdo_id:
            return jsonify({'error': 'OAuth 授权失败，未获取到用户信息'}), 400

        linuxdo_username = f'linuxdo_{linuxdo_id}'
        user = db.get_user_by_username(linuxdo_username)
        if not user:
            is_admin = db.count_users() == 0
            user_id = db.create_user(
                linuxdo_username,
                hash_password(f'linuxdo_oauth_{linuxdo_id}'),
                email,
                is_admin=is_admin
            )
        else:
            user_id = user['id']
            is_admin = bool(user.get('is_admin'))

        set_current_user_with_role(user_id, linuxdo_username, is_admin=is_admin)
        return redirect('/dashboard')
    except requests.RequestException as exc:
        return jsonify({'error': f'OAuth 授权失败: {exc}'}), 500
    except Exception as exc:
        return jsonify({'error': f'登录失败: {exc}'}), 500


@app.route('/api/auth/me', methods=['GET'])
@app.route('/api/user/info', methods=['GET'])
def get_current_user():
    user_id = get_current_user_id()
    if not user_id:
        return jsonify({'error': 'Not logged in'}), 401

    user = db.get_user_by_id(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    set_current_user_with_role(user['id'], user['username'], bool(user.get('is_admin')))
    return jsonify({
        'id': user['id'],
        'username': user['username'],
        'email': user.get('email'),
        'is_admin': bool(user.get('is_admin')),
        'created_at': utc_to_beijing(user['created_at']) if user.get('created_at') else None
    })


@app.route('/api/bet-profiles', methods=['GET'])
@login_required
def get_bet_profiles():
    user_id = get_current_user_id()
    lottery_type = request.args.get('lottery_type')
    items = db.list_bet_profiles(user_id, lottery_type=lottery_type)
    return jsonify([_serialize_bet_profile(item) for item in items])


@app.route('/api/bet-profiles', methods=['POST'])
@login_required
def create_bet_profile():
    user_id = get_current_user_id()
    data = request.get_json() or {}
    payload, errors = _validate_bet_profile_payload(data)
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400

    profile_id = db.create_bet_profile(user_id=user_id, **payload)
    profile = db.get_bet_profile(profile_id)
    return jsonify({
        'message': '下注策略创建成功',
        'item': _serialize_bet_profile(profile)
    })


@app.route('/api/bet-profiles/<int:profile_id>', methods=['PUT'])
@login_required
def update_bet_profile(profile_id: int):
    user_id = get_current_user_id()
    existing = db.get_bet_profile(profile_id)
    if not existing or int(existing.get('user_id') or 0) != int(user_id):
        return jsonify({'error': '下注策略不存在'}), 404

    data = request.get_json() or {}
    payload, errors = _validate_bet_profile_payload(data, existing_profile=existing)
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400

    db.update_bet_profile(profile_id, user_id, payload)
    profile = db.get_bet_profile(profile_id)
    return jsonify({
        'message': '下注策略更新成功',
        'item': _serialize_bet_profile(profile)
    })


@app.route('/api/bet-profiles/<int:profile_id>', methods=['DELETE'])
@login_required
def delete_bet_profile(profile_id: int):
    user_id = get_current_user_id()
    existing = db.get_bet_profile(profile_id)
    if not existing or int(existing.get('user_id') or 0) != int(user_id):
        return jsonify({'error': '下注策略不存在'}), 404
    db.delete_bet_profile(profile_id, user_id)
    return jsonify({'message': '下注策略已删除'})


@app.route('/api/notification-senders', methods=['GET'])
@login_required
def get_notification_senders():
    user_id = get_current_user_id()
    items = db.list_notification_sender_accounts(user_id)
    return jsonify([_serialize_notification_sender_account(item) for item in items])


@app.route('/api/notification-senders', methods=['POST'])
@login_required
def create_notification_sender():
    user_id = get_current_user_id()
    data = request.get_json() or {}
    payload, errors = _validate_notification_sender_account_payload(data)
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400
    sender_id = db.create_notification_sender_account(user_id=user_id, **payload)
    item = db.get_notification_sender_account(sender_id)
    return jsonify({
        'message': '通知发送方创建成功',
        'item': _serialize_notification_sender_account(item)
    })


@app.route('/api/notification-senders/<int:sender_id>', methods=['PUT'])
@login_required
def update_notification_sender(sender_id: int):
    user_id = get_current_user_id()
    existing = db.get_notification_sender_account(sender_id)
    if not existing or int(existing.get('user_id') or 0) != int(user_id):
        return jsonify({'error': '通知发送方不存在'}), 404
    data = request.get_json() or {}
    payload, errors = _validate_notification_sender_account_payload(data, existing_sender=existing)
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400
    db.update_notification_sender_account(sender_id, user_id, payload)
    item = db.get_notification_sender_account(sender_id)
    return jsonify({
        'message': '通知发送方更新成功',
        'item': _serialize_notification_sender_account(item)
    })


@app.route('/api/notification-senders/<int:sender_id>', methods=['DELETE'])
@login_required
def delete_notification_sender(sender_id: int):
    user_id = get_current_user_id()
    existing = db.get_notification_sender_account(sender_id)
    if not existing or int(existing.get('user_id') or 0) != int(user_id):
        return jsonify({'error': '通知发送方不存在'}), 404
    db.delete_notification_sender_account(sender_id, user_id)
    return jsonify({'message': '通知发送方已删除'})


@app.route('/api/notification-senders/test', methods=['POST'])
@login_required
def test_notification_sender():
    user_id = get_current_user_id()
    data = request.get_json() or {}
    sender_id = _parse_optional_int(data.get('sender_id'))
    bot_token = str(data.get('bot_token') or '').strip()
    payload, errors = _validate_notification_sender_account_payload(
        data,
        existing_sender=db.get_notification_sender_account(sender_id) if sender_id else None
    )
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400
    if sender_id:
        sender = db.get_notification_sender_account(sender_id)
        if not sender or int(sender.get('user_id') or 0) != int(user_id):
            return jsonify({'error': '通知发送方不存在'}), 404
    chat_id = str(data.get('chat_id') or '').strip()
    message = str(data.get('message') or '').strip()
    try:
        result = notification_service.send_test_message(
            chat_id=chat_id,
            text=message,
            sender_mode='user_sender',
            sender_account=payload if not sender_id else None,
            bot_token_override=bot_token if not sender_id else None,
            existing_sender_id=sender_id,
            user_id=user_id
        )
        return jsonify({
            'message': '测试消息发送成功',
            'result': result
        })
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except requests.RequestException as exc:
        return jsonify({'error': f'Telegram 请求失败: {exc}'}), 502
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/notification-endpoints', methods=['GET'])
@login_required
def get_notification_endpoints():
    user_id = get_current_user_id()
    items = db.list_notification_endpoints(user_id)
    return jsonify([_serialize_notification_endpoint(item) for item in items])


@app.route('/api/notification-endpoints', methods=['POST'])
@login_required
def create_notification_endpoint():
    user_id = get_current_user_id()
    data = request.get_json() or {}
    payload, errors = _validate_notification_endpoint_payload(data)
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400

    try:
        endpoint_id = db.create_notification_endpoint(user_id=user_id, **payload)
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400
    item = db.get_notification_endpoint(endpoint_id)
    return jsonify({
        'message': '通知接收端创建成功',
        'item': _serialize_notification_endpoint(item)
    })


@app.route('/api/notification-endpoints/<int:endpoint_id>', methods=['PUT'])
@login_required
def update_notification_endpoint(endpoint_id: int):
    user_id = get_current_user_id()
    existing = db.get_notification_endpoint(endpoint_id)
    if not existing or int(existing.get('user_id') or 0) != int(user_id):
        return jsonify({'error': '通知接收端不存在'}), 404

    data = request.get_json() or {}
    payload, errors = _validate_notification_endpoint_payload(data, existing_endpoint=existing)
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400

    try:
        db.update_notification_endpoint(endpoint_id, user_id, payload)
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400
    item = db.get_notification_endpoint(endpoint_id)
    return jsonify({
        'message': '通知接收端更新成功',
        'item': _serialize_notification_endpoint(item)
    })


@app.route('/api/notification-endpoints/<int:endpoint_id>', methods=['DELETE'])
@login_required
def delete_notification_endpoint(endpoint_id: int):
    user_id = get_current_user_id()
    existing = db.get_notification_endpoint(endpoint_id)
    if not existing or int(existing.get('user_id') or 0) != int(user_id):
        return jsonify({'error': '通知接收端不存在'}), 404
    db.delete_notification_endpoint(endpoint_id, user_id)
    return jsonify({'message': '通知接收端已删除'})


@app.route('/api/notification-endpoints/test', methods=['POST'])
@login_required
def test_notification_endpoint():
    data = request.get_json() or {}
    payload, errors = _validate_notification_endpoint_payload(data)
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400

    endpoint_key = str(payload.get('endpoint_key') or '').strip()
    message = str(data.get('message') or '').strip()
    try:
        result = notification_service.send_test_message(chat_id=endpoint_key, text=message)
        return jsonify({
            'message': '测试消息发送成功',
            'result': result
        })
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except requests.RequestException as exc:
        return jsonify({'error': f'Telegram 请求失败: {exc}'}), 502
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/notification-subscriptions', methods=['GET'])
@login_required
def get_notification_subscriptions():
    user_id = get_current_user_id()
    items = db.list_notification_subscriptions(user_id)
    return jsonify([_serialize_notification_subscription(item) for item in items])


@app.route('/api/public/predictors/<int:predictor_id>/subscription-context', methods=['GET'])
@login_required
def get_public_predictor_subscription_context(predictor_id: int):
    user_id = get_current_user_id()
    predictor = db.get_predictor(predictor_id, include_secret=False)
    if not predictor or not _can_user_subscribe_to_predictor(user_id, predictor):
        return jsonify({'error': '该方案暂不支持订阅'}), 403
    lottery_type = normalize_lottery_type(predictor.get('lottery_type'))
    subscriptions = [
        item for item in db.list_notification_subscriptions(user_id)
        if int(item.get('predictor_id') or 0) == int(predictor_id)
    ]
    return jsonify({
        'predictor': {
            'id': predictor['id'],
            'name': predictor.get('name') or '',
            'lottery_type': lottery_type,
            'lottery_label': get_lottery_definition(lottery_type).label
        },
        'endpoints': [_serialize_notification_endpoint(item) for item in db.list_notification_endpoints(user_id)],
        'senders': [_serialize_notification_sender_account(item) for item in db.list_notification_sender_accounts(user_id)],
        'bet_profiles': [_serialize_bet_profile(item) for item in db.list_bet_profiles(user_id, lottery_type=lottery_type)],
        'subscriptions': [_serialize_notification_subscription(item) for item in subscriptions]
    })


@app.route('/api/notification-subscriptions', methods=['POST'])
@login_required
def create_notification_subscription():
    user_id = get_current_user_id()
    data = request.get_json() or {}
    payload, errors = _validate_notification_subscription_payload(user_id, data)
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400

    try:
        subscription_id = db.create_notification_subscription(user_id=user_id, **payload)
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400
    item = db.get_notification_subscription(subscription_id)
    return jsonify({
        'message': '通知订阅创建成功',
        'item': _serialize_notification_subscription(item)
    })


@app.route('/api/notification-subscriptions/<int:subscription_id>', methods=['PUT'])
@login_required
def update_notification_subscription(subscription_id: int):
    user_id = get_current_user_id()
    existing = db.get_notification_subscription(subscription_id)
    if not existing or int(existing.get('user_id') or 0) != int(user_id):
        return jsonify({'error': '通知订阅不存在'}), 404

    data = request.get_json() or {}
    payload, errors = _validate_notification_subscription_payload(user_id, data, existing_subscription=existing)
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400

    try:
        db.update_notification_subscription(subscription_id, user_id, payload)
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400
    item = db.get_notification_subscription(subscription_id)
    return jsonify({
        'message': '通知订阅更新成功',
        'item': _serialize_notification_subscription(item)
    })


@app.route('/api/notification-subscriptions/<int:subscription_id>', methods=['DELETE'])
@login_required
def delete_notification_subscription(subscription_id: int):
    user_id = get_current_user_id()
    existing = db.get_notification_subscription(subscription_id)
    if not existing or int(existing.get('user_id') or 0) != int(user_id):
        return jsonify({'error': '通知订阅不存在'}), 404
    db.delete_notification_subscription(subscription_id, user_id)
    return jsonify({'message': '通知订阅已删除'})


@app.route('/api/notification-deliveries', methods=['GET'])
@login_required
def get_notification_deliveries():
    user_id = get_current_user_id()
    limit = max(1, min(int(request.args.get('limit', 50) or 50), 200))
    items = db.list_notification_deliveries(user_id, limit=limit)
    return jsonify([_serialize_notification_delivery(item) for item in items])


@app.route('/api/notification-deliveries/<int:delivery_id>/retry', methods=['POST'])
@login_required
def retry_notification_delivery(delivery_id: int):
    user_id = get_current_user_id()
    try:
        item = notification_service.retry_delivery(delivery_id, user_id)
        return jsonify({
            'message': '通知已重新发送',
            'item': _serialize_notification_delivery(item)
        })
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except requests.RequestException as exc:
        return jsonify({'error': f'Telegram 请求失败: {exc}'}), 502
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


# ============ PC28 Public APIs ============


@app.route('/api/pc28/overview', methods=['GET'])
def get_pc28_overview():
    limit = request.args.get('limit', 20, type=int)
    try:
        overview = pc28_service.build_overview(history_limit=max(5, min(limit, 50)))
        if overview.get('recent_draws'):
            db.upsert_draws('pc28', overview['recent_draws'])
        return jsonify(_serialize_overview(overview))
    except Exception as exc:
        cached_draws = db.get_recent_draws('pc28', limit=max(5, min(limit, 50)))
        if not cached_draws:
            return jsonify({'error': str(exc)}), 500

        latest_draw = cached_draws[0]
        return jsonify({
            'lottery_type': 'pc28',
            'latest_draw': _serialize_draw(latest_draw),
            'next_issue_no': None,
            'countdown': '--:--:--',
            'recent_draws': [_serialize_draw(draw) for draw in cached_draws],
            'omission_preview': {'top_numbers': [], 'groups': {}},
            'today_preview': {'summary': {}, 'hot_numbers': []},
            'preview': {},
            'generated_at': get_current_beijing_time_str(),
            'warning': f'官方接口不可用，已回退本地缓存：{exc}'
        })


@app.route('/api/lotteries/<lottery_type>/overview', methods=['GET'])
def get_lottery_overview(lottery_type: str):
    normalized_lottery_type = normalize_lottery_type(lottery_type)
    if normalized_lottery_type == 'pc28':
        return get_pc28_overview()

    limit = request.args.get('limit', 20, type=int)
    if normalized_lottery_type == 'jingcai_football':
        overview = jingcai_football_service.build_overview_with_fallback(db, limit=max(5, min(limit, 50)))
        return jsonify({
            **overview,
            'recent_events': [_serialize_lottery_event(item) for item in overview.get('recent_events', [])]
        })

    return jsonify({'error': '暂不支持的彩种'}), 404


@app.route('/api/lotteries/jingcai_football/sync-matches', methods=['POST'])
@login_required
def sync_jingcai_matches():
    data = request.get_json() or {}
    date = str(data.get('date') or '').strip()
    run_key = str(data.get('run_key') or '').strip()
    if not date and not run_key:
        return jsonify({'error': '请提供 date 或 run_key'}), 400

    raw_limit = data.get('limit', 20)
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        limit = 20
    limit = max(5, min(limit, 50))

    raw_is_prized = data.get('is_prized')
    prefer_is_prized = None if raw_is_prized is None else str(raw_is_prized).strip()

    try:
        result = jingcai_football_service.sync_batch_overview(
            db,
            date=date,
            run_key=run_key or None,
            limit=limit,
            prefer_is_prized=prefer_is_prized
        )
        overview = result.get('overview') or {}
        return jsonify({
            'message': 'sync_matches 执行完成',
            'requested_date': date or None,
            'requested_run_key': run_key or None,
            'run_key': result.get('run_key'),
            'used_is_prized': result.get('used_is_prized'),
            'batch_overview': {
                **overview,
                'recent_events': [_serialize_lottery_event(item) for item in overview.get('recent_events', [])]
            }
        })
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/public/predictors', methods=['GET'])
def get_public_predictors():
    lottery_type = request.args.get('lottery_type', 'pc28')
    sort_by = request.args.get('sort_by', 'recent100')
    metric = request.args.get('metric', 'combo')
    limit = request.args.get('limit', 10, type=int)
    limit = max(1, min(limit, 50))

    return jsonify({
        'lottery_type': normalize_lottery_type(lottery_type),
        'sort_by': sort_by,
        'metric': metric,
        'items': _build_public_predictor_rankings(
            sort_by=sort_by,
            metric=metric,
            limit=limit,
            lottery_type=lottery_type
        )
    })


@app.route('/api/public/predictors/<int:predictor_id>', methods=['GET'])
def get_public_predictor_detail(predictor_id: int):
    detail = _get_public_predictor_detail(predictor_id)
    if not detail:
        return jsonify({'error': '该方案未开放预测内容'}), 404
    return jsonify(detail)


@app.route('/api/public/predictors/<int:predictor_id>/simulation', methods=['GET'])
def get_public_predictor_simulation(predictor_id: int):
    detail = _get_public_predictor_detail(predictor_id)
    if not detail:
        return jsonify({'error': '该方案未开放预测内容'}), 404

    predictor = detail['predictor']
    if not supports_profit_simulation(predictor.get('lottery_type')):
        return jsonify({'error': '当前彩种暂不支持收益模拟'}), 400
    requested_metric = request.args.get('metric') or predictor.get('default_simulation_metric')
    profit_rule_id = request.args.get('profit_rule_id') or predictor.get('profit_rule_id')
    odds_profile = request.args.get('odds_profile', DEFAULT_ODDS_PROFILE)
    period_key = request.args.get('period_key') or profit_simulator.get_default_period_key(predictor)
    bet_mode = request.args.get('bet_mode')
    base_stake = request.args.get('base_stake', type=float)
    multiplier = request.args.get('multiplier', type=float)
    max_steps = request.args.get('max_steps', type=int)
    include_records = predictor.get('can_view_records', False)
    lottery_type = normalize_lottery_type(predictor.get('lottery_type'))
    force_include_records = include_records or lottery_type == 'jingcai_football'

    try:
        simulation = profit_simulator.build_profit_simulation(
            predictor_id,
            requested_metric=requested_metric,
            profit_rule_id=profit_rule_id,
            odds_profile=odds_profile,
            period_key=period_key,
            bet_mode=bet_mode,
            base_stake=base_stake,
            multiplier=multiplier,
            max_steps=max_steps,
            include_records=force_include_records
        )
        if lottery_type == 'jingcai_football':
            simulation = _apply_jingcai_simulation_sale_filter(predictor_id, simulation)
        return jsonify({
            'predictor_id': predictor_id,
            'share_level': predictor.get('share_level'),
            'can_view_records': include_records,
            'simulation': _serialize_profit_simulation(simulation, include_records=include_records)
        })
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400


# ============ Predictor APIs ============


@app.route('/api/admin/dashboard', methods=['GET'])
@admin_required
def get_admin_dashboard():
    return jsonify(_build_admin_dashboard_data())


@app.route('/api/admin/settings/prediction-guard', methods=['POST'])
@admin_required
def update_admin_prediction_guard_settings():
    data = request.get_json() or {}
    enabled = _parse_bool(data.get('enabled'), True)
    threshold = data.get('threshold', 3)
    try:
        threshold = int(threshold)
    except (TypeError, ValueError):
        threshold = 3
    threshold = max(1, min(threshold, 20))
    settings = prediction_guard.update_settings(enabled=enabled, threshold=threshold)
    return jsonify({
        'message': 'AI 故障保护设置已更新',
        'settings': settings
    })


@app.route('/api/admin/settings/notifications', methods=['GET'])
@admin_required
def get_admin_notification_settings():
    settings = notification_service.get_settings()
    return jsonify({
        'settings': {
            **settings,
            'telegram_bot_token_masked': mask_api_key(settings.get('telegram_bot_token'))
        }
    })


@app.route('/api/admin/settings/notifications', methods=['POST'])
@admin_required
def update_admin_notification_settings():
    data = request.get_json() or {}
    enabled = _parse_bool(data.get('enabled'), False)
    existing_settings = notification_service.get_settings()
    telegram_bot_token = str(data.get('telegram_bot_token') or '').strip() or str(existing_settings.get('telegram_bot_token') or '')
    telegram_bot_name = str(data.get('telegram_bot_name') or '').strip()
    settings = notification_service.update_settings(
        enabled=enabled,
        telegram_bot_token=telegram_bot_token,
        telegram_bot_name=telegram_bot_name
    )
    return jsonify({
        'message': '通知设置已更新',
        'settings': {
            **settings,
            'telegram_bot_token_masked': mask_api_key(settings.get('telegram_bot_token'))
        }
    })


@app.route('/api/admin/settings/notifications/test', methods=['POST'])
@admin_required
def send_admin_notification_test():
    data = request.get_json() or {}
    chat_id = str(data.get('chat_id') or '').strip()
    message = str(data.get('message') or '').strip()
    try:
        payload = notification_service.send_test_message(chat_id=chat_id, text=message)
        return jsonify({
            'message': '测试消息发送成功',
            'result': payload
        })
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except requests.RequestException as exc:
        return jsonify({'error': f'Telegram 请求失败: {exc}'}), 502
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/admin/users/<int:user_id>/toggle-admin', methods=['POST'])
@admin_required
def toggle_admin_user(user_id: int):
    current_user_id = get_current_user_id()
    user = db.get_user_by_id(user_id)
    if not user:
        return jsonify({'error': '用户不存在'}), 404

    target_is_admin = not bool(user.get('is_admin'))
    if not target_is_admin and current_user_id == user_id and db.count_admin_users() <= 1:
        return jsonify({'error': '至少需要保留一个管理员，不能取消自己最后一个管理员身份'}), 400

    db.update_user_admin(user_id, target_is_admin)
    updated = db.get_user_by_id(user_id)
    if current_user_id == user_id:
        set_current_user_with_role(updated['id'], updated['username'], bool(updated.get('is_admin')))

    return jsonify({
        'message': '管理员身份已更新',
        'user': {
            'id': updated['id'],
            'username': updated['username'],
            'is_admin': bool(updated.get('is_admin'))
        }
    })


@app.route('/api/admin/predictors/<int:predictor_id>/toggle-enabled', methods=['POST'])
@admin_required
def toggle_admin_predictor_enabled(predictor_id: int):
    predictor = db.get_predictor(predictor_id, include_secret=False)
    if not predictor:
        return jsonify({'error': '预测方案不存在'}), 404

    next_enabled = not bool(predictor.get('enabled'))
    db.update_predictor(predictor_id, {'enabled': next_enabled})
    updated = db.get_predictor(predictor_id, include_secret=False)
    return jsonify({
        'message': '方案状态已更新',
        'predictor': {
            'id': updated['id'],
            'enabled': bool(updated.get('enabled'))
        }
    })


@app.route('/api/admin/predictors/<int:predictor_id>/resume-auto-pause', methods=['POST'])
@admin_required
def resume_admin_predictor_auto_pause(predictor_id: int):
    predictor = db.get_predictor(predictor_id, include_secret=False)
    if not predictor:
        return jsonify({'error': '预测方案不存在'}), 404

    prediction_guard.resume_predictor(predictor_id)
    updated = db.get_predictor(predictor_id, include_secret=False)
    return jsonify({
        'message': '方案已解除自动暂停',
        'predictor': _serialize_admin_predictor(updated)
    })


@app.route('/api/user-algorithms', methods=['GET'])
@login_required
def get_user_algorithms():
    user_id = get_current_user_id()
    lottery_type = request.args.get('lottery_type')
    include_disabled = _parse_bool(request.args.get('include_disabled'), default=False)
    algorithms = db.get_user_algorithms_by_user(
        user_id,
        lottery_type=lottery_type,
        include_disabled=include_disabled
    )
    return jsonify([_serialize_user_algorithm(item) for item in algorithms])


@app.route('/api/user-algorithms', methods=['POST'])
@login_required
def create_user_algorithm():
    user_id = get_current_user_id()
    data = request.get_json() or {}
    if db.count_user_algorithms_by_user(user_id) >= MAX_USER_ALGORITHMS_PER_USER:
        return jsonify({'error': f'单个用户最多保留 {MAX_USER_ALGORITHMS_PER_USER} 个启用/草稿算法'}), 429
    payload, errors = _validate_user_algorithm_payload(data)
    if errors:
        return jsonify({'error': '；'.join(errors), 'validation': payload.get('validation')}), 400

    algorithm_id = db.create_user_algorithm(
        user_id=user_id,
        lottery_type=payload['lottery_type'],
        name=payload['name'],
        description=payload['description'],
        definition=payload['definition'],
        validation=payload['validation'],
        status=payload['status'],
        change_summary=payload['change_summary'] or '初始版本'
    )
    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    status_message = '用户算法创建成功' if payload['status'] == 'validated' else '用户算法已保存为草稿'
    return jsonify({
        'id': algorithm_id,
        'message': status_message,
        'algorithm': _serialize_user_algorithm(algorithm),
        'validation': payload['validation']
    })


@app.route('/api/user-algorithms/validate', methods=['POST'])
@login_required
def validate_user_algorithm_draft():
    data = request.get_json() or {}
    definition = data.get('definition') or {}
    lottery_type = normalize_lottery_type(data.get('lottery_type') or (definition.get('lottery_type') if isinstance(definition, dict) else 'pc28'))
    if isinstance(definition, str):
        try:
            definition = json.loads(definition)
        except json.JSONDecodeError:
            return jsonify({'valid': False, 'errors': ['算法定义必须是有效 JSON'], 'warnings': []}), 400
    validation = validate_algorithm_definition(definition, lottery_type=lottery_type)
    return jsonify(validation)


@app.route('/api/user-algorithms/dry-run', methods=['POST'])
@login_required
def dry_run_user_algorithm():
    data = request.get_json() or {}
    definition = data.get('definition') or {}
    lottery_type = normalize_lottery_type(data.get('lottery_type') or (definition.get('lottery_type') if isinstance(definition, dict) else 'pc28'))
    if isinstance(definition, str):
        try:
            definition = json.loads(definition)
        except json.JSONDecodeError:
            return jsonify({'error': '算法定义必须是有效 JSON'}), 400
    validation = validate_algorithm_definition(definition, lottery_type=lottery_type)
    if not validation['valid']:
        return jsonify({'error': '算法校验未通过', 'validation': validation}), 400
    if lottery_type != 'jingcai_football':
        return jsonify({'error': '当前仅支持竞彩足球用户算法试跑'}), 400

    normalized_definition = validation['normalized_definition']
    user_algorithm = {
        'id': 0,
        'name': normalized_definition.get('method_name') or '用户算法试跑',
        'definition': normalized_definition
    }
    items_payload, debug_payload = predict_jingcai_with_user_algorithm(
        'sample-run',
        _build_user_algorithm_sample_matches(),
        {
            'id': 0,
            'prediction_targets': normalized_definition.get('targets') or [],
            'user_algorithm': user_algorithm
        }
    )
    return jsonify({
        'message': '试跑完成',
        'validation': validation,
        'items': items_payload,
        'debug': debug_payload
    })


@app.route('/api/user-algorithms/backtest', methods=['POST'])
@login_required
def backtest_user_algorithm():
    user_id = get_current_user_id()
    allowed, wait_seconds = _check_user_algorithm_backtest_rate_limit(user_id)
    if not allowed:
        return jsonify({'error': f'回测请求过于频繁，请 {wait_seconds} 秒后再试'}), 429

    data = request.get_json() or {}
    definition = data.get('definition') or {}
    lottery_type = normalize_lottery_type(data.get('lottery_type') or (definition.get('lottery_type') if isinstance(definition, dict) else 'pc28'))
    if isinstance(definition, str):
        try:
            definition = json.loads(definition)
        except json.JSONDecodeError:
            return jsonify({'error': '算法定义必须是有效 JSON'}), 400
    validation = validate_algorithm_definition(definition, lottery_type=lottery_type)
    if not validation['valid']:
        return jsonify({'error': '算法校验未通过', 'validation': validation}), 400
    if lottery_type != 'jingcai_football':
        return jsonify({'error': '当前仅支持竞彩足球用户算法回测'}), 400

    result = backtest_jingcai_user_algorithm(
        db,
        validation['normalized_definition'],
        limit=_parse_positive_int(data.get('limit'), default=50, minimum=1, maximum=200),
        filters=_build_user_algorithm_backtest_filters(data, validation['normalized_definition'])
    )
    return jsonify({
        'message': '回测完成',
        'validation': validation,
        'backtest': result,
        'adjustment_suggestions': build_backtest_adjustment_suggestions(result)
    })


@app.route('/api/user-algorithms/templates', methods=['GET'])
@login_required
def get_user_algorithm_templates():
    lottery_type = normalize_lottery_type(request.args.get('lottery_type') or 'jingcai_football')
    return jsonify(list_algorithm_templates(lottery_type))


@app.route('/api/user-algorithms/ai-draft', methods=['POST'])
@login_required
def generate_user_algorithm_ai_draft():
    data = request.get_json() or {}
    api_key = str(data.get('api_key') or '').strip()
    api_url = str(data.get('api_url') or '').strip()
    model_name = str(data.get('model_name') or '').strip()
    api_mode = normalize_api_mode(data.get('api_mode') or 'auto')
    lottery_type = normalize_lottery_type(data.get('lottery_type') or 'jingcai_football')
    user_message = str(data.get('message') or '').strip()
    current_definition = data.get('current_definition') if isinstance(data.get('current_definition'), dict) else None
    chat_history = data.get('chat_history') if isinstance(data.get('chat_history'), list) else []
    backtest_summary = data.get('backtest_summary') if isinstance(data.get('backtest_summary'), dict) else None

    if not api_key:
        return jsonify({'error': 'AI 算法助手需要 API Key'}), 400
    if not api_url:
        return jsonify({'error': 'AI 算法助手需要 API 地址'}), 400
    if not model_name:
        return jsonify({'error': 'AI 算法助手需要模型名称'}), 400
    if not user_message:
        return jsonify({'error': '请先描述你的算法思路'}), 400

    try:
        result = generate_algorithm_draft(
            api_key=api_key,
            api_url=api_url,
            model_name=model_name,
            api_mode=api_mode,
            lottery_type=lottery_type,
            user_message=user_message,
            current_definition=current_definition,
            chat_history=chat_history,
            backtest_summary=backtest_summary,
            temperature=0.2
        )
        payload = result['payload']
        return jsonify({
            'message': payload.get('message') or 'AI 已生成算法草稿',
            'reply_type': payload.get('reply_type') or 'draft_algorithm',
            'questions': payload.get('questions') or [],
            'algorithm': result['algorithm'],
            'change_summary': payload.get('change_summary') or '',
            'risk_notes': payload.get('risk_notes') or [],
            'validation': result['validation'],
            'api_mode': result['api_mode'],
            'response_model': result['response_model'],
            'finish_reason': result['finish_reason'],
            'latency_ms': result['latency_ms'],
            'raw_response': result['raw_response'][:1200]
        })
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/user-algorithms/<int:algorithm_id>/ai-adjust', methods=['POST'])
@login_required
def ai_adjust_user_algorithm(algorithm_id: int):
    user_id = get_current_user_id()
    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    if not algorithm:
        return jsonify({'error': '用户算法不存在或无权操作'}), 404

    data = request.get_json() or {}
    api_key = str(data.get('api_key') or '').strip()
    api_url = str(data.get('api_url') or '').strip()
    model_name = str(data.get('model_name') or '').strip()
    api_mode = normalize_api_mode(data.get('api_mode') or 'auto')
    user_message = str(data.get('message') or '').strip() or '请根据最近回测结果调整当前算法，生成一个新版本。'
    chat_history = data.get('chat_history') if isinstance(data.get('chat_history'), list) else []
    backtest_summary = data.get('backtest_summary') if isinstance(data.get('backtest_summary'), dict) else None
    if backtest_summary is None:
        active_version = int(algorithm.get('active_version') or 1)
        active_version_row = db.get_user_algorithm_version_for_user(algorithm_id, user_id, active_version)
        backtest_summary = (active_version_row or {}).get('backtest') or {}

    if not api_key:
        return jsonify({'error': 'AI 调参需要 API Key'}), 400
    if not api_url:
        return jsonify({'error': 'AI 调参需要 API 地址'}), 400
    if not model_name:
        return jsonify({'error': 'AI 调参需要模型名称'}), 400

    try:
        result = generate_algorithm_draft(
            api_key=api_key,
            api_url=api_url,
            model_name=model_name,
            api_mode=api_mode,
            lottery_type=algorithm.get('lottery_type') or 'jingcai_football',
            user_message=user_message,
            current_definition=algorithm.get('definition') or {},
            chat_history=chat_history,
            backtest_summary=backtest_summary,
            temperature=0.2
        )
        payload = result['payload']
        if payload.get('reply_type') == 'need_clarification':
            return jsonify({
                'message': payload.get('message') or 'AI 需要补充信息',
                'reply_type': 'need_clarification',
                'questions': payload.get('questions') or [],
                'algorithm': _serialize_user_algorithm(algorithm),
                'validation': result['validation'],
                'raw_response': result['raw_response'][:1200]
            })

        validation = result['validation']
        adjusted_definition = validation['normalized_definition'] if validation.get('normalized_definition') else result['algorithm']
        status = 'validated' if validation.get('valid') else 'draft'
        change_summary = payload.get('change_summary') or 'AI 根据回测结果调参'
        db.update_user_algorithm(
            algorithm_id=algorithm_id,
            user_id=user_id,
            fields={
                'name': algorithm.get('name') or adjusted_definition.get('method_name') or '用户算法',
                'description': algorithm.get('description') or '',
                'definition_json': json.dumps(adjusted_definition or {}, ensure_ascii=False),
                'validation_json': json.dumps(validation, ensure_ascii=False),
                'status': status
            },
            create_version=True,
            change_summary=change_summary
        )
        updated = db.get_user_algorithm_for_user(algorithm_id, user_id)
        return jsonify({
            'message': 'AI 已根据回测生成新版本',
            'reply_type': payload.get('reply_type') or 'draft_algorithm',
            'change_summary': change_summary,
            'risk_notes': payload.get('risk_notes') or [],
            'algorithm': _serialize_user_algorithm(updated),
            'validation': validation,
            'versions': [
                _serialize_user_algorithm_version(item, active_version=int(updated.get('active_version') or 1))
                for item in db.get_user_algorithm_versions_for_user(algorithm_id, user_id)
            ],
            'api_mode': result['api_mode'],
            'response_model': result['response_model'],
            'finish_reason': result['finish_reason'],
            'latency_ms': result['latency_ms'],
            'raw_response': result['raw_response'][:1200]
        })
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/user-algorithms/<int:algorithm_id>', methods=['GET'])
@login_required
def get_user_algorithm(algorithm_id: int):
    user_id = get_current_user_id()
    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    if not algorithm:
        return jsonify({'error': '用户算法不存在或无权访问'}), 404
    return jsonify(_serialize_user_algorithm(algorithm))


@app.route('/api/user-algorithms/<int:algorithm_id>', methods=['PUT'])
@login_required
def update_user_algorithm(algorithm_id: int):
    user_id = get_current_user_id()
    existing = db.get_user_algorithm_for_user(algorithm_id, user_id)
    if not existing:
        return jsonify({'error': '用户算法不存在或无权操作'}), 404

    data = request.get_json() or {}
    payload, errors = _validate_user_algorithm_payload(data, existing_algorithm=existing)
    if errors:
        return jsonify({'error': '；'.join(errors), 'validation': payload.get('validation')}), 400

    db.update_user_algorithm(
        algorithm_id=algorithm_id,
        user_id=user_id,
        fields={
            'name': payload['name'],
            'description': payload['description'],
            'definition_json': json.dumps(payload['definition'], ensure_ascii=False),
            'validation_json': json.dumps(payload['validation'], ensure_ascii=False),
            'status': payload['status']
        },
        create_version=True,
        change_summary=payload['change_summary'] or '更新算法定义'
    )
    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    return jsonify({
        'message': '用户算法更新成功' if payload['status'] == 'validated' else '用户算法已保存为草稿',
        'algorithm': _serialize_user_algorithm(algorithm),
        'validation': payload['validation']
    })


@app.route('/api/user-algorithms/<int:algorithm_id>/validate', methods=['POST'])
@login_required
def validate_user_algorithm(algorithm_id: int):
    user_id = get_current_user_id()
    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    if not algorithm:
        return jsonify({'error': '用户算法不存在或无权操作'}), 404
    data = request.get_json() or {}
    definition = data.get('definition', algorithm.get('definition') or {})
    lottery_type = normalize_lottery_type(data.get('lottery_type') or algorithm.get('lottery_type'))
    if isinstance(definition, str):
        try:
            definition = json.loads(definition)
        except json.JSONDecodeError:
            return jsonify({'valid': False, 'errors': ['算法定义必须是有效 JSON'], 'warnings': []}), 400
    validation = validate_algorithm_definition(definition, lottery_type=lottery_type)
    return jsonify(validation)


@app.route('/api/user-algorithms/<int:algorithm_id>/backtest', methods=['POST'])
@login_required
def backtest_saved_user_algorithm(algorithm_id: int):
    user_id = get_current_user_id()
    allowed, wait_seconds = _check_user_algorithm_backtest_rate_limit(user_id)
    if not allowed:
        return jsonify({'error': f'回测请求过于频繁，请 {wait_seconds} 秒后再试'}), 429

    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    if not algorithm:
        return jsonify({'error': '用户算法不存在或无权操作'}), 404
    lottery_type = normalize_lottery_type(algorithm.get('lottery_type'))
    if lottery_type != 'jingcai_football':
        return jsonify({'error': '当前仅支持竞彩足球用户算法回测'}), 400

    definition = algorithm.get('definition') or {}
    validation = validate_algorithm_definition(definition, lottery_type=lottery_type)
    if not validation['valid']:
        return jsonify({'error': '算法校验未通过', 'validation': validation}), 400

    data = request.get_json() or {}
    result = backtest_jingcai_user_algorithm(
        db,
        validation['normalized_definition'],
        limit=_parse_positive_int(data.get('limit'), default=50, minimum=1, maximum=200),
        filters=_build_user_algorithm_backtest_filters(data, validation['normalized_definition'])
    )
    active_version = int(algorithm.get('active_version') or 1)
    db.update_user_algorithm_version_backtest(algorithm_id, user_id, active_version, result)
    return jsonify({
        'message': '回测完成',
        'validation': validation,
        'backtest': result,
        'adjustment_suggestions': build_backtest_adjustment_suggestions(result),
        'algorithm': _serialize_user_algorithm(db.get_user_algorithm_for_user(algorithm_id, user_id)),
        'version': active_version
    })


@app.route('/api/user-algorithms/<int:algorithm_id>/adjust', methods=['POST'])
@login_required
def adjust_user_algorithm(algorithm_id: int):
    user_id = get_current_user_id()
    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    if not algorithm:
        return jsonify({'error': '用户算法不存在或无权操作'}), 404

    data = request.get_json() or {}
    mode = str(data.get('mode') or '').strip()
    adjusted_definition, change_summary = apply_algorithm_adjustment(algorithm.get('definition') or {}, mode)
    validation = validate_algorithm_definition(adjusted_definition, lottery_type=algorithm.get('lottery_type'))
    status = 'validated' if validation['valid'] else 'draft'
    db.update_user_algorithm(
        algorithm_id=algorithm_id,
        user_id=user_id,
        fields={
            'name': algorithm.get('name') or adjusted_definition.get('method_name') or '用户算法',
            'description': algorithm.get('description') or '',
            'definition_json': json.dumps(validation['normalized_definition'] or adjusted_definition, ensure_ascii=False),
            'validation_json': json.dumps(validation, ensure_ascii=False),
            'status': status
        },
        create_version=True,
        change_summary=change_summary
    )
    updated = db.get_user_algorithm_for_user(algorithm_id, user_id)
    return jsonify({
        'message': '算法已生成新版本',
        'change_summary': change_summary,
        'algorithm': _serialize_user_algorithm(updated),
        'validation': validation,
        'versions': [
            _serialize_user_algorithm_version(item, active_version=int(updated.get('active_version') or 1))
            for item in db.get_user_algorithm_versions_for_user(algorithm_id, user_id)
        ]
    })


@app.route('/api/user-algorithms/<int:algorithm_id>/versions', methods=['GET'])
@login_required
def get_user_algorithm_versions(algorithm_id: int):
    user_id = get_current_user_id()
    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    if not algorithm:
        return jsonify({'error': '用户算法不存在或无权访问'}), 404
    active_version = int(algorithm.get('active_version') or 1)
    versions = db.get_user_algorithm_versions_for_user(algorithm_id, user_id)
    return jsonify([
        _serialize_user_algorithm_version(item, active_version=active_version)
        for item in versions
    ])


@app.route('/api/user-algorithms/<int:algorithm_id>/execution-logs', methods=['GET'])
@login_required
def get_user_algorithm_execution_logs(algorithm_id: int):
    user_id = get_current_user_id()
    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    if not algorithm:
        return jsonify({'error': '用户算法不存在或无权访问'}), 404
    limit = _parse_positive_int(request.args.get('limit'), default=30, minimum=1, maximum=100)
    logs = db.get_user_algorithm_execution_logs_for_user(algorithm_id, user_id, limit=limit)
    return jsonify([
        _serialize_user_algorithm_execution_log(item)
        for item in logs
    ])


@app.route('/api/user-algorithms/<int:algorithm_id>/ops-summary', methods=['GET'])
@login_required
def get_user_algorithm_ops_summary(algorithm_id: int):
    user_id = get_current_user_id()
    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    if not algorithm:
        return jsonify({'error': '用户算法不存在或无权访问'}), 404
    return jsonify(_build_user_algorithm_ops_summary(algorithm, user_id))


@app.route('/api/user-algorithms/<int:algorithm_id>/compare-versions', methods=['POST'])
@login_required
def compare_user_algorithm_versions(algorithm_id: int):
    user_id = get_current_user_id()
    allowed, wait_seconds = _check_user_algorithm_backtest_rate_limit(user_id)
    if not allowed:
        return jsonify({'error': f'回测请求过于频繁，请 {wait_seconds} 秒后再试'}), 429

    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    if not algorithm:
        return jsonify({'error': '用户算法不存在或无权操作'}), 404
    lottery_type = normalize_lottery_type(algorithm.get('lottery_type'))
    if lottery_type != 'jingcai_football':
        return jsonify({'error': '当前仅支持竞彩足球用户算法版本对比'}), 400

    data = request.get_json() or {}
    base_version = _parse_positive_int(data.get('base_version'), default=0, minimum=0, maximum=100000)
    candidate_version = _parse_positive_int(data.get('candidate_version'), default=0, minimum=0, maximum=100000)
    if not base_version or not candidate_version:
        return jsonify({'error': 'base_version 和 candidate_version 不能为空'}), 400
    if base_version == candidate_version:
        return jsonify({'error': 'base_version 和 candidate_version 不能相同'}), 400

    base_row = db.get_user_algorithm_version_for_user(algorithm_id, user_id, base_version)
    candidate_row = db.get_user_algorithm_version_for_user(algorithm_id, user_id, candidate_version)
    if not base_row or not candidate_row:
        return jsonify({'error': '用户算法版本不存在或无权操作'}), 404

    base_validation = validate_algorithm_definition(base_row.get('definition') or {}, lottery_type=lottery_type)
    candidate_validation = validate_algorithm_definition(candidate_row.get('definition') or {}, lottery_type=lottery_type)
    if not base_validation['valid']:
        return jsonify({'error': f'基准版本 V{base_version} 校验未通过', 'validation': base_validation}), 400
    if not candidate_validation['valid']:
        return jsonify({'error': f'候选版本 V{candidate_version} 校验未通过', 'validation': candidate_validation}), 400

    filters = _build_user_algorithm_backtest_filters(data, candidate_validation['normalized_definition'])
    limit = _parse_positive_int(data.get('limit'), default=50, minimum=1, maximum=200)
    base_backtest = backtest_jingcai_user_algorithm(
        db,
        base_validation['normalized_definition'],
        limit=limit,
        filters=filters
    )
    candidate_backtest = backtest_jingcai_user_algorithm(
        db,
        candidate_validation['normalized_definition'],
        limit=limit,
        filters=filters
    )
    return jsonify({
        'message': '版本对比回测完成',
        **_build_user_algorithm_compare_payload(
            base_row,
            candidate_row,
            base_backtest,
            candidate_backtest,
            base_backtest.get('filters') or filters
        )
    })


@app.route('/api/user-algorithms/<int:algorithm_id>/diagnose', methods=['POST'])
@login_required
def diagnose_user_algorithm(algorithm_id: int):
    user_id = get_current_user_id()
    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    if not algorithm:
        return jsonify({'error': '用户算法不存在或无权操作'}), 404

    data = request.get_json() or {}
    version = _parse_positive_int(
        data.get('version'),
        default=int(algorithm.get('active_version') or 1),
        minimum=1,
        maximum=100000
    )
    version_row = db.get_user_algorithm_version_for_user(algorithm_id, user_id, version)
    if not version_row:
        return jsonify({'error': '用户算法版本不存在或无权操作'}), 404
    backtest = version_row.get('backtest') or {}
    if not backtest:
        return jsonify({'error': '当前版本暂无回测结果，请先运行回测'}), 400

    return jsonify({
        'algorithm_id': algorithm_id,
        'version': version,
        'backtest_summary': _summarize_user_algorithm_backtest(backtest),
        'diagnosis': _build_user_algorithm_diagnosis(backtest)
    })


@app.route('/api/user-algorithms/<int:algorithm_id>/version-comparison', methods=['GET'])
@login_required
def get_user_algorithm_version_comparison(algorithm_id: int):
    user_id = get_current_user_id()
    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    if not algorithm:
        return jsonify({'error': '用户算法不存在或无权访问'}), 404
    active_version = int(algorithm.get('active_version') or 1)
    versions = [
        _serialize_user_algorithm_version(item, active_version=active_version)
        for item in db.get_user_algorithm_versions_for_user(algorithm_id, user_id)
    ]
    return jsonify({
        'algorithm_id': algorithm_id,
        'active_version': active_version,
        'comparison': _build_user_algorithm_version_comparison(versions)
    })


@app.route('/api/user-algorithms/<int:algorithm_id>/activate-version', methods=['POST'])
@login_required
def activate_user_algorithm_version(algorithm_id: int):
    user_id = get_current_user_id()
    data = request.get_json() or {}
    version = _parse_positive_int(data.get('version'), default=0, minimum=0, maximum=100000)
    if not version:
        return jsonify({'error': '版本号不能为空'}), 400
    if not db.activate_user_algorithm_version(algorithm_id, user_id, version):
        return jsonify({'error': '用户算法版本不存在或无权操作'}), 404
    algorithm = db.get_user_algorithm_for_user(algorithm_id, user_id)
    return jsonify({
        'message': '算法版本已启用',
        'algorithm': _serialize_user_algorithm(algorithm),
        'versions': [
            _serialize_user_algorithm_version(item, active_version=int(algorithm.get('active_version') or 1))
            for item in db.get_user_algorithm_versions_for_user(algorithm_id, user_id)
        ]
    })


@app.route('/api/user-algorithms/<int:algorithm_id>', methods=['DELETE'])
@login_required
def disable_user_algorithm(algorithm_id: int):
    user_id = get_current_user_id()
    if not db.user_algorithm_exists_for_user(algorithm_id, user_id):
        return jsonify({'error': '用户算法不存在或无权操作'}), 404
    affected_predictors = db.get_predictors_using_user_algorithm(user_id, algorithm_id)
    db.update_user_algorithm(
        algorithm_id=algorithm_id,
        user_id=user_id,
        fields={'status': 'disabled'},
        create_version=False
    )
    return jsonify({
        'message': '用户算法已停用',
        'affected_predictors': [_serialize_predictor(item) for item in affected_predictors],
        'warning': '以下预测方案仍引用该算法，请切换算法或停用方案。' if affected_predictors else ''
    })


@app.route('/api/predictors', methods=['GET'])
@login_required
def get_predictors():
    user_id = get_current_user_id()
    predictors = db.get_predictors_by_user(user_id, include_secret=True)
    return jsonify([_serialize_predictor(item) for item in predictors])


@app.route('/api/predictors/<int:predictor_id>', methods=['GET'])
@login_required
def get_predictor(predictor_id: int):
    user_id = get_current_user_id()
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return jsonify({'error': '无权访问此预测方案'}), 403

    predictor = db.get_predictor(predictor_id, include_secret=True)
    if not predictor:
        return jsonify({'error': '预测方案不存在'}), 404

    return jsonify(_serialize_predictor(predictor))


@app.route('/api/predictors', methods=['POST'])
@login_required
def create_predictor():
    user_id = get_current_user_id()
    data = request.get_json() or {}
    payload, errors = _validate_predictor_payload(data, user_id=user_id)
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400

    predictor_id = db.create_predictor(
        user_id=user_id,
        name=payload['name'],
        api_key=payload['api_key'],
        api_url=payload['api_url'],
        model_name=payload['model_name'],
        api_mode=payload['api_mode'],
        primary_metric=payload['primary_metric'],
        profit_default_metric=payload['profit_default_metric'],
        profit_rule_id=payload['profit_rule_id'],
        share_level=payload['share_level'],
        prediction_method=payload['prediction_method'],
        system_prompt=payload['system_prompt'],
        data_injection_mode=payload['data_injection_mode'],
        prediction_targets=payload['prediction_targets'],
        history_window=payload['history_window'],
        temperature=payload['temperature'],
        enabled=payload['enabled'],
        lottery_type=payload['lottery_type'],
        engine_type=payload['engine_type'],
        algorithm_key=payload['algorithm_key'],
        user_algorithm_fallback_strategy=payload['user_algorithm_fallback_strategy']
    )

    predictor = db.get_predictor(predictor_id, include_secret=True)
    return jsonify({
        'id': predictor_id,
        'message': '预测方案创建成功',
        'predictor': _serialize_predictor(predictor)
    })


@app.route('/api/predictors/test', methods=['POST'])
@login_required
def test_predictor():
    user_id = get_current_user_id()
    data = request.get_json() or {}
    try:
        _, resolved = _resolve_predictor_form_context(user_id, data)
    except PermissionError as exc:
        return jsonify({'error': str(exc)}), 403

    api_key = resolved['api_key']
    api_url = resolved['api_url']
    model_name = resolved['model_name']
    api_mode = resolved['api_mode']
    engine_type = normalize_engine_type(resolved.get('engine_type'))

    if engine_type == 'machine':
        user_algorithm = resolved.get('user_algorithm') if is_user_algorithm_key(resolved.get('algorithm_key')) else None
        algorithm_label = (
            user_algorithm.get('name') if user_algorithm else get_algorithm_label(
                resolved['lottery_type'],
                engine_type,
                resolved.get('algorithm_key')
            )
        )
        if user_algorithm:
            validation = validate_algorithm_definition(
                user_algorithm.get('definition') or {},
                lottery_type=resolved['lottery_type']
            )
            if not validation['valid']:
                return jsonify({'error': '用户算法校验未通过', 'validation': validation}), 400
            items_payload, debug_payload = predict_jingcai_with_user_algorithm(
                'predictor-test',
                _build_user_algorithm_sample_matches(),
                {
                    'id': resolved.get('predictor_id') or 0,
                    'prediction_targets': resolved.get('prediction_targets') or [],
                    'user_algorithm': {
                        **user_algorithm,
                        'definition': validation['normalized_definition']
                    }
                }
            )
            first_item = (items_payload or [{}])[0]
            return jsonify({
                'message': '用户算法检查通过',
                'api_mode': None,
                'response_model': algorithm_label,
                'finish_reason': 'user_algorithm',
                'latency_ms': 0,
                'response_preview': (
                    f"当前方案将使用 {algorithm_label}。"
                    f"样例试跑：SPF={first_item.get('predicted_spf') or '跳过'}，"
                    f"RQSPF={first_item.get('predicted_rqspf') or '跳过'}，"
                    f"置信度={first_item.get('confidence') if first_item.get('confidence') is not None else '--'}。"
                ),
                'raw_response': json.dumps({
                    'items': items_payload,
                    'debug': debug_payload
                }, ensure_ascii=False)
            })
        return jsonify({
            'message': '内置机器算法检查通过',
            'api_mode': None,
            'response_model': algorithm_label,
            'finish_reason': 'builtin_machine',
            'latency_ms': 0,
            'response_preview': f'当前方案将使用 {algorithm_label}，无需 API 连通性测试。',
            'raw_response': ''
        })

    if not api_key:
        return jsonify({'error': '请填写 API Key，或在编辑已有方案时使用已保存的 Key'}), 400
    if not api_url:
        return jsonify({'error': 'API 地址不能为空'}), 400
    if not model_name:
        return jsonify({'error': '模型名称不能为空'}), 400

    tester = AIPredictor(
        api_key=api_key,
        api_url=api_url,
        model_name=model_name,
        api_mode=api_mode,
        temperature=0
    )

    try:
        result = tester.run_connectivity_test()
        return jsonify({
            'message': '连接测试成功',
            'api_mode': result['api_mode'],
            'response_model': result['response_model'],
            'finish_reason': result['finish_reason'],
            'latency_ms': result['latency_ms'],
            'response_preview': result['response_preview'],
            'raw_response': result['raw_response'][:1000]
        })
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/predictors/prompt-check', methods=['POST'])
@login_required
def check_predictor_prompt():
    user_id = get_current_user_id()
    data = request.get_json() or {}

    try:
        _, resolved = _resolve_predictor_form_context(user_id, data)
    except PermissionError as exc:
        return jsonify({'error': str(exc)}), 403

    if normalize_engine_type(resolved.get('engine_type')) != 'ai':
        return jsonify({
            'risk_level': 'low',
            'summary': '当前方案使用内置机器算法，不依赖提示词与模型调用。',
            'issues': [],
            'detected_placeholders': [],
            'unknown_placeholders': [],
            'recommended_variables': [],
            'recommended_snippets': [],
            'prediction_targets': resolved['prediction_targets'],
            'data_injection_mode': resolved['data_injection_mode'],
            'primary_metric': resolved['primary_metric']
        })

    if not supports_prompt_assistant(resolved['lottery_type']):
        return jsonify({
            'risk_level': 'low',
            'summary': '当前彩种暂不支持内置提示词体检，可直接使用默认模板或自行编写。',
            'issues': [],
            'detected_placeholders': [],
            'unknown_placeholders': [],
            'recommended_variables': [],
            'recommended_snippets': [],
            'prediction_targets': resolved['prediction_targets'],
            'data_injection_mode': resolved['data_injection_mode'],
            'primary_metric': resolved['primary_metric']
        })

    analysis = analyze_prompt(
        prompt=resolved['system_prompt'],
        prediction_targets=resolved['prediction_targets'],
        data_injection_mode=resolved['data_injection_mode'],
        primary_metric=resolved['primary_metric'],
        lottery_type=resolved['lottery_type']
    )
    return jsonify(analysis)


@app.route('/api/predictors/prompt-optimize', methods=['POST'])
@login_required
def optimize_predictor_prompt():
    user_id = get_current_user_id()
    data = request.get_json() or {}

    try:
        _, resolved = _resolve_predictor_form_context(user_id, data)
    except PermissionError as exc:
        return jsonify({'error': str(exc)}), 403

    if normalize_engine_type(resolved.get('engine_type')) != 'ai':
        return jsonify({'error': '当前方案使用内置机器算法，不支持提示词优化'}), 400

    if not supports_prompt_assistant(resolved['lottery_type']):
        return jsonify({'error': '当前彩种暂不支持内置 AI 提示词优化'}), 400

    if not resolved['api_key']:
        return jsonify({'error': 'AI 优化需要可用的 API Key，请先填写或使用已有方案配置'}), 400
    if not resolved['api_url']:
        return jsonify({'error': 'AI 优化需要 API 地址'}), 400
    if not resolved['model_name']:
        return jsonify({'error': 'AI 优化需要模型名称'}), 400

    analysis = analyze_prompt(
        prompt=resolved['system_prompt'],
        prediction_targets=resolved['prediction_targets'],
        data_injection_mode=resolved['data_injection_mode'],
        primary_metric=resolved['primary_metric'],
        lottery_type=resolved['lottery_type']
    )

    optimizer_prompt = build_optimizer_prompt(
        current_prompt=resolved['system_prompt'],
        analysis=analysis,
        predictor_payload=resolved,
        lottery_type=resolved['lottery_type']
    )
    optimizer = AIPredictor(
        api_key=resolved['api_key'],
        api_url=resolved['api_url'],
        model_name=resolved['model_name'],
        api_mode=resolved['api_mode'],
        temperature=0.2
    )

    try:
        result = optimizer.run_prompt_optimization(optimizer_prompt)
        payload = result['payload']
        return jsonify({
            'message': 'AI 优化完成',
            'api_mode': result['api_mode'],
            'response_model': result['response_model'],
            'finish_reason': result['finish_reason'],
            'latency_ms': result['latency_ms'],
            'summary': payload.get('summary') or '',
            'issues': payload.get('issues') or [],
            'why': payload.get('why') or [],
            'optimized_prompt': payload.get('optimized_prompt') or '',
            'raw_response': result['raw_response'][:1200],
            'static_analysis': analysis
        })
    except Exception as exc:
        return jsonify({'error': str(exc), 'static_analysis': analysis}), 500


@app.route('/api/predictors/prompt-template', methods=['POST'])
@login_required
def build_predictor_prompt_template():
    user_id = get_current_user_id()
    data = request.get_json() or {}

    try:
        _, resolved = _resolve_predictor_form_context(user_id, data)
    except PermissionError as exc:
        return jsonify({'error': str(exc)}), 403

    if normalize_engine_type(resolved.get('engine_type')) != 'ai':
        return jsonify({'error': '当前方案使用内置机器算法，不需要生成网页 AI 提示词模板'}), 400

    return jsonify({
        'prompt_template': build_external_prompt_template(resolved, lottery_type=resolved['lottery_type'])
    })


@app.route('/api/predictors/<int:predictor_id>', methods=['PUT'])
@login_required
def update_predictor(predictor_id: int):
    user_id = get_current_user_id()
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return jsonify({'error': '无权操作此预测方案'}), 403

    existing = db.get_predictor(predictor_id, include_secret=True)
    if not existing:
        return jsonify({'error': '预测方案不存在'}), 404

    data = request.get_json() or {}
    payload, errors = _validate_predictor_payload(data, existing_predictor=existing, user_id=user_id)
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400

    updates = {
        'name': payload['name'],
        'engine_type': payload['engine_type'],
        'algorithm_key': payload['algorithm_key'],
        'api_key': payload['api_key'],
        'api_url': payload['api_url'],
        'model_name': payload['model_name'],
        'api_mode': payload['api_mode'],
        'primary_metric': payload['primary_metric'],
        'profit_default_metric': payload['profit_default_metric'],
        'profit_rule_id': payload['profit_rule_id'],
        'share_level': payload['share_level'],
        'share_predictions': payload['share_predictions'],
        'prediction_method': payload['prediction_method'],
        'system_prompt': payload['system_prompt'],
        'data_injection_mode': payload['data_injection_mode'],
        'user_algorithm_fallback_strategy': payload['user_algorithm_fallback_strategy'],
        'prediction_targets': payload['prediction_targets'],
        'history_window': payload['history_window'],
        'temperature': payload['temperature'],
        'enabled': payload['enabled']
    }
    db.update_predictor(predictor_id, updates)

    predictor = db.get_predictor(predictor_id, include_secret=True)
    return jsonify({
        'message': '预测方案更新成功',
        'predictor': _serialize_predictor(predictor)
    })


@app.route('/api/predictors/<int:predictor_id>', methods=['DELETE'])
@login_required
def delete_predictor(predictor_id: int):
    user_id = get_current_user_id()
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return jsonify({'error': '无权操作此预测方案'}), 403

    db.delete_predictor(predictor_id)
    return jsonify({'message': '预测方案已删除'})


@app.route('/api/predictors/<int:predictor_id>/dashboard', methods=['GET'])
@login_required
def get_predictor_dashboard(predictor_id: int):
    user_id = get_current_user_id()
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return jsonify({'error': '无权访问此预测方案'}), 403

    return jsonify(_get_predictor_dashboard_data(predictor_id))


@app.route('/api/predictors/<int:predictor_id>/history', methods=['GET'])
@login_required
def get_predictor_history(predictor_id: int):
    user_id = get_current_user_id()
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return jsonify({'error': '无权访问此预测方案'}), 403

    requested_tab = request.args.get('tab', 'predictions')
    requested_page = _parse_positive_int(request.args.get('page'), 1, minimum=1, maximum=100000)
    requested_page_size = _parse_optional_int(request.args.get('page_size'))
    requested_status = request.args.get('status', 'all')
    requested_outcome = request.args.get('outcome', 'all')
    return jsonify(_get_predictor_history_data(
        predictor_id,
        tab=requested_tab,
        page=requested_page,
        page_size=requested_page_size,
        status=requested_status,
        outcome=requested_outcome
    ))


@app.route('/api/predictors/<int:predictor_id>/stats', methods=['GET'])
@login_required
def get_predictor_stats(predictor_id: int):
    user_id = get_current_user_id()
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return jsonify({'error': '无权访问此预测方案'}), 403

    return jsonify(db.get_predictor_stats(predictor_id))


@app.route('/api/predictors/<int:predictor_id>/simulation', methods=['GET'])
@login_required
def get_predictor_simulation(predictor_id: int):
    user_id = get_current_user_id()
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return jsonify({'error': '无权访问此预测方案'}), 403

    predictor = db.get_predictor(predictor_id, include_secret=False)
    if predictor and not supports_profit_simulation(predictor.get('lottery_type')):
        return jsonify({'error': '当前彩种暂不支持收益模拟'}), 400
    lottery_type = normalize_lottery_type(predictor.get('lottery_type') if predictor else 'pc28')
    requested_metric = request.args.get('metric') or (profit_simulator.get_default_metric(predictor) if predictor else None)
    profit_rule_id = request.args.get('profit_rule_id') or (predictor.get('profit_rule_id') if predictor else DEFAULT_PROFIT_RULE_ID)
    odds_profile = request.args.get('odds_profile', DEFAULT_ODDS_PROFILE)
    period_key = request.args.get('period_key') or profit_simulator.get_default_period_key(predictor)
    bet_profile_id = request.args.get('bet_profile_id', type=int)
    bet_mode = request.args.get('bet_mode')
    base_stake = request.args.get('base_stake', type=float)
    multiplier = request.args.get('multiplier', type=float)
    max_steps = request.args.get('max_steps', type=int)
    bet_profile = None

    if bet_profile_id:
        bet_profile = db.get_bet_profile(bet_profile_id)
        if not bet_profile or int(bet_profile.get('user_id') or 0) != int(user_id):
            return jsonify({'error': '下注策略不存在'}), 404
        if normalize_lottery_type(bet_profile.get('lottery_type')) != lottery_type:
            return jsonify({'error': '下注策略和彩种不匹配'}), 400
        bet_mode = bet_profile.get('mode')
        base_stake = bet_profile.get('base_stake')
        multiplier = bet_profile.get('multiplier')
        max_steps = bet_profile.get('max_steps')

    try:
        simulation = profit_simulator.build_profit_simulation(
            predictor_id,
            requested_metric=requested_metric,
            profit_rule_id=profit_rule_id,
            odds_profile=odds_profile,
            period_key=period_key,
            bet_mode=bet_mode,
            base_stake=base_stake,
            multiplier=multiplier,
            max_steps=max_steps,
            include_records=True
        )
        if lottery_type == 'jingcai_football':
            simulation = _apply_jingcai_simulation_sale_filter(predictor_id, simulation)
        return jsonify({
            'predictor_id': predictor_id,
            'bet_profile': _serialize_bet_profile(bet_profile) if bet_profile_id and bet_profile else None,
            'simulation': _serialize_profit_simulation(simulation, include_records=True)
        })
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400


@app.route('/api/predictors/<int:predictor_id>/jingcai/replay', methods=['POST'])
@login_required
def replay_jingcai_batch(predictor_id: int):
    user_id = get_current_user_id()
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return jsonify({'error': '无权操作此预测方案'}), 403

    predictor = db.get_predictor(predictor_id, include_secret=False)
    if not predictor or normalize_lottery_type(predictor.get('lottery_type')) != 'jingcai_football':
        return jsonify({'error': '当前方案不是竞彩足球方案'}), 400

    data = request.get_json() or {}
    replay_date = str(data.get('date') or '').strip()
    limit = data.get('limit', 20)
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        limit = 20

    try:
        result = jingcai_football_service.replay_batch(db, predictor_id, replay_date, limit=limit)
        overview = result.get('overview') or {}
        return jsonify({
            'predictor_id': predictor_id,
            'message': result.get('message') or '批次回放完成',
            'run_key': result.get('run_key'),
            'used_is_prized': result.get('used_is_prized'),
            'warning': result.get('warning'),
            'overview': {
                **overview,
                'recent_events': [_serialize_lottery_event(item) for item in overview.get('recent_events', [])]
            },
            'run': _serialize_prediction_run(result.get('run')) if result.get('run') else None
        })
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/jingcai-football/history-backfill', methods=['POST'])
@admin_required
def backfill_jingcai_history():
    data = request.get_json() or {}
    start_date = str(data.get('start_date') or '').strip()
    end_date = str(data.get('end_date') or start_date).strip()
    include_details = _parse_bool(data.get('include_details'), default=True)
    max_days = _parse_positive_int(data.get('max_days'), default=31, minimum=1, maximum=90)
    try:
        payload = jingcai_football_service.run_backfill_job(
            db,
            start_date=start_date,
            end_date=end_date,
            include_details=include_details,
            max_days=max_days,
            trigger_source='manual'
        )
        return jsonify({
            'message': '竞彩足球历史数据补齐完成',
            'job': _serialize_jingcai_backfill_job(payload.get('job')),
            'result': payload.get('result') or {}
        })
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/jingcai-football/data-health', methods=['GET'])
@admin_required
def get_jingcai_data_health():
    return jsonify(db.build_jingcai_data_health())


@app.route('/api/export/predictors/<int:predictor_id>/signals', methods=['GET'])
def export_predictor_signals(predictor_id: int):
    predictor = db.get_predictor(predictor_id, include_secret=False)
    if not predictor:
        return jsonify({'error': '预测方案不存在'}), 404

    lottery_type = normalize_lottery_type(predictor.get('lottery_type'))
    if lottery_type != 'pc28':
        return jsonify({'error': '当前仅支持导出 PC28 信号'}), 400

    view = str(request.args.get('view') or 'execution').strip().lower()
    if view not in {'execution', 'analysis'}:
        return jsonify({'error': 'view 仅支持 execution 或 analysis'}), 400

    latest_prediction = db.get_latest_prediction(predictor_id)
    if not latest_prediction:
        return jsonify({
            'predictor_id': predictor_id,
            'lottery_type': lottery_type,
            'view': view,
            'items': []
        })

    item = (
        _build_pc28_execution_signal_view(predictor, latest_prediction)
        if view == 'execution'
        else _build_pc28_analysis_signal_view(predictor, latest_prediction)
    )
    return jsonify({
        'predictor_id': predictor_id,
        'lottery_type': lottery_type,
        'view': view,
        'items': [item] if item else []
    })


@app.route('/api/export/predictors/<int:predictor_id>/performance', methods=['GET'])
def export_predictor_performance(predictor_id: int):
    predictor = db.get_predictor(predictor_id, include_secret=False)
    if not predictor:
        return jsonify({'error': '预测方案不存在'}), 404

    lottery_type = normalize_lottery_type(predictor.get('lottery_type'))
    if lottery_type != 'pc28':
        return jsonify({'error': '当前仅支持导出 PC28 表现统计'}), 400

    return jsonify(_build_pc28_performance_export_view(predictor))


@app.route('/api/predictors/<int:predictor_id>/jingcai/settle', methods=['POST'])
@login_required
def settle_jingcai_predictions(predictor_id: int):
    return _handle_settle_jingcai_predictions(predictor_id)


@app.route('/api/predictors/<int:predictor_id>/settle-now', methods=['POST'])
@login_required
def settle_jingcai_predictions_now(predictor_id: int):
    return _handle_settle_jingcai_predictions(predictor_id)


def _handle_settle_jingcai_predictions(predictor_id: int):
    user_id = get_current_user_id()
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return jsonify({'error': '无权操作此预测方案'}), 403

    predictor = db.get_predictor(predictor_id, include_secret=False)
    if not predictor or normalize_lottery_type(predictor.get('lottery_type')) != 'jingcai_football':
        return jsonify({'error': '当前方案不是竞彩足球方案'}), 400

    data = request.get_json() or {}
    run_key = str(data.get('run_key') or '').strip() or None

    try:
        result = jingcai_football_service.settle_predictor_runs(db, predictor_id, run_key=run_key)
        return jsonify({
            'predictor_id': predictor_id,
            'message': result.get('message') or '结算完成',
            'run_keys': result.get('run_keys') or [],
            'settled_items_count': int(result.get('settled_items_count') or 0),
            'settled_runs_count': int(result.get('settled_runs_count') or 0),
            'pending_runs_count': int(result.get('pending_runs_count') or 0),
            'runs': [
                {
                    **item,
                    'run': _serialize_prediction_run(item.get('run')) if item.get('run') else None
                }
                for item in (result.get('runs') or [])
            ]
        })
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/predictors/<int:predictor_id>/predict-now', methods=['POST'])
@login_required
def predict_now(predictor_id: int):
    user_id = get_current_user_id()
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return jsonify({'error': '无权操作此预测方案'}), 403

    try:
        prediction = lottery_runtime.generate_prediction(predictor_id)
        predictor = db.get_predictor(predictor_id, include_secret=False)
        lottery_type = normalize_lottery_type(predictor.get('lottery_type') if predictor else 'pc28')
        return jsonify({
            'message': '预测执行完成',
            'prediction': _serialize_prediction(prediction) if lottery_type == 'pc28' else _serialize_prediction_run(prediction)
        })
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/predictors/<int:predictor_id>/resume-auto-pause', methods=['POST'])
@login_required
def resume_predictor_auto_pause(predictor_id: int):
    user_id = get_current_user_id()
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return jsonify({'error': '无权操作此预测方案'}), 403

    predictor = db.get_predictor(predictor_id, include_secret=False)
    if not predictor:
        return jsonify({'error': '预测方案不存在'}), 404

    prediction_guard.resume_predictor(predictor_id)
    updated = db.get_predictor(predictor_id, include_secret=True)
    return jsonify({
        'message': '方案已解除自动暂停',
        'predictor': _serialize_predictor(updated)
    })


# ============= 共识分析 API =============

def _resolve_consensus_scope(scope_arg: str | None) -> tuple[str, int | None]:
    """
    解析共识接口的 scope 参数。
    返回 (scope, user_id)。当 scope='all' 时校验管理员权限并返回 user_id=None。
    若无权限抛 PermissionError。
    """
    scope = (scope_arg or 'user').strip().lower()
    if scope not in {'user', 'all'}:
        scope = 'user'
    if scope == 'all':
        if not get_current_user_is_admin():
            raise PermissionError('需要管理员权限')
        return 'all', None
    user_id = get_current_user_id()
    if not user_id:
        raise PermissionError('未登录')
    return 'user', int(user_id)


def _resolve_consensus_window(window_arg: str | None) -> int | None:
    """
    解析时间窗口参数。
    支持 7 / 30 / 90 / all（或空 -> 默认 30）。
    """
    text = (window_arg or '30').strip().lower()
    if text in {'all', 'full', '0'}:
        return None
    try:
        days = int(text)
    except (TypeError, ValueError):
        days = 30
    if days <= 0:
        return None
    return min(days, 3650)


@app.route('/api/consensus/analysis', methods=['GET'])
@login_required
def api_consensus_analysis():
    """返回方案共识分析结果。"""
    try:
        scope, user_id = _resolve_consensus_scope(request.args.get('scope'))
    except PermissionError as exc:
        return jsonify({'error': str(exc)}), 403

    lottery_type = normalize_lottery_type(request.args.get('lottery_type') or 'jingcai_football')
    if lottery_type != 'jingcai_football':
        return jsonify({'error': '当前只支持竞彩足球的共识分析'}), 400

    window = _resolve_consensus_window(request.args.get('window'))
    try:
        analysis = build_consensus_analysis(
            db,
            user_id=user_id,
            lottery_type=lottery_type,
            time_window_days=window
        )
    except Exception as exc:
        runtime_logger.exception('build_consensus_analysis 失败: %s', exc)
        return jsonify({'error': f'分析失败: {exc}'}), 500
    return jsonify({**analysis, 'scope': scope})


@app.route('/api/consensus/today-detail', methods=['GET'])
@login_required
def api_consensus_today_detail():
    """
    返回今日（含未结算）所有比赛的方案预测明细 + 最近 N 条历史样本，给 AI 聊天用。
    """
    try:
        scope, user_id = _resolve_consensus_scope(request.args.get('scope'))
    except PermissionError as exc:
        return jsonify({'error': str(exc)}), 403

    lottery_type = normalize_lottery_type(request.args.get('lottery_type') or 'jingcai_football')
    if lottery_type != 'jingcai_football':
        return jsonify({'error': '当前只支持竞彩足球'}), 400

    history_limit = max(20, min(int(request.args.get('history_limit') or 100), 300))

    # 复用 analysis_service 的内部函数：用 build_consensus_analysis 生成的 today_recommendations 已经够用，
    # 这里再补充原始 prediction_payload 明细
    from services.consensus_analysis_service import (
        _select_predictor_pool,
        _fetch_prediction_items,
        _group_by_match
    )
    pool = _select_predictor_pool(db, user_id=user_id, lottery_type=lottery_type)
    pids = [p['id'] for p in pool]
    today_items = _fetch_prediction_items(
        db,
        predictor_ids=pids,
        lottery_type=lottery_type,
        only_settled=False,
        only_pending=True,
        time_window_days=None
    )
    history_items_raw = _fetch_prediction_items(
        db,
        predictor_ids=pids,
        lottery_type=lottery_type,
        only_settled=True,
        only_pending=False,
        time_window_days=30
    )
    # 历史只截取 history_limit 个 item（不是 match）作为给 AI 的样本
    history_items = history_items_raw[:history_limit]

    name_lookup = {p['id']: p.get('name') or f"方案#{p['id']}" for p in pool}

    def _slim(item):
        return {
            'predictor_id': item['predictor_id'],
            'predictor_name': name_lookup.get(item['predictor_id'], str(item['predictor_id'])),
            'run_key': item['run_key'],
            'event_key': item['event_key'],
            'title': item['title'],
            'prediction': item['prediction'],
            'hit': item['hit'],
            'actual': item['actual'],
            'status': item['status']
        }

    today_grouped = _group_by_match(today_items)
    today_payload = []
    for (run_key, event_key), items in today_grouped.items():
        today_payload.append({
            'run_key': run_key,
            'event_key': event_key,
            'title': next((it.get('title') or '' for it in items), ''),
            'predictions': [_slim(it) for it in items]
        })

    return jsonify({
        'scope': scope,
        'lottery_type': lottery_type,
        'predictor_pool': [
            {'id': p['id'], 'name': name_lookup[p['id']], 'engine_type': p.get('engine_type')}
            for p in pool
        ],
        'today_matches': today_payload,
        'history_sample': [_slim(it) for it in history_items]
    })


@app.route('/api/consensus/chat', methods=['POST'])
@login_required
def api_consensus_chat():
    """AI 深度分析聊天接口。"""
    data = request.get_json() or {}
    user_message = str(data.get('message') or '').strip()
    chat_history = data.get('chat_history') if isinstance(data.get('chat_history'), list) else []

    if not user_message:
        return jsonify({'error': '请输入问题'}), 400

    # 解析 AI 配置：优先用户手填，其次复用某个方案
    api_key = str(data.get('api_key') or '').strip()
    api_url = str(data.get('api_url') or '').strip()
    model_name = str(data.get('model_name') or '').strip()
    api_mode = normalize_api_mode(data.get('api_mode') or 'auto')
    predictor_id = data.get('predictor_id')

    if not (api_key and api_url and model_name) and predictor_id:
        # 从指定方案读取 AI 配置
        try:
            predictor_id_int = int(predictor_id)
        except (TypeError, ValueError):
            return jsonify({'error': '无效的 predictor_id'}), 400
        predictor = db.get_predictor(predictor_id_int, include_secret=True)
        if not predictor:
            return jsonify({'error': '方案不存在'}), 404
        # 仅允许使用自己的方案，或管理员任意方案
        current_uid = get_current_user_id()
        if int(predictor.get('user_id') or 0) != int(current_uid) and not get_current_user_is_admin():
            return jsonify({'error': '无权使用该方案的 AI 配置'}), 403
        api_key = api_key or str(predictor.get('api_key') or '').strip()
        api_url = api_url or str(predictor.get('api_url') or '').strip()
        model_name = model_name or str(predictor.get('model_name') or '').strip()
        if not (data.get('api_mode')):
            api_mode = normalize_api_mode(predictor.get('api_mode') or 'auto')

    if not api_key or not api_url or not model_name:
        return jsonify({'error': '缺少 AI 配置（api_key/api_url/model_name）'}), 400

    consensus_summary = data.get('consensus_summary') if isinstance(data.get('consensus_summary'), dict) else None
    today_matches = data.get('today_matches') if isinstance(data.get('today_matches'), list) else []
    history_sample = data.get('history_sample') if isinstance(data.get('history_sample'), list) else []

    try:
        result = chat_consensus_analysis(
            api_key=api_key,
            api_url=api_url,
            model_name=model_name,
            api_mode=api_mode,
            user_message=user_message,
            chat_history=chat_history,
            consensus_summary=consensus_summary,
            today_matches_detail=today_matches,
            historical_sample=history_sample,
            temperature=0.4
        )
    except Exception as exc:
        runtime_logger.exception('共识聊天调用失败: %s', exc)
        return jsonify({'error': f'AI 调用失败: {exc}'}), 500

    return jsonify(result)


@app.route('/api/export/consensus/<lottery_type>', methods=['GET'])
@login_required
def export_consensus(lottery_type: str):
    """导出共识分析的标准化 JSON（与 PC28 export 风格一致）。"""
    try:
        scope, user_id = _resolve_consensus_scope(request.args.get('scope'))
    except PermissionError as exc:
        return jsonify({'error': str(exc)}), 403

    normalized_lottery = normalize_lottery_type(lottery_type)
    if normalized_lottery != 'jingcai_football':
        return jsonify({'error': '当前只支持竞彩足球的共识导出'}), 400

    window = _resolve_consensus_window(request.args.get('window'))
    try:
        analysis = build_consensus_analysis(
            db,
            user_id=user_id,
            lottery_type=normalized_lottery,
            time_window_days=window
        )
    except Exception as exc:
        runtime_logger.exception('共识导出失败: %s', exc)
        return jsonify({'error': f'导出失败: {exc}'}), 500

    return jsonify(build_export_envelope(analysis, scope=scope))


initialize_application()


if __name__ == '__main__':
    runtime_logger.info('=' * 60)
    runtime_logger.info('AI Lottery Predictor')
    runtime_logger.info('Server: http://localhost:%s', config.PORT)
    runtime_logger.info('Auto Prediction: %s', config.AUTO_PREDICTION)
    runtime_logger.info('=' * 60)
    app.run(debug=config.DEBUG, host=config.HOST, port=config.PORT, use_reloader=False)
