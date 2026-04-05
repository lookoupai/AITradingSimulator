from __future__ import annotations

import os
import threading
import time
import uuid
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
from services.lottery_runtime import LotteryRuntime
from services.pc28_service import PC28Service
from services.profit_simulator import DEFAULT_ODDS_PROFILE, ProfitSimulator
from services.prediction_engine import PredictionEngine
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
from utils.pc28 import DEFAULT_PROFIT_RULE_ID, mask_api_key, normalize_api_mode, normalize_injection_mode, normalize_share_level
from utils.timezone import get_current_beijing_time_str, utc_to_beijing


app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'pc28-predictor-secret')
CORS(app, supports_credentials=True)

APP_VERSION = str(int(time.time()))

db = Database(config.DATABASE_PATH)
pc28_service = PC28Service()
jingcai_football_service = JingcaiFootballService()
prediction_engine = PredictionEngine(db, pc28_service)
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
_scheduler_owner_id = f'{os.getpid()}-{uuid.uuid4().hex}'
AUTO_PREDICTION_SCHEDULER = 'lottery-auto-prediction'


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
    global _app_initialized, _scheduler_started

    with _init_lock:
        if not _app_initialized:
            db.init_db()
            _app_initialized = True

        if config.AUTO_PREDICTION and not _scheduler_started:
            scheduler_thread = threading.Thread(target=prediction_loop, daemon=True)
            scheduler_thread.start()
            _scheduler_started = True


def prediction_loop():
    print(f'[INFO] 多彩种自动预测线程启动，进程={os.getpid()}')
    scheduler_name = AUTO_PREDICTION_SCHEDULER
    stale_after_seconds = max(config.PREDICTION_POLL_INTERVAL * 3, 60)
    last_cycle_at = {
        'pc28': 0.0,
        'jingcai_football': 0.0
    }

    while config.AUTO_PREDICTION:
        try:
            acquired = db.try_acquire_scheduler(
                scheduler_name,
                _scheduler_owner_id,
                stale_after_seconds=stale_after_seconds
            )
            if acquired:
                db.heartbeat_scheduler(scheduler_name, _scheduler_owner_id)
                now_monotonic = time.monotonic()
                total_settled = 0
                total_predictions = []

                if now_monotonic - last_cycle_at['pc28'] >= max(config.PREDICTION_POLL_INTERVAL, 5):
                    pc28_result = lottery_runtime.run_pc28_cycle()
                    total_settled += int(pc28_result.get('settled_count') or 0)
                    total_predictions.extend(pc28_result.get('predictions') or [])
                    last_cycle_at['pc28'] = now_monotonic

                jingcai_plan = jingcai_football_service.get_scheduler_plan(db)
                if now_monotonic - last_cycle_at['jingcai_football'] >= max(int(jingcai_plan.get('interval_seconds') or 0), 5):
                    jingcai_result = lottery_runtime.run_lottery_cycle('jingcai_football')
                    total_settled += int(jingcai_result.get('settled_count') or 0)
                    total_predictions.extend(jingcai_result.get('predictions') or [])
                    last_cycle_at['jingcai_football'] = now_monotonic

                result = {
                    'settled_count': total_settled,
                    'predictions': total_predictions
                }
                db.heartbeat_scheduler(scheduler_name, _scheduler_owner_id)
                print(
                    f"[AUTO] {get_current_beijing_time_str()} settled={result['settled_count']} "
                    f"predictions={len(result['predictions'])} "
                    f"jingcai_mode={jingcai_plan.get('mode')} "
                    f"jingcai_interval={jingcai_plan.get('interval_seconds')}s"
                )

            loop_tick_seconds = max(5, min(config.PREDICTION_POLL_INTERVAL, config.JINGCAI_NEAR_MATCH_INTERVAL))
            time.sleep(loop_tick_seconds)
        except Exception as exc:
            print(f'[ERROR] 自动预测线程异常: {exc}')
            time.sleep(10)


def _serialize_predictor(predictor: dict) -> dict:
    lottery_type = normalize_lottery_type(predictor.get('lottery_type'))
    lottery_definition = get_lottery_definition(lottery_type)
    share_level = predictor.get('share_level') or ('records' if predictor.get('share_predictions') else 'stats_only')
    public_links = _build_public_links(predictor['id'])
    simulation_metrics = profit_simulator.get_metric_options(predictor) if supports_profit_simulation(lottery_type) else []
    default_simulation_metric = profit_simulator.get_default_metric(predictor) if supports_profit_simulation(lottery_type) else None
    default_profit_rule_id = profit_simulator.get_default_rule_id(predictor) if supports_profit_simulation(lottery_type) else ''
    data = {
        'id': predictor['id'],
        'user_id': predictor['user_id'],
        'name': predictor['name'],
        'lottery_type': lottery_type,
        'lottery_label': lottery_definition.label,
        'api_url': predictor['api_url'],
        'model_name': predictor['model_name'],
        'api_mode': predictor.get('api_mode') or 'auto',
        'primary_metric': predictor.get('primary_metric') or (lottery_definition.primary_metric_options[0][0] if lottery_definition.primary_metric_options else ''),
        'primary_metric_label': get_target_label(lottery_type, predictor.get('primary_metric') or ''),
        'profit_default_metric': predictor.get('profit_default_metric') or default_simulation_metric,
        'profit_rule_id': predictor.get('profit_rule_id') or default_profit_rule_id,
        'profit_rule_label': profit_simulator.get_rule_label(predictor.get('profit_rule_id') or default_profit_rule_id) if supports_profit_simulation(lottery_type) else '',
        'share_level': share_level,
        'share_level_label': _share_level_label(share_level),
        'share_predictions': share_level != 'stats_only',
        'prediction_method': predictor.get('prediction_method') or '',
        'system_prompt': predictor.get('system_prompt') or '',
        'data_injection_mode': predictor.get('data_injection_mode') or 'summary',
        'prediction_targets': normalize_prediction_targets(lottery_type, predictor.get('prediction_targets')),
        'target_options': lottery_definition.to_catalog_item()['target_options'],
        'primary_metric_options': lottery_definition.to_catalog_item()['primary_metric_options'],
        'capabilities': lottery_definition.to_catalog_item()['capabilities'],
        'simulation_metrics': simulation_metrics,
        'default_simulation_metric': default_simulation_metric,
        'profit_rule_options': profit_simulator.get_rule_options() if supports_profit_simulation(lottery_type) else [],
        'odds_profiles': profit_simulator.get_odds_profile_options() if supports_profit_simulation(lottery_type) else [],
        'history_window': predictor.get('history_window'),
        'temperature': predictor.get('temperature'),
        'enabled': bool(predictor.get('enabled')),
        'created_at': utc_to_beijing(predictor['created_at']) if predictor.get('created_at') else None,
        'updated_at': utc_to_beijing(predictor['updated_at']) if predictor.get('updated_at') else None,
        'masked_api_key': mask_api_key(predictor.get('api_key')),
        'has_api_key': bool(predictor.get('api_key')),
        'public_path': public_links['path'],
        'public_url': public_links['url'],
        'public_page_available': bool(predictor.get('enabled')) and supports_public_pages(lottery_type)
    }
    return data


def _serialize_prediction(prediction: dict) -> dict:
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
    return {
        **event,
        'issue_no': event.get('issue_no') or meta_payload.get('match_no') or meta_payload.get('match_no_value') or '',
        'created_at': utc_to_beijing(event['created_at']) if event.get('created_at') else None,
        'updated_at': utc_to_beijing(event['updated_at']) if event.get('updated_at') else None
    }


def _serialize_prediction_item_group(item: dict) -> dict:
    return {
        **item,
        'predicted_spf_label': item.get('predicted_spf_label') or '--',
        'predicted_rqspf_label': item.get('predicted_rqspf_label') or '--',
        'actual_spf_label': item.get('actual_spf_label') or '--',
        'actual_rqspf_label': item.get('actual_rqspf_label') or '--'
    }


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


def _serialize_profit_simulation(simulation: dict, include_records: bool = True) -> dict:
    payload = dict(simulation)
    if not include_records:
        payload['records'] = []
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
    return {
        'id': item['id'],
        'user_id': item['user_id'],
        'username': item.get('username'),
        'name': item['name'],
        'model_name': item.get('model_name'),
        'primary_metric': item.get('primary_metric'),
        'primary_metric_label': get_target_label(lottery_type, item.get('primary_metric')),
        'profit_default_metric': item.get('profit_default_metric') or item.get('primary_metric'),
        'profit_default_metric_label': get_target_label(lottery_type, item.get('profit_default_metric') or item.get('primary_metric')),
        'profit_rule_id': item.get('profit_rule_id') or ('pc28_high' if supports_profit_simulation(lottery_type) else ''),
        'profit_rule_label': profit_simulator.get_rule_label(item.get('profit_rule_id') or 'pc28_high') if supports_profit_simulation(lottery_type) else '',
        'lottery_type': lottery_type,
        'lottery_label': get_lottery_definition(lottery_type).label,
        'share_level': item.get('share_level'),
        'enabled': bool(item.get('enabled')),
        'created_at': utc_to_beijing(item['created_at']) if item.get('created_at') else None,
        'updated_at': utc_to_beijing(item['updated_at']) if item.get('updated_at') else None,
        'prediction_count': int(item.get('prediction_count') or 0),
        'failed_prediction_count': int(item.get('failed_prediction_count') or 0),
        'latest_issue_no': item.get('latest_issue_no'),
        'latest_prediction_update': utc_to_beijing(item['latest_prediction_update']) if item.get('latest_prediction_update') else None
    }


def _serialize_admin_failure(item: dict) -> dict:
    return {
        'issue_no': item.get('issue_no'),
        'status': item.get('status'),
        'error_message': item.get('error_message'),
        'updated_at': utc_to_beijing(item['updated_at']) if item.get('updated_at') else None,
        'predictor_id': item.get('predictor_id'),
        'predictor_name': item.get('predictor_name'),
        'username': item.get('username')
    }


def _build_admin_dashboard_data() -> dict:
    summary = db.get_admin_summary_counts()
    users = db.get_admin_users_overview()
    predictors = db.get_admin_predictors_overview()
    failed_predictions = db.get_recent_failed_predictions(limit=20)
    scheduler = db.get_scheduler_snapshot(AUTO_PREDICTION_SCHEDULER)

    scheduler_data = {
        'name': AUTO_PREDICTION_SCHEDULER,
        'auto_prediction_enabled': config.AUTO_PREDICTION,
        'poll_interval_seconds': config.PREDICTION_POLL_INTERVAL,
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

    return {
        'summary': {
            'total_users': int(summary.get('total_users') or 0),
            'admin_users': int(summary.get('admin_users') or 0),
            'total_predictors': int(summary.get('total_predictors') or 0),
            'enabled_predictors': int(summary.get('enabled_predictors') or 0),
            'shared_predictors': int(summary.get('shared_predictors') or 0),
            'total_predictions': int(summary.get('total_predictions') or 0),
            'pending_predictions': int(summary.get('pending_predictions') or 0),
            'failed_predictions': int(summary.get('failed_predictions') or 0),
            'settled_predictions': int(summary.get('settled_predictions') or 0),
            'total_draws': int(summary.get('total_draws') or 0)
        },
        'scheduler': scheduler_data,
        'users': [_serialize_admin_user(item) for item in users],
        'predictors': [_serialize_admin_predictor(item) for item in predictors],
        'recent_failures': [_serialize_admin_failure(item) for item in failed_predictions]
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


def _build_public_predictor_rankings(sort_by: str = 'recent100', metric: str = 'combo', limit: int = 10, lottery_type: str = 'pc28') -> list[dict]:
    normalized_lottery_type = normalize_lottery_type(lottery_type)
    predictors = [
        item for item in db.get_all_predictors(include_secret=False)
        if item.get('enabled') and normalize_lottery_type(item.get('lottery_type')) == normalized_lottery_type
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
            'primary_metric': predictor.get('primary_metric') or 'combo',
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
    lottery_type = normalize_lottery_type(predictor.get('lottery_type')) if predictor else 'pc28'
    if not predictor or not predictor.get('enabled') or not supports_public_pages(lottery_type):
        return None

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
        predictions = jingcai_football_service.get_recent_prediction_items(db, predictor_id, limit=20) if can_view_records else []
        current_prediction = None
        latest_prediction = predictions[0] if predictions else None
    else:
        predictions = db.get_recent_predictions(predictor_id, limit=20) if can_view_records else []
        current_prediction = next((item for item in predictions if item['status'] == 'pending'), None) if predictions else None
        latest_prediction = predictions[0] if predictions else None
    current_prediction = next((item for item in predictions if item['status'] == 'pending'), None) if predictions else None
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
            'primary_metric': predictor.get('primary_metric') or 'combo',
            'primary_metric_label': stats.get('primary_metric_label') or get_target_label(lottery_type, predictor.get('primary_metric') or ''),
            'profit_default_metric': predictor.get('profit_default_metric') or (profit_simulator.get_default_metric(predictor) if supports_profit_simulation(lottery_type) else ''),
            'profit_rule_id': predictor.get('profit_rule_id') or (profit_simulator.get_default_rule_id(predictor) if supports_profit_simulation(lottery_type) else ''),
            'profit_rule_label': profit_simulator.get_rule_label(predictor.get('profit_rule_id') or profit_simulator.get_default_rule_id(predictor)) if supports_profit_simulation(lottery_type) else '',
            'prediction_method': predictor.get('prediction_method') or '自定义策略',
            'prediction_targets': predictor.get('prediction_targets') or [],
            'simulation_metrics': profit_simulator.get_metric_options(predictor) if supports_profit_simulation(lottery_type) else [],
            'default_simulation_metric': profit_simulator.get_default_metric(predictor) if supports_profit_simulation(lottery_type) else None,
            'profit_rule_options': profit_simulator.get_rule_options() if supports_profit_simulation(lottery_type) else [],
            'odds_profiles': profit_simulator.get_odds_profile_options() if supports_profit_simulation(lottery_type) else [],
            'capabilities': get_lottery_definition(lottery_type).to_catalog_item()['capabilities'],
            'history_window': predictor.get('history_window'),
            'share_level': share_level,
            'share_level_label': _share_level_label(share_level),
            'share_predictions': can_view_records,
            'can_view_records': can_view_records,
            'can_view_analysis': can_view_analysis,
            'public_path': public_links['path'],
            'public_url': public_links['url']
        },
        'stats': stats,
        'current_prediction': _serialize_public_prediction_with_level(current_prediction, share_level) if lottery_type == 'pc28' else current_prediction,
        'latest_prediction': _serialize_public_prediction_with_level(latest_prediction, share_level) if lottery_type == 'pc28' else latest_prediction,
        'recent_predictions': [_serialize_public_prediction_with_level(item, share_level) for item in predictions] if lottery_type == 'pc28' else predictions
    }


def _parse_bool(value, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def _validate_predictor_payload(data: dict, existing_predictor: dict | None = None) -> tuple[dict, list[str]]:
    errors = []
    lottery_type = normalize_lottery_type(data.get('lottery_type') or (existing_predictor.get('lottery_type') if existing_predictor else 'pc28'))
    lottery_definition = get_lottery_definition(lottery_type)

    fallback_name = existing_predictor.get('name') if existing_predictor else ''
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

    name = str(data.get('name') or fallback_name).strip()
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
        errors.append('API Key 不能为空')

    if existing_predictor is not None and not api_key:
        api_key = existing_predictor.get('api_key', '')

    if not api_url:
        errors.append('API 地址不能为空')

    if not model_name:
        errors.append('模型名称不能为空')

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
        'prediction_targets': prediction_targets,
        'history_window': history_window,
        'temperature': temperature,
        'enabled': enabled,
        'lottery_type': lottery_type
    }
    return payload, errors


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

    return existing, resolved


@app.route('/image/<path:filename>')
def serve_image(filename):
    from flask import send_from_directory
    return send_from_directory('image', filename)


@app.route('/')
def index():
    return render_template('home.html')


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
    return render_template('dashboard.html', prompt_placeholders=get_prompt_placeholder_catalog())


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
        try:
            overview = jingcai_football_service.build_overview(db, limit=max(5, min(limit, 50)))
            return jsonify({
                **overview,
                'recent_events': [_serialize_lottery_event(item) for item in overview.get('recent_events', [])]
            })
        except Exception as exc:
            cached_events = db.get_recent_lottery_events('jingcai_football', limit=max(5, min(limit, 50)))
            if not cached_events:
                return jsonify({
                    'lottery_type': 'jingcai_football',
                    'batch_key': None,
                    'match_count': 0,
                    'open_match_count': 0,
                    'settled_match_count': 0,
                    'next_match_time': None,
                    'next_match_name': None,
                    'recent_events': [],
                    'warning': f'新浪接口不可用，且本地暂无竞彩足球缓存：{exc}'
                })
            return jsonify({
                'lottery_type': 'jingcai_football',
                'batch_key': None,
                'match_count': len(cached_events),
                'open_match_count': len([item for item in cached_events if not (item.get('meta_payload') or {}).get('settled')]),
                'settled_match_count': len([item for item in cached_events if (item.get('meta_payload') or {}).get('settled')]),
                'next_match_time': None,
                'next_match_name': None,
                'recent_events': [_serialize_lottery_event(item) for item in cached_events],
                'warning': f'新浪接口不可用，已回退本地缓存：{exc}'
            })

    return jsonify({'error': '暂不支持的彩种'}), 404


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
    bet_mode = request.args.get('bet_mode')
    base_stake = request.args.get('base_stake', type=float)
    multiplier = request.args.get('multiplier', type=float)
    max_steps = request.args.get('max_steps', type=int)
    include_records = predictor.get('can_view_records', False)

    try:
        simulation = profit_simulator.build_today_simulation(
            predictor_id,
            requested_metric=requested_metric,
            profit_rule_id=profit_rule_id,
            odds_profile=odds_profile,
            bet_mode=bet_mode,
            base_stake=base_stake,
            multiplier=multiplier,
            max_steps=max_steps,
            include_records=include_records
        )
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
    payload, errors = _validate_predictor_payload(data)
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
        lottery_type=payload['lottery_type']
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
        primary_metric=resolved['primary_metric']
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
        primary_metric=resolved['primary_metric']
    )

    optimizer_prompt = build_optimizer_prompt(
        current_prompt=resolved['system_prompt'],
        analysis=analysis,
        predictor_payload=resolved
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

    if normalize_lottery_type(resolved['lottery_type']) == 'jingcai_football':
        return jsonify({
            'prompt_template': _build_jingcai_external_prompt_template(resolved)
        })

    return jsonify({
        'prompt_template': build_external_prompt_template(resolved)
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
    payload, errors = _validate_predictor_payload(data, existing_predictor=existing)
    if errors:
        return jsonify({'error': '；'.join(errors)}), 400

    updates = {
        'name': payload['name'],
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
    requested_metric = request.args.get('metric') or (profit_simulator.get_default_metric(predictor) if predictor else None)
    profit_rule_id = request.args.get('profit_rule_id') or (predictor.get('profit_rule_id') if predictor else DEFAULT_PROFIT_RULE_ID)
    odds_profile = request.args.get('odds_profile', DEFAULT_ODDS_PROFILE)
    bet_mode = request.args.get('bet_mode')
    base_stake = request.args.get('base_stake', type=float)
    multiplier = request.args.get('multiplier', type=float)
    max_steps = request.args.get('max_steps', type=int)

    try:
        simulation = profit_simulator.build_today_simulation(
            predictor_id,
            requested_metric=requested_metric,
            profit_rule_id=profit_rule_id,
            odds_profile=odds_profile,
            bet_mode=bet_mode,
            base_stake=base_stake,
            multiplier=multiplier,
            max_steps=max_steps,
            include_records=True
        )
        return jsonify({
            'predictor_id': predictor_id,
            'simulation': _serialize_profit_simulation(simulation, include_records=True)
        })
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400


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


initialize_application()


if __name__ == '__main__':
    print('\n' + '=' * 60)
    print('AI Lottery Predictor')
    print('=' * 60)
    print(f'Server: http://localhost:{config.PORT}')
    print(f'Auto Prediction: {config.AUTO_PREDICTION}')
    print('=' * 60 + '\n')
    app.run(debug=config.DEBUG, host=config.HOST, port=config.PORT, use_reloader=False)
