from __future__ import annotations

import os
import threading
import time
import uuid

import requests
from flask import Flask, jsonify, redirect, render_template, request
from flask_cors import CORS

import config
from ai_trader import AIPredictor
from database import Database
from services.pc28_service import PC28Service
from services.prediction_engine import PredictionEngine
from utils.auth import (
    clear_current_user,
    get_current_user_id,
    hash_password,
    login_required,
    set_current_user,
    verify_password
)
from utils.pc28 import TARGET_LABELS, mask_api_key, normalize_api_mode, normalize_injection_mode, normalize_primary_metric, normalize_target_list
from utils.timezone import get_current_beijing_time_str, utc_to_beijing


app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'pc28-predictor-secret')
CORS(app, supports_credentials=True)

APP_VERSION = str(int(time.time()))

db = Database(config.DATABASE_PATH)
pc28_service = PC28Service()
prediction_engine = PredictionEngine(db, pc28_service)

_init_lock = threading.Lock()
_app_initialized = False
_scheduler_started = False
_scheduler_owner_id = f'{os.getpid()}-{uuid.uuid4().hex}'


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
    print(f'[INFO] PC28 自动预测线程启动，进程={os.getpid()}')
    scheduler_name = 'pc28-auto-prediction'
    stale_after_seconds = max(config.PREDICTION_POLL_INTERVAL * 3, 60)

    while config.AUTO_PREDICTION:
        try:
            acquired = db.try_acquire_scheduler(
                scheduler_name,
                _scheduler_owner_id,
                stale_after_seconds=stale_after_seconds
            )
            if acquired:
                db.heartbeat_scheduler(scheduler_name, _scheduler_owner_id)
                result = prediction_engine.run_auto_cycle()
                db.heartbeat_scheduler(scheduler_name, _scheduler_owner_id)
                print(
                    f"[AUTO] {get_current_beijing_time_str()} settled={result['settled_count']} "
                    f"predictions={len(result['predictions'])}"
                )

            time.sleep(max(config.PREDICTION_POLL_INTERVAL, 5))
        except Exception as exc:
            print(f'[ERROR] 自动预测线程异常: {exc}')
            time.sleep(10)


def _serialize_predictor(predictor: dict) -> dict:
    data = {
        'id': predictor['id'],
        'user_id': predictor['user_id'],
        'name': predictor['name'],
        'lottery_type': predictor['lottery_type'],
        'api_url': predictor['api_url'],
        'model_name': predictor['model_name'],
        'api_mode': predictor.get('api_mode') or 'auto',
        'primary_metric': predictor.get('primary_metric') or 'combo',
        'prediction_method': predictor.get('prediction_method') or '',
        'system_prompt': predictor.get('system_prompt') or '',
        'data_injection_mode': predictor.get('data_injection_mode') or 'summary',
        'prediction_targets': predictor.get('prediction_targets') or [],
        'history_window': predictor.get('history_window'),
        'temperature': predictor.get('temperature'),
        'enabled': bool(predictor.get('enabled')),
        'created_at': utc_to_beijing(predictor['created_at']) if predictor.get('created_at') else None,
        'updated_at': utc_to_beijing(predictor['updated_at']) if predictor.get('updated_at') else None,
        'masked_api_key': mask_api_key(predictor.get('api_key')),
        'has_api_key': bool(predictor.get('api_key'))
    }
    return data


def _serialize_prediction(prediction: dict) -> dict:
    score_values = [
        prediction.get('hit_number'),
        prediction.get('hit_big_small'),
        prediction.get('hit_odd_even'),
        prediction.get('hit_combo')
    ]
    effective_scores = [value for value in score_values if value is not None]

    data = {
        **prediction,
        'requested_target_labels': [TARGET_LABELS.get(item, item) for item in prediction.get('requested_targets', [])],
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


def _build_public_predictor_rankings(sort_by: str = 'recent100', metric: str = 'combo', limit: int = 10) -> list[dict]:
    predictors = [item for item in db.get_all_predictors(include_secret=False) if item.get('enabled')]
    ranked_items = []

    for predictor in predictors:
        stats = db.get_predictor_stats(predictor['id'])
        metric_stats = (stats.get('metrics') or {}).get(metric) or {}
        recent20 = metric_stats.get('recent_20') or {}
        recent100 = metric_stats.get('recent_100') or {}
        overall = metric_stats.get('overall') or {}
        settled_rows = [row for row in db.get_recent_predictions(predictor['id'], limit=None) if row['status'] == 'settled']
        streaks = db._build_streak_stats(settled_rows, metric)
        user = db.get_user_by_id(predictor['user_id'])

        ranked_items.append({
            'predictor_id': predictor['id'],
            'predictor_name': predictor['name'],
            'username': user['username'] if user else 'unknown',
            'model_name': predictor['model_name'],
            'primary_metric': predictor.get('primary_metric') or 'combo',
            'primary_metric_label': stats.get('primary_metric_label') or '组合',
            'metric': metric,
            'metric_label': metric_stats.get('label') or metric,
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


def _parse_bool(value, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def _validate_predictor_payload(data: dict, existing_predictor: dict | None = None) -> tuple[dict, list[str]]:
    errors = []

    fallback_name = existing_predictor.get('name') if existing_predictor else ''
    fallback_api_url = existing_predictor.get('api_url') if existing_predictor else ''
    fallback_model_name = existing_predictor.get('model_name') if existing_predictor else ''
    fallback_api_mode = existing_predictor.get('api_mode') if existing_predictor else 'auto'
    fallback_primary_metric = existing_predictor.get('primary_metric') if existing_predictor else 'combo'
    fallback_method = existing_predictor.get('prediction_method') if existing_predictor else ''
    fallback_prompt = existing_predictor.get('system_prompt') if existing_predictor else ''
    fallback_injection_mode = existing_predictor.get('data_injection_mode') if existing_predictor else 'summary'

    name = str(data.get('name') or fallback_name).strip()
    api_key = str(data.get('api_key') or '').strip()
    api_url = str(data.get('api_url') or fallback_api_url).strip()
    model_name = str(data.get('model_name') or fallback_model_name).strip()
    api_mode = normalize_api_mode(data.get('api_mode') or fallback_api_mode)
    primary_metric = normalize_primary_metric(data.get('primary_metric') or fallback_primary_metric)
    prediction_method = str(data.get('prediction_method') or fallback_method).strip()
    system_prompt = str(data.get('system_prompt') or fallback_prompt).strip()
    data_injection_mode = normalize_injection_mode(data.get('data_injection_mode') or fallback_injection_mode)
    history_window = data.get('history_window', existing_predictor.get('history_window') if existing_predictor else config.DEFAULT_HISTORY_WINDOW)
    temperature = data.get('temperature', existing_predictor.get('temperature') if existing_predictor else config.DEFAULT_PREDICTION_TEMPERATURE)
    enabled = _parse_bool(data.get('enabled'), existing_predictor.get('enabled') if existing_predictor else True)
    prediction_targets = normalize_target_list(
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

    if primary_metric in {'number', 'big_small', 'odd_even', 'combo'} and primary_metric not in prediction_targets:
        errors.append('主玩法必须包含在预测目标中')
    if primary_metric in {'double_group', 'kill_group'} and 'combo' not in prediction_targets:
        errors.append('双组/杀组统计依赖组合预测，请勾选组合目标')

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
        'prediction_method': prediction_method or '自定义策略',
        'system_prompt': system_prompt,
        'data_injection_mode': data_injection_mode,
        'prediction_targets': prediction_targets,
        'history_window': history_window,
        'temperature': temperature,
        'enabled': enabled,
        'lottery_type': 'pc28'
    }
    return payload, errors


def _get_predictor_dashboard_data(predictor_id: int) -> dict:
    predictor = db.get_predictor(predictor_id, include_secret=True)
    stats = db.get_predictor_stats(predictor_id)
    recent_predictions = db.get_recent_predictions(predictor_id, limit=30)
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


@app.route('/dashboard')
def dashboard():
    if not get_current_user_id():
        return redirect('/login')
    return render_template('dashboard.html')


@app.route('/api/health', methods=['GET'])
def healthcheck():
    return jsonify({
        'status': 'ok',
        'time': get_current_beijing_time_str(),
        'auto_prediction': config.AUTO_PREDICTION
    })


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
    user_id = db.create_user(username, password_hash, email)
    set_current_user(user_id, username)

    return jsonify({
        'message': '注册成功',
        'user': {
            'id': user_id,
            'username': username,
            'email': email
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

    set_current_user(user['id'], user['username'])
    return jsonify({
        'message': '登录成功',
        'user': {
            'id': user['id'],
            'username': user['username'],
            'email': user.get('email')
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
            user_id = db.create_user(
                linuxdo_username,
                hash_password(f'linuxdo_oauth_{linuxdo_id}'),
                email
            )
        else:
            user_id = user['id']

        set_current_user(user_id, linuxdo_username)
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

    return jsonify({
        'id': user['id'],
        'username': user['username'],
        'email': user.get('email'),
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


@app.route('/api/public/predictors', methods=['GET'])
def get_public_predictors():
    sort_by = request.args.get('sort_by', 'recent100')
    metric = request.args.get('metric', 'combo')
    limit = request.args.get('limit', 10, type=int)
    limit = max(1, min(limit, 50))

    return jsonify({
        'sort_by': sort_by,
        'metric': metric,
        'items': _build_public_predictor_rankings(sort_by=sort_by, metric=metric, limit=limit)
    })


# ============ Predictor APIs ============


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
        prediction_method=payload['prediction_method'],
        system_prompt=payload['system_prompt'],
        data_injection_mode=payload['data_injection_mode'],
        prediction_targets=payload['prediction_targets'],
        history_window=payload['history_window'],
        temperature=payload['temperature'],
        enabled=payload['enabled'],
        lottery_type='pc28'
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

    predictor_id = data.get('predictor_id')
    existing = None
    if predictor_id:
        if not db.predictor_exists_for_user(int(predictor_id), user_id):
            return jsonify({'error': '无权访问此预测方案'}), 403
        existing = db.get_predictor(int(predictor_id), include_secret=True)

    fallback_api_url = existing.get('api_url') if existing else ''
    fallback_model_name = existing.get('model_name') if existing else ''
    fallback_api_mode = existing.get('api_mode') if existing else 'auto'
    api_key = str(data.get('api_key') or '').strip() or (existing.get('api_key') if existing else '')
    api_url = str(data.get('api_url') or fallback_api_url).strip()
    model_name = str(data.get('model_name') or fallback_model_name).strip()
    api_mode = normalize_api_mode(data.get('api_mode') or fallback_api_mode)

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


@app.route('/api/predictors/<int:predictor_id>/predict-now', methods=['POST'])
@login_required
def predict_now(predictor_id: int):
    user_id = get_current_user_id()
    if not db.predictor_exists_for_user(predictor_id, user_id):
        return jsonify({'error': '无权操作此预测方案'}), 403

    try:
        prediction = prediction_engine.generate_prediction(predictor_id)
        return jsonify({
            'message': '预测执行完成',
            'prediction': _serialize_prediction(prediction)
        })
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


initialize_application()


if __name__ == '__main__':
    print('\n' + '=' * 60)
    print('AI PC28 Predictor')
    print('=' * 60)
    print(f'Server: http://localhost:{config.PORT}')
    print(f'Auto Prediction: {config.AUTO_PREDICTION}')
    print('=' * 60 + '\n')
    app.run(debug=config.DEBUG, host=config.HOST, port=config.PORT, use_reloader=False)
