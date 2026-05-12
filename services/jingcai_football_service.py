"""
竞彩足球数据与预测服务
"""
from __future__ import annotations

from datetime import datetime, timedelta
import re
import time

import requests

from ai_trader import AIPredictor
import config
from services import machine_prediction
from services.prediction_guard import AIPredictionError, PredictionGuardService
from utils import jingcai_football as football_utils
from utils.jingcai_sources import (
    SINA_DETAIL_HEADERS,
    SINA_JINGCAI_DETAIL_URL,
    SINA_JINGCAI_URL,
    SINA_LIST_HEADERS,
    build_sina_detail_params,
    build_sina_match_list_params
)
from utils.predictor_engine import is_user_algorithm_key, uses_ai_engine
from utils.timezone import get_current_beijing_time, parse_beijing_time


DEFAULT_TIMEOUT = getattr(config, 'JINGCAI_REQUEST_TIMEOUT', 15)
DETAIL_CACHE_SECONDS = getattr(config, 'JINGCAI_DETAIL_CACHE_SECONDS', 6 * 60 * 60)
HISTORY_RESULT_REFRESH_GRACE_MINUTES = max(
    30,
    int(getattr(config, 'JINGCAI_HISTORY_RESULT_REFRESH_GRACE_MINUTES', 90))
)
UNSETTLED_EXPIRE_HOURS = max(1, int(getattr(config, 'JINGCAI_UNSETTLED_EXPIRE_HOURS', 72)))
PREDICTION_BATCH_MAX_MATCHES = max(1, int(getattr(config, 'JINGCAI_PREDICTION_BATCH_MAX_MATCHES', 10)))
PREDICTION_PROMPT_SOFT_LIMIT = max(4000, int(getattr(config, 'JINGCAI_PREDICTION_PROMPT_SOFT_LIMIT', 20000)))
PREDICTION_MAX_OUTPUT_TOKENS = max(800, int(getattr(config, 'JINGCAI_PREDICTION_MAX_OUTPUT_TOKENS', 3200)))
PREDICTION_CONCISE_REASONING_THRESHOLD = max(1, int(getattr(config, 'JINGCAI_PREDICTION_CONCISE_REASONING_THRESHOLD', 8)))
FAILED_ITEM_RETRY_MAX_ATTEMPTS = max(1, int(getattr(config, 'JINGCAI_FAILED_ITEM_RETRY_MAX_ATTEMPTS', 3)))
FAILED_ITEM_RETRY_COOLDOWN_SECONDS = max(60, int(getattr(config, 'JINGCAI_FAILED_ITEM_RETRY_COOLDOWN_SECONDS', 15 * 60)))


class JingcaiFootballService:
    lottery_type = 'jingcai_football'

    def __init__(self, timeout: int = DEFAULT_TIMEOUT, prediction_guard: PredictionGuardService | None = None, notification_service=None):
        self.timeout = timeout
        self.prediction_guard = prediction_guard
        self.notification_service = notification_service
        self._next_auto_run_at = None
        self._last_auto_reason = ''

    def _request_payload(
        self,
        date: str = '',
        is_prized: str = '',
        game_types: str = 'spf',
        cache_bust: bool = False
    ) -> dict:
        response = requests.get(
            SINA_JINGCAI_URL,
            params=build_sina_match_list_params(
                date=date,
                is_prized=is_prized,
                game_types=game_types,
                cache_bust=int(datetime.utcnow().timestamp() * 1000) if cache_bust else None
            ),
            headers=SINA_LIST_HEADERS,
            timeout=self.timeout
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get('result') or {}
        status = (result.get('status') or {}).get('code')
        if status not in {0, '0', None}:
            raise ValueError(f'新浪竞彩接口返回失败: {payload}')
        return result

    def _request_detail_payload(self, cat1: str, params: dict, host: str = 'mix') -> dict:
        base_url = SINA_JINGCAI_DETAIL_URL if host == 'mix' else SINA_JINGCAI_URL
        headers = SINA_DETAIL_HEADERS if host == 'mix' else SINA_LIST_HEADERS
        response = requests.get(
            base_url,
            params=build_sina_detail_params(cat1, params),
            headers=headers,
            timeout=self.timeout
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get('result') or {}
        status = (result.get('status') or {}).get('code')
        if status not in {0, '0', None}:
            raise ValueError(f'新浪竞彩详情接口返回失败: cat1={cat1}, payload={payload}')
        return result.get('data') or {}

    def fetch_matches(
        self,
        date: str = '',
        is_prized: str = '',
        game_types: str = 'spf',
        cache_bust: bool = False
    ) -> dict:
        result = self._request_payload(
            date=date,
            is_prized=is_prized,
            game_types=game_types,
            cache_bust=cache_bust
        )
        batch_key = str(result.get('date') or date or '').strip()
        matches = [self._normalize_match(item, batch_key=batch_key) for item in (result.get('data') or [])]
        matches = [item for item in matches if item]
        return {
            'lottery_type': self.lottery_type,
            'batch_key': batch_key,
            'dates': result.get('dates') or [],
            'matches': matches
        }

    def sync_matches(
        self,
        db,
        date: str = '',
        is_prized: str = '',
        game_types: str = 'spf',
        cache_bust: bool = False
    ) -> dict:
        payload = self.fetch_matches(
            date=date,
            is_prized=is_prized,
            game_types=game_types,
            cache_bust=cache_bust
        )
        event_keys = [item.get('event_key') for item in payload['matches'] if item.get('event_key')]
        existing_event_map = db.get_lottery_event_map(self.lottery_type, event_keys) if event_keys else {}
        events = [
            self._build_event_record(
                item,
                existing_meta=(existing_event_map.get(item.get('event_key')) or {}).get('meta_payload') or {}
            )
            for item in payload['matches']
        ]
        db.upsert_lottery_events(events)
        return payload

    def backfill_history(
        self,
        db,
        start_date: str,
        end_date: str,
        include_details: bool = True,
        max_days: int = 31
    ) -> dict:
        start = self._parse_date(start_date)
        end = self._parse_date(end_date)
        if not start or not end:
            raise ValueError('补齐任务需要 YYYY-MM-DD 格式的开始和结束日期')
        if end < start:
            raise ValueError('结束日期不能早于开始日期')

        days = (end - start).days + 1
        if days > max(1, int(max_days or 31)):
            raise ValueError(f'单次最多补齐 {max_days} 天历史数据')

        results = []
        total_matches = 0
        total_details = 0
        for offset in range(days):
            current_date = (start + timedelta(days=offset)).strftime('%Y-%m-%d')
            payload = self.sync_matches(db, date=current_date, is_prized='1', game_types='spf', cache_bust=True)
            matches = payload.get('matches') or []
            detail_count = 0
            if include_details:
                for match in matches:
                    bundle = self.get_or_fetch_match_detail_bundle(db, match, force_refresh=False)
                    if bundle:
                        detail_count += 1
            total_matches += len(matches)
            total_details += detail_count
            results.append({
                'date': current_date,
                'batch_key': payload.get('batch_key') or current_date,
                'match_count': len(matches),
                'detail_count': detail_count
            })

        return {
            'lottery_type': self.lottery_type,
            'source_provider': 'sina',
            'start_date': start.strftime('%Y-%m-%d'),
            'end_date': end.strftime('%Y-%m-%d'),
            'day_count': days,
            'match_count': total_matches,
            'detail_count': total_details,
            'results': results
        }

    def run_backfill_job(
        self,
        db,
        start_date: str,
        end_date: str,
        include_details: bool = True,
        max_days: int = 31,
        trigger_source: str = 'manual'
    ) -> dict:
        start = self._parse_date(start_date)
        end = self._parse_date(end_date)
        if not start or not end:
            raise ValueError('补齐任务需要 YYYY-MM-DD 格式的开始和结束日期')
        requested_days = (end - start).days + 1
        job_id = db.create_jingcai_backfill_job(
            trigger_source=trigger_source,
            start_date=start.strftime('%Y-%m-%d'),
            end_date=end.strftime('%Y-%m-%d'),
            include_details=include_details,
            requested_days=requested_days
        )
        db.update_jingcai_backfill_job(job_id, {
            'status': 'running',
            'started_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })
        try:
            result = self.backfill_history(
                db,
                start_date=start.strftime('%Y-%m-%d'),
                end_date=end.strftime('%Y-%m-%d'),
                include_details=include_details,
                max_days=max_days
            )
            db.update_jingcai_backfill_job(job_id, {
                'status': 'succeeded',
                'match_count': int(result.get('match_count') or 0),
                'detail_count': int(result.get('detail_count') or 0),
                'result_json': result,
                'finished_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            })
            return {
                'job': db.get_latest_jingcai_backfill_job(),
                'result': result
            }
        except Exception as exc:
            db.update_jingcai_backfill_job(job_id, {
                'status': 'failed',
                'error_message': str(exc),
                'finished_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            })
            raise

    def run_scheduled_history_backfill(
        self,
        db,
        lookback_days: int,
        include_details: bool = True,
        max_days: int = 31
    ) -> dict:
        end = get_current_beijing_time().date() - timedelta(days=1)
        days = max(1, min(int(lookback_days or 7), int(max_days or 31)))
        start = end - timedelta(days=days - 1)
        return self.run_backfill_job(
            db,
            start_date=start.strftime('%Y-%m-%d'),
            end_date=end.strftime('%Y-%m-%d'),
            include_details=include_details,
            max_days=max_days,
            trigger_source='scheduled'
        )

    def fetch_match_detail_bundle(self, match: dict) -> dict:
        raw_item = match.get('raw_item') or {}
        match_id = str(match.get('source_match_id') or raw_item.get('matchId') or '').strip()
        if not match_id:
            return {}

        bundle = {}
        bundle['detail'] = self._safe_detail_request('footballMatchDetail', {'matchId': match_id, 't': int(datetime.utcnow().timestamp() * 1000)})
        detail_payload = bundle.get('detail') or {}
        team1_id = str(detail_payload.get('team1Id') or raw_item.get('team1Id') or '').strip()
        team2_id = str(detail_payload.get('team2Id') or raw_item.get('team2Id') or '').strip()
        match_timestamp = str(detail_payload.get('matchTime') or raw_item.get('matchTime') or '').strip()

        bundle['battle_history'] = self._safe_detail_request(
            'footballMatchTeamBattleHistory',
            {'matchId': match_id, 'limit': 10, 'isSameHostAway': 0, 'isSameLeague': 0}
        )
        bundle['odds_euro'] = self._safe_detail_request('footballMatchOddsEuro', {'matchId': match_id})
        bundle['odds_asia'] = self._safe_detail_request('footballMatchOddsAsia', {'matchId': match_id})
        bundle['odds_totals'] = self._safe_detail_request('footballMatchOddsTotals', {'matchId': match_id})
        bundle['team_table'] = self._safe_detail_request('footballMatchTeamTable', {'matchId': match_id})
        if team1_id:
            bundle['recent_form_team1'] = self._safe_detail_request(
                'footballMatchTeamRecentZhanJi',
                {'teamId': team1_id, 'limit': 10, 'hostOrAway': '', 'leagueId': '', 'matchTime': match_timestamp}
            )
        if team2_id:
            bundle['recent_form_team2'] = self._safe_detail_request(
                'footballMatchTeamRecentZhanJi',
                {'teamId': team2_id, 'limit': 10, 'hostOrAway': '', 'leagueId': '', 'matchTime': match_timestamp}
            )
        bundle['recent_matches'] = self._safe_detail_request('footballMatchTeamRecentMatches', {'matchId': match_id})
        bundle['injury'] = self._safe_detail_request('footballMatchTeamInjury', {'matchId': match_id})
        bundle['intelligence'] = self._safe_detail_request('FootballMatchIntelligence', {'matchId': match_id, 't': int(datetime.utcnow().timestamp() * 1000)}, host='alpha')
        bundle['odds_snapshots'] = football_utils.extract_odds_snapshots(
            bundle.get('odds_euro') or [],
            bundle.get('odds_asia') or [],
            bundle.get('odds_totals') or []
        )
        return bundle

    def _safe_detail_request(self, cat1: str, params: dict, host: str = 'mix'):
        try:
            return self._request_detail_payload(cat1, params, host=host)
        except Exception:
            return {} if cat1 not in {'footballMatchTeamBattleHistory', 'footballMatchTeamRecentZhanJi', 'footballMatchOddsEuro', 'footballMatchOddsAsia', 'footballMatchOddsTotals'} else []

    def _parse_date(self, value: str):
        try:
            return datetime.strptime(str(value or '').strip()[:10], '%Y-%m-%d')
        except ValueError:
            return None

    def get_or_fetch_match_detail_bundle(self, db, match: dict, force_refresh: bool = False) -> dict:
        event_key = match['event_key']
        cached = db.get_lottery_event_details(self.lottery_type, event_key, source_provider='sina')
        if not force_refresh and self._is_detail_cache_fresh(cached):
            return {key: value.get('payload') or {} for key, value in cached.items()}

        try:
            bundle = self.fetch_match_detail_bundle(match)
        except Exception:
            return {key: value.get('payload') or {} for key, value in cached.items()}

        detail_records = [
            {
                'lottery_type': self.lottery_type,
                'event_key': event_key,
                'detail_type': key,
                'source_provider': 'sina',
                'payload': value
            }
            for key, value in bundle.items()
        ]
        db.upsert_lottery_event_details(detail_records)
        return bundle

    def enrich_matches_for_prediction(self, db, matches: list[dict]) -> list[dict]:
        enriched = []
        for match in matches:
            detail_bundle = self.get_or_fetch_match_detail_bundle(db, match)
            detail_payload = detail_bundle.get('detail') or {}
            team1_id = match.get('team1_id') or str(detail_payload.get('team1Id') or '').strip()
            team2_id = match.get('team2_id') or str(detail_payload.get('team2Id') or '').strip()
            enriched_match = {
                **match,
                'team1_id': team1_id,
                'team2_id': team2_id,
                'detail_bundle': detail_bundle
            }
            enriched.append({
                **enriched_match,
                'detail_summary': self._build_match_detail_summary(enriched_match, detail_bundle)
            })
        return enriched

    def build_overview(self, db, limit: int = 20) -> dict:
        payload = self.sync_matches(db, is_prized='')
        return self._build_overview_from_matches(
            payload['matches'],
            batch_key=payload.get('batch_key'),
            limit=limit
        )

    def build_overview_with_fallback(self, db, limit: int = 20) -> dict:
        normalized_limit = max(5, min(limit, 50))
        try:
            return self.build_overview(db, limit=normalized_limit)
        except Exception as exc:
            return self._build_cached_overview(db, normalized_limit, str(exc))

    def _build_cached_overview(self, db, limit: int, error_message: str) -> dict:
        cached_events = db.get_recent_lottery_events(self.lottery_type, limit=limit)
        if not cached_events:
            return {
                'lottery_type': self.lottery_type,
                'batch_key': None,
                'match_count': 0,
                'open_match_count': 0,
                'sale_open_match_count': 0,
                'awaiting_result_match_count': 0,
                'settled_match_count': 0,
                'next_match_time': None,
                'next_match_name': None,
                'recent_events': [],
                'warning': f'新浪接口不可用，且本地暂无竞彩足球缓存：{error_message}'
            }

        return self._build_overview_from_events(
            cached_events,
            batch_key=None,
            limit=limit,
            warning=f'新浪接口不可用，已回退本地缓存：{error_message}'
        )

    def sync_batch_overview(
        self,
        db,
        date: str = '',
        run_key: str | None = None,
        limit: int = 20,
        prefer_is_prized: str | None = None
    ) -> dict:
        batch_date = str(run_key or date or '').strip()
        payload, used_is_prized = self._sync_matches_best_effort(db, batch_date, prefer_is_prized=prefer_is_prized)
        overview = self._build_overview_from_matches(
            payload['matches'],
            batch_key=payload.get('batch_key') or batch_date or None,
            limit=max(5, min(limit, 50))
        )
        return {
            'run_key': batch_date or (payload.get('batch_key') or ''),
            'used_is_prized': used_is_prized,
            'overview': overview
        }

    def replay_batch(self, db, predictor_id: int, date: str, limit: int = 20) -> dict:
        batch_date = str(date or '').strip()
        if not batch_date:
            raise ValueError('回放日期不能为空')

        payload, used_is_prized = self._sync_matches_best_effort(db, batch_date)
        overview = self._build_overview_from_matches(
            payload['matches'],
            batch_key=payload.get('batch_key') or batch_date,
            limit=max(5, min(limit, 50))
        )
        runs = db.get_recent_prediction_runs(predictor_id, lottery_type=self.lottery_type, limit=100)
        target_run = next((item for item in runs if item.get('run_key') == batch_date), None)
        return {
            'predictor_id': predictor_id,
            'run_key': batch_date,
            'used_is_prized': used_is_prized,
            'overview': overview,
            'run': self.build_run_view_model(db, target_run) if target_run else None,
            'message': f'已刷新 {batch_date} 批次赛程，共 {overview.get("match_count") or 0} 场',
            'warning': payload.get('warning')
        }

    def settle_predictor_runs(self, db, predictor_id: int, run_key: str | None = None) -> dict:
        runs = db.get_recent_prediction_runs(predictor_id, lottery_type=self.lottery_type, limit=100)
        pending_runs = [item for item in runs if item.get('status') == 'pending']
        target_run_key = str(run_key or '').strip()
        if target_run_key:
            target_runs = [item for item in runs if item.get('run_key') == target_run_key]
        else:
            target_runs = pending_runs

        if not target_runs:
            message = f'未找到批次 {target_run_key} 的待结算记录' if target_run_key else '当前没有待结算竞彩足球批次'
            return {
                'predictor_id': predictor_id,
                'run_keys': [],
                'settled_items_count': 0,
                'expired_items_count': 0,
                'settled_runs_count': 0,
                'pending_runs_count': len(pending_runs),
                'runs': [],
                'message': message
            }

        processed_runs = []
        settled_items_count = 0
        settled_runs_count = 0
        expired_items_count = 0
        for run in target_runs:
            payload, used_is_prized = self._sync_matches_best_effort(db, run.get('run_key') or '')
            result = self._settle_run_with_payload(db, run, payload)
            expired_result = self._expire_run_overdue_items(db, result.get('run') or run, payload)
            settled_items_count += result['settled_item_count']
            settled_runs_count += 1 if result['settled_item_count'] > 0 else 0
            expired_items_count += expired_result['expired_item_count']
            processed_runs.append({
                'run_key': run.get('run_key'),
                'used_is_prized': used_is_prized,
                'settled_item_count': result['settled_item_count'],
                'expired_item_count': expired_result['expired_item_count'],
                'status': (expired_result.get('run') or result.get('run') or {}).get('status') or run.get('status'),
                'run': self.build_run_view_model(db, expired_result.get('run') or result.get('run') or run)
            })

        pending_after = [
            item for item in db.get_recent_prediction_runs(predictor_id, lottery_type=self.lottery_type, limit=100)
            if item.get('status') == 'pending'
        ]
        if settled_items_count or expired_items_count:
            message = f'已结算 {settled_items_count} 场预测，自动关闭 {expired_items_count} 场超时预测，涉及 {settled_runs_count} 个批次'
        else:
            message = '当前没有新增可结算赛果'
        return {
            'predictor_id': predictor_id,
            'run_keys': [item.get('run_key') for item in target_runs if item.get('run_key')],
            'settled_items_count': settled_items_count,
            'expired_items_count': expired_items_count,
            'settled_runs_count': settled_runs_count,
            'pending_runs_count': len(pending_after),
            'runs': processed_runs,
            'message': message
        }

    def current_sale_date(self) -> str:
        return get_current_beijing_time().strftime('%Y-%m-%d')

    def get_scheduler_plan(self, db, now_beijing: datetime | None = None) -> dict:
        now = now_beijing or get_current_beijing_time()
        pending_runs = db.get_pending_prediction_runs(self.lottery_type)
        if pending_runs:
            return {
                'mode': 'settlement',
                'interval_seconds': config.JINGCAI_SETTLEMENT_INTERVAL,
                'reason': f'存在 {len(pending_runs)} 个未结算竞彩足球批次'
            }

        recent_events = db.get_recent_lottery_events(self.lottery_type, limit=200)
        open_events = []
        for event in recent_events:
            meta_payload = event.get('meta_payload') or {}
            if meta_payload.get('settled') or meta_payload.get('expired'):
                continue
            event_time = parse_beijing_time(event.get('event_time') or '')
            if event_time is None:
                continue
            open_events.append((event_time, event))

        if open_events:
            nearest_time, nearest_event = min(open_events, key=lambda item: item[0])
            seconds_until_match = max(0, int((nearest_time - now).total_seconds()))
            if seconds_until_match <= 0:
                return {
                    'mode': 'settlement',
                    'interval_seconds': config.JINGCAI_SETTLEMENT_INTERVAL,
                    'reason': f'存在已开赛未结算赛事：{nearest_event.get("event_name") or nearest_event.get("event_key")}'
                }
            if seconds_until_match <= config.JINGCAI_NEAR_MATCH_LOOKAHEAD_HOURS * 3600:
                return {
                    'mode': 'near_match',
                    'interval_seconds': config.JINGCAI_NEAR_MATCH_INTERVAL,
                    'reason': f'最近赛事 {seconds_until_match // 60} 分钟后开赛'
                }
            if seconds_until_match <= config.JINGCAI_PREMATCH_LOOKAHEAD_HOURS * 3600:
                return {
                    'mode': 'prematch',
                    'interval_seconds': config.JINGCAI_PREMATCH_INTERVAL,
                    'reason': f'最近赛事 {seconds_until_match // 3600} 小时内开赛'
                }
            return {
                'mode': 'discovery',
                'interval_seconds': config.JINGCAI_DISCOVERY_INTERVAL,
                'reason': f'已有待赛批次，最近赛事时间 {nearest_time.strftime("%Y-%m-%d %H:%M:%S")}'
            }

        if config.JINGCAI_DAYTIME_IDLE_START_HOUR <= now.hour < config.JINGCAI_DAYTIME_IDLE_END_HOUR:
            return {
                'mode': 'daytime_idle',
                'interval_seconds': config.JINGCAI_DAYTIME_IDLE_INTERVAL,
                'reason': '北京时间白天且本地无待赛竞彩足球批次'
            }

        return {
            'mode': 'idle',
            'interval_seconds': config.JINGCAI_IDLE_INTERVAL,
            'reason': '当前无待赛竞彩足球批次，低频探测新批次'
        }

    def build_predictor_dashboard(self, db, predictor: dict) -> dict:
        overview = self.build_overview_with_fallback(db, limit=20)
        stats = self.build_predictor_stats(db, predictor['id'], predictor.get('primary_metric', 'spf'))
        recent_items = self.get_recent_prediction_items(db, predictor['id'], limit=100)
        recent_runs = db.get_recent_prediction_runs(predictor['id'], lottery_type=self.lottery_type, limit=20)
        current_run = next((item for item in recent_runs if item['status'] == 'pending'), None) or (recent_runs[0] if recent_runs else None)
        current_prediction = self.build_run_view_model(db, current_run) if current_run else None
        latest_prediction = recent_items[0] if recent_items else None

        return {
            'predictor': predictor,
            'stats': stats,
            'current_prediction': current_prediction,
            'latest_prediction': latest_prediction,
            'recent_predictions': recent_items,
            'recent_events': overview.get('recent_events') or [],
            'overview': overview
        }

    def build_run_view_model(self, db, run: dict | None) -> dict | None:
        return self._build_run_view_model(db, run)

    def generate_prediction(self, db, predictor: dict, auto_mode: bool = False, batch_payload: dict | None = None) -> dict:
        batch_payload = batch_payload or self.sync_matches(db, is_prized='')
        run_key = batch_payload.get('batch_key') or datetime.now().strftime('%Y-%m-%d')
        existing_run = db.get_prediction_run_by_key(predictor['id'], run_key)

        upcoming_matches = [item for item in batch_payload['matches'] if football_utils.is_match_sale_open(item)]
        if not upcoming_matches:
            raise ValueError('当前没有处于已开售状态的竞彩足球比赛')

        retry_matches = self._select_retry_target_matches(
            db=db,
            existing_run=existing_run,
            upcoming_matches=upcoming_matches,
            auto_mode=auto_mode
        )
        if existing_run and existing_run.get('status') == 'settled':
            return existing_run
        if existing_run and not retry_matches:
            return existing_run

        target_matches = retry_matches or upcoming_matches

        history_matches = self._fetch_recent_history(
            db,
            batch_payload.get('dates') or [],
            run_key,
            predictor.get('history_window') or 40
        )
        enriched_matches = self.enrich_matches_for_prediction(db, target_matches)
        user_algorithm_execution_log = None
        if uses_ai_engine(predictor):
            predictor_client = AIPredictor(
                api_key=predictor['api_key'],
                api_url=predictor['api_url'],
                model_name=predictor['model_name'],
                api_mode=predictor.get('api_mode', 'auto'),
                temperature=predictor['temperature']
            )

            batch_results = self._predict_in_batches(
                predictor_client=predictor_client,
                predictor=predictor,
                run_key=run_key,
                matches=enriched_matches,
                history_matches=history_matches,
                data_injection_mode=predictor.get('data_injection_mode') or 'summary'
            )
        else:
            items_payload, raw_response, prompt_snapshot, user_algorithm_execution_log = self._predict_jingcai_with_machine_strategy(
                db=db,
                run_key=run_key,
                matches=enriched_matches,
                predictor=predictor
            )
            batch_results = [{
                'batch_label': 'machine.1',
                'matches': enriched_matches,
                'item_order_offset': 0,
                'items_payload': items_payload,
                'raw_response': raw_response,
                'prompt_snapshot': prompt_snapshot,
                'error_message': None
            }]
        raw_response = self._compose_batch_trace(batch_results, field_name='raw_response')
        prompt_snapshot = self._compose_batch_trace(batch_results, field_name='prompt_snapshot')
        algorithm_snapshot = self._build_algorithm_snapshot(predictor)
        run_payload = {
            'predictor_id': predictor['id'],
            'lottery_type': self.lottery_type,
            'run_key': run_key,
            'title': f'{run_key} 竞彩足球批次预测',
            'requested_targets': predictor.get('prediction_targets') or [],
            'status': 'pending',
            'total_items': existing_run.get('total_items') if existing_run else len(upcoming_matches),
            'settled_items': existing_run.get('settled_items', 0) if existing_run else 0,
            'hit_items': existing_run.get('hit_items', 0) if existing_run else 0,
            'confidence': self._average_confidence([
                item
                for result in batch_results
                for item in (result.get('items_payload') or [])
                if isinstance(item, dict)
            ]),
            'reasoning_summary': '',
            'raw_response': self._merge_trace_payload(existing_run.get('raw_response') if existing_run else '', raw_response),
            'prompt_snapshot': self._merge_trace_payload(existing_run.get('prompt_snapshot') if existing_run else '', prompt_snapshot),
            'error_message': None,
            'settled_at': None,
            'algorithm_key': algorithm_snapshot.get('algorithm_key') or '',
            'algorithm_version': algorithm_snapshot.get('algorithm_version'),
            'algorithm_snapshot': algorithm_snapshot,
            'execution_log': {
                'engine_type': predictor.get('engine_type') or 'ai',
                'batch_count': len(batch_results),
                'match_count': len(enriched_matches),
                'retry_match_count': len(retry_matches or []),
                'auto_mode': bool(auto_mode),
                'user_algorithm_execution_log': user_algorithm_execution_log or {}
            }
        }
        run_id = db.upsert_prediction_run(run_payload)

        normalized_items = []
        for result in batch_results:
            if result.get('items_payload') is not None:
                normalized_items.extend(
                    self._normalize_prediction_items(
                        run_id=run_id,
                        predictor=predictor,
                        run_key=run_key,
                        matches=result['matches'],
                        items_payload=result.get('items_payload') or [],
                        raw_response=result.get('raw_response') or '',
                        item_order_offset=result.get('item_order_offset', 0)
                    )
                )
                continue

            normalized_items.extend(
                self._build_failed_prediction_items(
                    run_id=run_id,
                    predictor=predictor,
                    run_key=run_key,
                    matches=result['matches'],
                    raw_response=result.get('raw_response') or '',
                    error_message=result.get('error_message') or 'AI 子批次预测失败',
                    batch_label=result.get('batch_label') or '',
                    item_order_offset=result.get('item_order_offset', 0)
                )
            )

        if normalized_items:
            db.upsert_prediction_items(normalized_items)

        all_items = db.get_prediction_run_items(run_id)
        pending_items = [item for item in all_items if item.get('status') == 'pending']
        failed_items = [item for item in all_items if item.get('status') == 'failed']
        failed_batches = self._build_failed_batch_summaries(failed_items)
        error_message = self._build_prediction_error_summary(failed_batches, len(failed_items), len(all_items))
        final_run_payload = {
            **run_payload,
            'status': 'pending' if pending_items else 'failed',
            'confidence': self._average_confidence([
                {'confidence': item.get('confidence')}
                for item in pending_items
            ]),
            'total_items': len(all_items) or run_payload.get('total_items', 0),
            'error_message': error_message
        }
        db.upsert_prediction_run(final_run_payload)
        if all_items:
            self._refresh_run_summary(db, run_id)

        if not pending_items:
            failure_message = error_message or (
                'AI 未返回任何有效预测项' if uses_ai_engine(predictor) else '机器算法未生成任何有效预测项'
            )
            failure = AIPredictionError(failure_message, category='parse')
            if auto_mode and self.prediction_guard and uses_ai_engine(predictor):
                self.prediction_guard.record_ai_failure(
                    predictor['id'],
                    failure,
                    failure_key=f'{self.lottery_type}:{run_key}'
                )
            return db.get_prediction_run(run_id) or final_run_payload

        if self.prediction_guard and uses_ai_engine(predictor):
            self.prediction_guard.record_success(predictor['id'])
        saved = db.get_prediction_run(run_id) or final_run_payload
        if self.notification_service and saved.get('status') == 'pending' and not existing_run:
            self.notification_service.notify_prediction_created(
                predictor=predictor,
                prediction=saved,
                lottery_type=self.lottery_type,
                detail_builder=lambda run: self.build_run_view_model(db, run)
            )
        return saved

    def settle_pending_predictions(self, db) -> list[dict]:
        settled_items: list[dict] = []
        pending_runs = db.get_pending_prediction_runs(self.lottery_type)
        for run in pending_runs:
            batch_key = run.get('run_key')
            if not batch_key:
                continue

            try:
                payload, _ = self._sync_matches_best_effort(db, batch_key)
            except Exception:
                continue

            result = self._settle_run_with_payload(db, run, payload)
            self._expire_run_overdue_items(db, result.get('run') or run, payload)
            if result['settled_item_count']:
                settled_items.extend(result['items'])

        return [item for item in settled_items if item.get('status') == 'settled']

    def run_auto_cycle(self, db) -> dict:
        now = get_current_beijing_time()
        if self._next_auto_run_at and now < self._next_auto_run_at:
            return {
                'settled_count': 0,
                'predictions': []
            }

        settled_items = self.settle_pending_predictions(db)
        predictors = db.get_enabled_predictors(
            lottery_type=self.lottery_type,
            include_secret=True,
            exclude_auto_paused=True
        )
        pending_runs = db.get_pending_prediction_runs(self.lottery_type)

        if not predictors:
            interval = config.JINGCAI_IDLE_POLL_INTERVAL
            self._set_next_auto_run(now, interval, 'no enabled predictors')
            return {
                'settled_count': len(settled_items),
                'predictions': []
            }

        try:
            batch_payload = self.sync_matches(db, is_prized='')
            open_matches = [item for item in batch_payload['matches'] if football_utils.is_match_sale_open(item)]
            next_match_time = open_matches[0].get('match_time') if open_matches else None
        except Exception as exc:
            interval = config.JINGCAI_SETTLEMENT_POLL_INTERVAL if pending_runs else config.JINGCAI_IDLE_POLL_INTERVAL
            self._set_next_auto_run(now, interval, f'overview unavailable: {exc}')
            return {
                'settled_count': len(settled_items),
                'predictions': []
            }

        prediction_results = []
        batch_key = batch_payload.get('batch_key') or ''
        if open_matches and batch_key:
            for predictor in predictors:
                existing_run = db.get_prediction_run_by_key(predictor['id'], batch_key)
                if existing_run and existing_run.get('status') == 'settled':
                    prediction_results.append({
                        'predictor_id': predictor['id'],
                        'lottery_type': self.lottery_type,
                        'issue_no': existing_run.get('run_key'),
                        'status': existing_run.get('status')
                    })
                    continue
                if existing_run and existing_run.get('status') in {'pending', 'failed'} and not self._run_has_retryable_failed_items(
                    db,
                    existing_run,
                    open_matches,
                    auto_mode=True
                ):
                    prediction_results.append({
                        'predictor_id': predictor['id'],
                        'lottery_type': self.lottery_type,
                        'issue_no': existing_run.get('run_key'),
                        'status': existing_run.get('status')
                    })
                    continue

                try:
                    result = self.generate_prediction(db, predictor, auto_mode=True, batch_payload=batch_payload)
                    prediction_results.append({
                        'predictor_id': predictor['id'],
                        'lottery_type': self.lottery_type,
                        'issue_no': result.get('run_key'),
                        'status': result.get('status', 'pending')
                    })
                except Exception as exc:
                    prediction_results.append({
                        'predictor_id': predictor['id'],
                        'lottery_type': self.lottery_type,
                        'status': 'failed',
                        'error': str(exc)
                    })

        interval = self._compute_next_poll_seconds(
            next_match_time=next_match_time,
            has_open_matches=bool(open_matches),
            has_pending_runs=bool(db.get_pending_prediction_runs(self.lottery_type))
        )
        self._set_next_auto_run(now, interval, self._describe_poll_reason(bool(open_matches), next_match_time, bool(pending_runs)))

        return {
            'settled_count': len(settled_items),
            'predictions': prediction_results
        }

    def get_recent_prediction_items(self, db, predictor_id: int, limit: int | None = 100, offset: int = 0) -> list[dict]:
        items = db.get_recent_prediction_items(predictor_id, lottery_type=self.lottery_type, limit=limit, offset=offset)
        items = self._decorate_prediction_items(db, items)
        for item in items:
            item['score_percentage'] = self._compute_score_percentage(item)
        return items

    def build_predictor_stats(self, db, predictor_id: int, primary_metric: str) -> dict:
        items = self.get_recent_prediction_items(db, predictor_id, limit=1000)
        settled_items = [item for item in items if item['status'] == 'settled']
        metrics = {}
        metric_streaks = {}
        metric_keys = ['spf', 'rqspf']
        for metric_key in metric_keys:
            metrics[metric_key] = {
                'label': football_utils.TARGET_LABELS.get(metric_key, metric_key),
                'recent_20': self._build_metric_stats(settled_items[:20], metric_key),
                'recent_100': self._build_metric_stats(settled_items[:100], metric_key),
                'overall': self._build_metric_stats(settled_items, metric_key)
            }
            metric_streaks[metric_key] = self._build_streak_stats(settled_items, metric_key)

        primary = football_utils.normalize_primary_metric(primary_metric)
        latest_settled = settled_items[0] if settled_items else None
        return {
            'total_predictions': len(items),
            'settled_predictions': len(settled_items),
            'pending_predictions': len([item for item in items if item['status'] == 'pending']),
            'failed_predictions': len([item for item in items if item['status'] == 'failed']),
            'expired_predictions': len([item for item in items if item['status'] == 'expired']),
            'latest_settled_issue': latest_settled['issue_no'] if latest_settled else None,
            'primary_metric': primary,
            'primary_metric_label': football_utils.TARGET_LABELS.get(primary, primary),
            'metrics': metrics,
            'metric_streaks': metric_streaks,
            'streaks': metric_streaks.get(primary, {})
        }

    def build_external_prompt_template(self, predictor_payload: dict) -> str:
        history_window = predictor_payload.get('history_window') or 40
        return f"""你是一个资深提示词工程师。请帮我为“AITradingSimulator”的竞彩足球预测功能编写一版可直接使用的自定义提示词。\n\n当前配置：\n- 彩种：竞彩足球\n- 预测玩法：{', '.join(football_utils.TARGET_LABELS.get(item, item) for item in predictor_payload.get('prediction_targets') or [])}\n- 主玩法：{football_utils.TARGET_LABELS.get(predictor_payload.get('primary_metric') or 'spf', '胜平负')}\n- 历史窗口：最近 {history_window} 场已结束比赛\n\n平台会提供的数据包括：\n1. 待售比赛列表与 SPF / RQSPF 赔率\n2. 每场比赛详情、积分排名、历史交锋、双方近期战绩、伤停信息\n3. 近期已结束比赛样本\n\n要求：\n1. 只能预测当前待售比赛列表中的场次。\n2. 重点参考联赛、对阵、赔率、让球、历史交锋、近期战绩、积分排名、伤停信息。\n3. 输出严格 JSON，不要 Markdown，不要解释性前缀。\n4. JSON 字段只包含：batch_key、predictions。\n5. predictions 中每项字段只包含：event_key、match_no、predicted_spf、predicted_rqspf、confidence、reasoning_summary。\n6. predicted_spf / predicted_rqspf 只能输出“胜 / 平 / 负”或 null。\n7. confidence 取值 0-1。\n8. reasoning_summary 要简短，不要空泛。\n\n如果我补充“偏保守”“更重视赔率”“重点看让球”这类自然语言需求，请自动把它改写成专业、可执行的竞彩足球提示词。"""

    def _predict_jingcai_with_machine_strategy(
        self,
        db,
        run_key: str,
        matches: list[dict],
        predictor: dict
    ) -> tuple[list[dict], str, str, dict | None]:
        algorithm_key = str(predictor.get('algorithm_key') or '').strip()
        if not is_user_algorithm_key(algorithm_key):
            items_payload, raw_response, prompt_snapshot = machine_prediction.predict_jingcai(
                run_key,
                matches,
                predictor
            )
            return items_payload, raw_response, prompt_snapshot, None

        started_at = time.monotonic()
        fallback_strategy = str(predictor.get('user_algorithm_fallback_strategy') or 'fail').strip().lower()
        if fallback_strategy not in {'fail', 'builtin_baseline', 'skip'}:
            fallback_strategy = 'fail'

        try:
            items_payload, raw_response, prompt_snapshot = machine_prediction.predict_jingcai(
                run_key,
                matches,
                predictor
            )
            log_payload = self._record_user_algorithm_execution_log(
                db=db,
                predictor=predictor,
                run_key=run_key,
                matches=matches,
                items_payload=items_payload,
                started_at=started_at,
                status='succeeded',
                fallback_strategy=fallback_strategy,
                fallback_used=False,
                error_message='',
                debug={'algorithm_key': algorithm_key}
            )
            return items_payload, raw_response, prompt_snapshot, log_payload
        except Exception as exc:
            if fallback_strategy == 'builtin_baseline':
                try:
                    fallback_predictor = {
                        **predictor,
                        'algorithm_key': 'football_odds_baseline_v1',
                        'user_algorithm': None
                    }
                    items_payload, raw_response, prompt_snapshot = machine_prediction.predict_jingcai(
                        run_key,
                        matches,
                        fallback_predictor
                    )
                    log_payload = self._record_user_algorithm_execution_log(
                        db=db,
                        predictor=predictor,
                        run_key=run_key,
                        matches=matches,
                        items_payload=items_payload,
                        started_at=started_at,
                        status='fallback_succeeded',
                        fallback_strategy=fallback_strategy,
                        fallback_used=True,
                        error_message=str(exc),
                        debug={
                            'algorithm_key': algorithm_key,
                            'fallback_algorithm_key': 'football_odds_baseline_v1'
                        }
                    )
                    raw_response = self._append_user_algorithm_fallback_trace(raw_response, str(exc), fallback_strategy)
                    return items_payload, raw_response, prompt_snapshot, log_payload
                except Exception as fallback_exc:
                    log_payload = self._record_user_algorithm_execution_log(
                        db=db,
                        predictor=predictor,
                        run_key=run_key,
                        matches=matches,
                        items_payload=[],
                        started_at=started_at,
                        status='failed',
                        fallback_strategy=fallback_strategy,
                        fallback_used=True,
                        error_message=f'{exc}；降级失败：{fallback_exc}',
                        debug={'algorithm_key': algorithm_key}
                    )
                    exc.args = (*exc.args, f'降级失败：{fallback_exc}', f'执行日志ID：{log_payload.get("id")}')
                    raise

            if fallback_strategy == 'skip':
                raw_response = self._build_user_algorithm_skip_trace(str(exc), fallback_strategy)
                log_payload = self._record_user_algorithm_execution_log(
                    db=db,
                    predictor=predictor,
                    run_key=run_key,
                    matches=matches,
                    items_payload=[],
                    started_at=started_at,
                    status='skipped',
                    fallback_strategy=fallback_strategy,
                    fallback_used=True,
                    error_message=str(exc),
                    debug={'algorithm_key': algorithm_key}
                )
                return [], raw_response, '用户算法失败后按策略跳过本批次', log_payload

            log_payload = self._record_user_algorithm_execution_log(
                db=db,
                predictor=predictor,
                run_key=run_key,
                matches=matches,
                items_payload=[],
                started_at=started_at,
                status='failed',
                fallback_strategy=fallback_strategy,
                fallback_used=False,
                error_message=str(exc),
                debug={'algorithm_key': algorithm_key}
            )
            exc.args = (*exc.args, f'执行日志ID：{log_payload.get("id")}')
            raise

    def _record_user_algorithm_execution_log(
        self,
        db,
        predictor: dict,
        run_key: str,
        matches: list[dict],
        items_payload: list[dict],
        started_at: float,
        status: str,
        fallback_strategy: str,
        fallback_used: bool,
        error_message: str,
        debug: dict | None = None
    ) -> dict:
        user_algorithm = predictor.get('user_algorithm') or {}
        requested_targets = predictor.get('prediction_targets') or []
        prediction_count = self._count_prediction_payload_items(items_payload, requested_targets)
        match_count = len(matches or [])
        payload = {
            'user_id': predictor.get('user_id'),
            'algorithm_id': user_algorithm.get('id'),
            'algorithm_version': user_algorithm.get('active_version'),
            'predictor_id': predictor.get('id'),
            'run_key': run_key,
            'status': status,
            'match_count': match_count,
            'prediction_count': prediction_count,
            'skip_count': max(0, match_count - prediction_count),
            'duration_ms': int(max(0, time.monotonic() - started_at) * 1000),
            'fallback_strategy': fallback_strategy,
            'fallback_used': fallback_used,
            'error_message': str(error_message or '')[:1000],
            'debug': debug or {}
        }
        log_id = db.create_user_algorithm_execution_log(payload)
        return {**payload, 'id': log_id}

    def _count_prediction_payload_items(self, items_payload: list[dict], requested_targets: list[str]) -> int:
        targets = requested_targets or ['spf', 'rqspf']
        count = 0
        for item in items_payload or []:
            if not isinstance(item, dict):
                continue
            if any(football_utils.normalize_prediction_outcome(item.get(f'predicted_{target}')) for target in targets):
                count += 1
        return count

    def _append_user_algorithm_fallback_trace(self, raw_response: str, error_message: str, fallback_strategy: str) -> str:
        trace = {
            'user_algorithm_error': str(error_message or ''),
            'fallback_strategy': fallback_strategy,
            'fallback_used': True
        }
        return '\n\n'.join([part for part in [raw_response, f'用户算法降级记录：{trace}'] if part])

    def _build_user_algorithm_skip_trace(self, error_message: str, fallback_strategy: str) -> str:
        return str({
            'user_algorithm_error': str(error_message or ''),
            'fallback_strategy': fallback_strategy,
            'fallback_used': True,
            'result': '本批次按策略跳过，未生成预测'
        })

    def _select_retry_target_matches(self, db, existing_run: dict | None, upcoming_matches: list[dict], auto_mode: bool) -> list[dict]:
        if not existing_run:
            return []

        match_map = {
            str(match.get('event_key') or '').strip(): match
            for match in upcoming_matches
            if str(match.get('event_key') or '').strip()
        }
        if not match_map:
            return []

        retry_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        retry_matches: list[dict] = []
        existing_items = db.get_prediction_run_items(existing_run['id'])
        for item in sorted(existing_items, key=lambda current: (int(current.get('item_order') or 0), int(current.get('id') or 0))):
            if str(item.get('status') or '').strip() != 'failed':
                continue

            event_key = str(item.get('event_key') or '').strip()
            match = match_map.get(event_key)
            if not match:
                continue
            if not self._should_retry_failed_item(item, auto_mode):
                continue

            retry_matches.append({
                **match,
                'prediction_item_order': int(item.get('item_order') or 0),
                'prediction_next_retry_count': int(item.get('retry_count') or 0) + 1,
                'prediction_last_retry_at': retry_timestamp,
                'prediction_existing_retry_error': str(item.get('last_retry_error') or '').strip()
            })
        return retry_matches

    def _should_retry_failed_item(self, item: dict, auto_mode: bool) -> bool:
        if str(item.get('status') or '').strip() != 'failed':
            return False
        if not auto_mode:
            return True

        retry_count = int(item.get('retry_count') or 0)
        if retry_count >= FAILED_ITEM_RETRY_MAX_ATTEMPTS:
            return False

        last_attempt_at = (
            self._parse_utc_timestamp(item.get('last_retry_at'))
            or self._parse_utc_timestamp(item.get('updated_at'))
            or self._parse_utc_timestamp(item.get('created_at'))
        )
        if last_attempt_at is None:
            return True
        return (datetime.utcnow() - last_attempt_at).total_seconds() >= FAILED_ITEM_RETRY_COOLDOWN_SECONDS

    def _parse_utc_timestamp(self, value) -> datetime | None:
        text = str(value or '').strip()
        if not text:
            return None
        try:
            return datetime.strptime(text, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None

    def _merge_trace_payload(self, previous_text: str, new_text: str) -> str:
        previous = str(previous_text or '').strip()
        current = str(new_text or '').strip()
        if previous and current:
            return f'{previous}\n\n{current}'
        return current or previous

    def _run_has_retryable_failed_items(self, db, run: dict | None, upcoming_matches: list[dict], auto_mode: bool) -> bool:
        return bool(self._select_retry_target_matches(db, run, upcoming_matches, auto_mode=auto_mode))

    def _predict_in_batches(
        self,
        predictor_client: AIPredictor,
        predictor: dict,
        run_key: str,
        matches: list[dict],
        history_matches: list[dict],
        data_injection_mode: str
    ) -> list[dict]:
        initial_batches = self._split_prediction_batches(
            predictor=predictor,
            run_key=run_key,
            matches=matches,
            history_matches=history_matches,
            data_injection_mode=data_injection_mode
        )
        results: list[dict] = []
        total_batches = len(initial_batches)
        for batch_index, batch in enumerate(initial_batches, start=1):
            results.extend(
                self._predict_batch_with_retry(
                    predictor_client=predictor_client,
                    predictor=predictor,
                    run_key=run_key,
                    matches=batch['matches'],
                    history_matches=history_matches,
                    data_injection_mode=data_injection_mode,
                    item_order_offset=batch['item_order_offset'],
                    batch_label=f'{batch_index}/{total_batches}'
                )
            )
        return results

    def _split_prediction_batches(
        self,
        predictor: dict,
        run_key: str,
        matches: list[dict],
        history_matches: list[dict],
        data_injection_mode: str
    ) -> list[dict]:
        if not matches:
            return []

        batches: list[dict] = []
        current_matches: list[dict] = []
        current_offset = 0
        for match in matches:
            candidate_matches = current_matches + [match]
            candidate_prompt = self._build_prediction_prompt(
                predictor,
                run_key,
                candidate_matches,
                history_matches,
                data_injection_mode
            )
            should_split = (
                current_matches
                and (
                    len(candidate_matches) > PREDICTION_BATCH_MAX_MATCHES
                    or len(candidate_prompt) > PREDICTION_PROMPT_SOFT_LIMIT
                )
            )
            if should_split:
                batches.append({
                    'matches': current_matches,
                    'item_order_offset': current_offset
                })
                current_offset += len(current_matches)
                current_matches = [match]
                continue
            current_matches = candidate_matches

        if current_matches:
            batches.append({
                'matches': current_matches,
                'item_order_offset': current_offset
            })
        return batches

    def _predict_batch_with_retry(
        self,
        predictor_client: AIPredictor,
        predictor: dict,
        run_key: str,
        matches: list[dict],
        history_matches: list[dict],
        data_injection_mode: str,
        item_order_offset: int,
        batch_label: str
    ) -> list[dict]:
        prompt = self._build_prediction_prompt(
            predictor,
            run_key,
            matches,
            history_matches,
            data_injection_mode
        )
        raw_response = ''
        prompt_snapshot = prompt
        try:
            llm_result = predictor_client.run_json_task(
                prompt=prompt,
                system_prompt='你是中国竞彩足球预测助手。你必须只输出单个 JSON 对象。',
                max_output_tokens=PREDICTION_MAX_OUTPUT_TOKENS
            )
            raw_response = llm_result['raw_response']
            payload = llm_result['payload']
            items_payload = payload.get('predictions') if isinstance(payload, dict) else None
            if not isinstance(items_payload, list):
                raise AIPredictionError('AI 返回 JSON 缺少 predictions 数组', category='parse')

            missing_matches = self._find_missing_prediction_matches(matches, items_payload)
            if missing_matches and len(matches) > 1:
                return self._split_and_retry_prediction_batch(
                    predictor_client=predictor_client,
                    predictor=predictor,
                    run_key=run_key,
                    matches=matches,
                    history_matches=history_matches,
                    data_injection_mode=data_injection_mode,
                    item_order_offset=item_order_offset,
                    batch_label=batch_label
                )

            return [{
                'batch_label': batch_label,
                'matches': matches,
                'item_order_offset': item_order_offset,
                'items_payload': items_payload,
                'raw_response': raw_response,
                'prompt_snapshot': prompt_snapshot,
                'error_message': None
            }]
        except AIPredictionError as exc:
            raw_response = getattr(exc, 'raw_response', raw_response) or raw_response
            prompt_snapshot = getattr(exc, 'prompt_snapshot', prompt_snapshot) or prompt_snapshot
            if self._should_retry_prediction_batch(exc) and len(matches) > 1:
                return self._split_and_retry_prediction_batch(
                    predictor_client=predictor_client,
                    predictor=predictor,
                    run_key=run_key,
                    matches=matches,
                    history_matches=history_matches,
                    data_injection_mode=data_injection_mode,
                    item_order_offset=item_order_offset,
                    batch_label=batch_label
                )
            return [{
                'batch_label': batch_label,
                'matches': matches,
                'item_order_offset': item_order_offset,
                'items_payload': None,
                'raw_response': raw_response,
                'prompt_snapshot': prompt_snapshot,
                'error_message': str(exc)
            }]
        except Exception as exc:
            raw_response = getattr(exc, 'raw_response', raw_response) or raw_response
            prompt_snapshot = getattr(exc, 'prompt_snapshot', prompt_snapshot) or prompt_snapshot
            return [{
                'batch_label': batch_label,
                'matches': matches,
                'item_order_offset': item_order_offset,
                'items_payload': None,
                'raw_response': raw_response,
                'prompt_snapshot': prompt_snapshot,
                'error_message': str(exc)
            }]

    def _split_and_retry_prediction_batch(
        self,
        predictor_client: AIPredictor,
        predictor: dict,
        run_key: str,
        matches: list[dict],
        history_matches: list[dict],
        data_injection_mode: str,
        item_order_offset: int,
        batch_label: str
    ) -> list[dict]:
        midpoint = max(1, len(matches) // 2)
        left_matches = matches[:midpoint]
        right_matches = matches[midpoint:]
        if not left_matches or not right_matches:
            return [{
                'batch_label': batch_label,
                'matches': matches,
                'item_order_offset': item_order_offset,
                'items_payload': None,
                'raw_response': '',
                'prompt_snapshot': '',
                'error_message': 'AI 子批次拆分失败'
            }]

        results = self._predict_batch_with_retry(
            predictor_client=predictor_client,
            predictor=predictor,
            run_key=run_key,
            matches=left_matches,
            history_matches=history_matches,
            data_injection_mode=data_injection_mode,
            item_order_offset=item_order_offset,
            batch_label=f'{batch_label}.1'
        )
        results.extend(
            self._predict_batch_with_retry(
                predictor_client=predictor_client,
                predictor=predictor,
                run_key=run_key,
                matches=right_matches,
                history_matches=history_matches,
                data_injection_mode=data_injection_mode,
                item_order_offset=item_order_offset + len(left_matches),
                batch_label=f'{batch_label}.2'
            )
        )
        return results

    def _should_retry_prediction_batch(self, error: Exception) -> bool:
        category = str(getattr(error, 'category', '') or '').strip().lower()
        finish_reason = str(getattr(error, 'finish_reason', '') or '').strip().lower()
        message = str(error or '').lower()
        if category == 'parse':
            return True
        if finish_reason in {'length', 'incomplete'}:
            return True
        return any(
            keyword in message
            for keyword in (
                '无法从模型响应中解析 json',
                'json 缺少 predictions',
                '只输出单个 json 对象',
                '返回为空'
            )
        )

    def _find_missing_prediction_matches(self, matches: list[dict], items_payload: list[dict]) -> list[dict]:
        item_by_key = {}
        item_by_match_no = {}
        for item in items_payload:
            if not isinstance(item, dict):
                continue
            event_key = str(item.get('event_key') or '').strip()
            match_no = str(item.get('match_no') or '').strip()
            if event_key:
                item_by_key[event_key] = item
            if match_no:
                item_by_match_no[match_no] = item

        return [
            match
            for match in matches
            if not item_by_key.get(match.get('event_key') or '')
            and not item_by_match_no.get(match.get('match_no') or '')
        ]

    def _compose_batch_trace(self, batch_results: list[dict], field_name: str) -> str:
        blocks: list[str] = []
        for result in batch_results:
            content = str(result.get(field_name) or '').strip()
            if not content:
                continue
            match_labels = '、'.join(
                str(match.get('match_no') or match.get('event_key') or '').strip()
                for match in (result.get('matches') or [])[:5]
            )
            if len(result.get('matches') or []) > 5:
                match_labels = f'{match_labels}...'
            blocks.append(
                f"[子批次 {result.get('batch_label') or '--'} | 场次 {len(result.get('matches') or [])} | {match_labels}]\n{content}"
            )
        return '\n\n'.join(blocks)

    def _build_algorithm_snapshot(self, predictor: dict) -> dict:
        algorithm_key = str(predictor.get('algorithm_key') or '').strip()
        user_algorithm = predictor.get('user_algorithm') or {}
        if is_user_algorithm_key(algorithm_key) and user_algorithm:
            return {
                'algorithm_key': algorithm_key,
                'algorithm_source': 'user',
                'algorithm_id': user_algorithm.get('id'),
                'algorithm_version': user_algorithm.get('active_version'),
                'algorithm_name': user_algorithm.get('name') or '',
                'definition': user_algorithm.get('definition') or {}
            }
        return {
            'algorithm_key': algorithm_key,
            'algorithm_source': 'builtin' if algorithm_key else 'ai',
            'algorithm_id': None,
            'algorithm_version': None,
            'algorithm_name': predictor.get('algorithm_label') or predictor.get('model_name') or '',
            'definition': {}
        }

    def _build_prediction_error_summary(self, failed_batches: list[dict], failed_item_count: int, total_items: int) -> str | None:
        if not failed_batches:
            return None

        previews = []
        for batch in failed_batches[:3]:
            previews.append(
                f"子批次 {batch.get('batch_label') or '--'}（{batch.get('match_text') or '未知场次'}）：{batch.get('reason') or 'AI 子批次预测失败'}"
            )
        joined_preview = '；'.join(previews)
        prefix = 'AI 未返回任何有效预测项' if failed_item_count >= total_items else 'AI 未返回部分场次的有效预测'
        return f'{prefix}，失败 {failed_item_count}/{total_items} 场，涉及 {len(failed_batches)} 个子批次。{joined_preview}'

    def _fetch_recent_history(self, db, dates: list[str], batch_key: str, history_window: int) -> list[dict]:
        normalized_dates = [date for date in dates if str(date) < str(batch_key)]
        history: list[dict] = []
        for date in reversed(normalized_dates):
            if len(history) >= history_window:
                break
            try:
                payload = self.sync_matches(db, date=date, is_prized='1')
            except Exception:
                continue
            settled_matches = [item for item in payload['matches'] if item.get('settled')]
            history.extend(settled_matches)
        return history[:history_window]

    def _build_prediction_prompt(
        self,
        predictor: dict,
        run_key: str,
        upcoming_matches: list[dict],
        history_matches: list[dict],
        data_injection_mode: str
    ) -> str:
        requested_targets = predictor.get('prediction_targets') or []
        custom_prompt = (predictor.get('system_prompt') or '').strip()
        method_name = predictor.get('prediction_method') or '自定义策略'
        prompt_variables = self._build_prompt_variables(
            run_key=run_key,
            requested_targets=requested_targets,
            upcoming_matches=upcoming_matches,
            history_matches=history_matches,
            data_injection_mode=data_injection_mode
        )
        rendered_custom_prompt = self._render_prompt_template(custom_prompt, prompt_variables)
        placeholders_used = self._contains_placeholder(custom_prompt)
        target_labels = prompt_variables['prediction_targets']
        concise_reasoning_rule = ''
        if len(upcoming_matches) >= PREDICTION_CONCISE_REASONING_THRESHOLD:
            concise_reasoning_rule = '\n8. 当前子批次场次较多，reasoning_summary 必须压缩为一句短句，尽量控制在 30 个汉字内。'
        return f"""你正在为中国竞彩足球生成一批比赛预测。\n\n基础要求：\n1. 只能预测我提供的待售比赛。\n2. 当前批次：{run_key}。\n3. 预测玩法：{target_labels}。\n4. 不能编造比赛，也不能输出列表外的 event_key。\n5. 只输出一个 JSON 对象，不要 Markdown，不要额外解释。\n6. predicted_spf / predicted_rqspf 只能输出“胜”“平”“负”或 null。\n7. confidence 取值 0-1。{concise_reasoning_rule}\n\n方案信息：\n- 方案名称：{predictor.get('name', '')}\n- 预测方法：{method_name}\n- 历史窗口：最近 {len(history_matches)} 场已结束比赛\n\n用户自定义策略：\n{rendered_custom_prompt or '无，按赔率、让球、联赛、历史交锋、近期战绩、积分排名与伤停信息做稳健分析'}\n\n平台数据输入：\n{prompt_variables['default_data_block'] if not placeholders_used else '你在自定义提示词中已经使用了项目占位符，平台不再重复拼接整批比赛上下文。'}\n\n输出格式：\n{{\n  \"batch_key\": \"{run_key}\",\n  \"predictions\": [\n    {{\n      \"event_key\": \"示例event_key\",\n      \"match_no\": \"周日024\",\n      \"predicted_spf\": \"胜\",\n      \"predicted_rqspf\": \"平\",\n      \"confidence\": 0.68,\n      \"reasoning_summary\": \"一句简短依据\"\n    }}\n  ]\n}}\n\n现在开始，只输出 JSON。"""

    def _build_prompt_variables(
        self,
        run_key: str,
        requested_targets: list[str],
        upcoming_matches: list[dict],
        history_matches: list[dict],
        data_injection_mode: str
    ) -> dict[str, str]:
        target_labels = '、'.join(football_utils.TARGET_LABELS.get(item, item) for item in requested_targets) or '胜平负'
        match_batch_lines: list[str] = []
        detail_lines: list[str] = []
        odds_lines: list[str] = []
        injury_lines: list[str] = []
        intelligence_lines: list[str] = []

        for match in upcoming_matches:
            detail_bundle = match.get('detail_bundle') or {}
            detail = detail_bundle.get('detail') or {}
            battle_history = detail_bundle.get('battle_history') or []
            odds_euro = detail_bundle.get('odds_euro') or []
            odds_asia = detail_bundle.get('odds_asia') or []
            odds_totals = detail_bundle.get('odds_totals') or []
            odds_snapshots = detail_bundle.get('odds_snapshots') or football_utils.extract_odds_snapshots(
                odds_euro,
                odds_asia,
                odds_totals
            )
            team_table = detail_bundle.get('team_table') or {}
            recent_team1 = detail_bundle.get('recent_form_team1') or []
            recent_team2 = detail_bundle.get('recent_form_team2') or []
            injury = detail_bundle.get('injury') or {}
            intelligence = detail_bundle.get('intelligence') or {}
            recent_matches = detail_bundle.get('recent_matches') or {}

            spf_text = self._format_odds_text(match.get('spf_odds') or {})
            rqspf = match.get('rqspf') or {}
            rq_text = self._format_odds_text(rqspf.get('odds') or {})
            match_batch_lines.append(
                f"- event_key={match['event_key']} | {match['match_no']} | {match['league']} | "
                f"{match['home_team']} vs {match['away_team']} | 开赛={match['match_time']} | "
                f"SPF={spf_text} | RQSPF={rqspf.get('handicap_text') or '--'} [{rq_text}]"
            )
            detail_lines.append(
                f"- {match['event_key']} / {match['match_no']} / 比赛详情={self._build_detail_line(detail)} / "
                f"积分排名={self._build_table_line(team_table)} / "
                f"历史交锋={self._build_battle_history_line(battle_history, match['home_team'], match['away_team'])} / "
                f"近期战绩={self._build_recent_form_line(recent_team1, match['home_team'], match.get('team1_id'))}；"
                f"{self._build_recent_form_line(recent_team2, match['away_team'], match.get('team2_id'))} / "
                f"后续赛程={self._build_recent_matches_line(recent_matches)}"
            )
            odds_lines.append(
                f"- {match['event_key']} / {match['match_no']} / 市场赔率={self._build_market_odds_line(odds_euro, odds_asia, odds_totals)} / "
                f"快照摘要={self._build_odds_snapshot_line(odds_snapshots)}"
            )
            injury_lines.append(
                f"- {match['event_key']} / {match['match_no']} / {self._build_injury_line(injury)}"
            )
            intelligence_lines.append(
                f"- {match['event_key']} / {match['match_no']} / {self._build_intelligence_line(intelligence)}"
            )

        recent_results_lines = [
            f"- {match['match_no']} | {match['league']} | {match['home_team']} {match.get('score_text') or '--'} {match['away_team']} | "
            f"SPF={match.get('actual_spf') or '--'} | RQSPF={match.get('actual_rqspf') or '--'}"
            for match in history_matches[:60]
        ]

        current_time_beijing = get_current_beijing_time().strftime('%Y-%m-%d %H:%M:%S')
        prompt_variables = {
            'match_batch_summary': '\n'.join(match_batch_lines) or '- 暂无待预测比赛',
            'match_detail_summary': '\n'.join(detail_lines) or '- 暂无比赛详情摘要',
            'market_odds_summary': '\n'.join(odds_lines) or '- 暂无市场赔率摘要',
            'recent_results_summary': '\n'.join(recent_results_lines) or '- 暂无可用历史比赛',
            'injury_summary': '\n'.join(injury_lines) or '- 暂无伤停摘要',
            'intelligence_summary': '\n'.join(intelligence_lines) or '- 暂无情报摘要',
            'current_time_beijing': current_time_beijing,
            'history_window': str(len(history_matches)),
            'prediction_targets': target_labels
        }
        prompt_variables['default_data_block'] = self._build_default_data_block(
            run_key=run_key,
            prompt_variables=prompt_variables,
            data_injection_mode=data_injection_mode
        )
        return prompt_variables

    def _build_default_data_block(self, run_key: str, prompt_variables: dict[str, str], data_injection_mode: str) -> str:
        is_raw_mode = str(data_injection_mode).strip().lower() == 'raw'
        sections = [
            f"- 当前北京时间：{prompt_variables['current_time_beijing']}",
            f"- 当前批次：{run_key}",
            f"- 预测玩法：{prompt_variables['prediction_targets']}",
            "- 待预测比赛：",
            prompt_variables['match_batch_summary'],
            "- 市场赔率摘要：",
            prompt_variables['market_odds_summary'],
            "- 比赛详情摘要：",
            prompt_variables['match_detail_summary'],
            "- 伤停摘要：",
            prompt_variables['injury_summary'],
            "- 情报摘要：",
            prompt_variables['intelligence_summary'],
            "- 近期已结束比赛：",
            prompt_variables['recent_results_summary']
        ]
        if is_raw_mode:
            sections.insert(3, '- 数据注入模式：原始模式（已裁剪为结构化摘要，避免上下文冗余）')
        else:
            sections.insert(3, '- 数据注入模式：摘要模式')
        return '\n'.join(sections)

    def _render_prompt_template(self, prompt: str, prompt_variables: dict[str, str]) -> str:
        if not prompt:
            return ''

        def replace(match: re.Match) -> str:
            key = str(match.group(1) or '').strip()
            return str(prompt_variables.get(key, match.group(0)))

        return re.sub(r'\{\{\s*([a-zA-Z0-9_]+)\s*\}\}', replace, prompt)

    def _contains_placeholder(self, prompt: str) -> bool:
        if not prompt:
            return False
        return re.search(r'\{\{\s*[a-zA-Z0-9_]+\s*\}\}', prompt) is not None

    def _normalize_match(self, item: dict, batch_key: str) -> dict | None:
        event_key = football_utils.resolve_event_key(item)
        if not event_key:
            return None

        score1 = football_utils.parse_int(item.get('score1'))
        score2 = football_utils.parse_int(item.get('score2'))
        rqspf = football_utils.parse_rqspf_odds(item.get('rqspf'))
        actual_spf = football_utils.derive_spf_result(score1, score2)
        actual_rqspf = football_utils.derive_rqspf_result(score1, score2, rqspf.get('handicap'))
        show_sell_status = football_utils.match_status_code(item)
        show_sell_status_label = football_utils.match_status_label(item)

        return {
            'lottery_type': self.lottery_type,
            'event_key': event_key,
            'source_match_id': str(item.get('matchId') or '').strip(),
            'source_ticai_id': str(item.get('tiCaiId') or '').strip(),
            'team1_id': str(item.get('team1Id') or '').strip(),
            'team2_id': str(item.get('team2Id') or '').strip(),
            'league_id': str(item.get('leagueId') or '').strip(),
            'match_timestamp': str(item.get('matchTime') or '').strip(),
            'batch_key': batch_key,
            'match_no': str(item.get('matchNo') or '').strip(),
            'match_no_value': str(item.get('matchNoValue') or '').strip(),
            'league': str(item.get('leagueOfficial') or item.get('league') or '').strip(),
            'home_team': str(item.get('team1') or '').strip(),
            'away_team': str(item.get('team2') or '').strip(),
            'event_name': football_utils.build_match_name(
                item.get('leagueOfficial') or item.get('league') or '',
                item.get('team1') or '',
                item.get('team2') or ''
            ),
            'match_time': str(item.get('matchTimeFormat') or '').strip(),
            'show_sell_status': show_sell_status,
            'show_sell_status_label': show_sell_status_label,
            'spf_sell_status': str(item.get('spfSellStatus') or '').strip(),
            'rqspf_sell_status': str(item.get('rqspfSellStatus') or '').strip(),
            'spf_odds': football_utils.parse_spf_odds(item.get('spf')),
            'rqspf': rqspf,
            'score1': score1,
            'score2': score2,
            'half_score1': football_utils.parse_int(item.get('halfScore1')),
            'half_score2': football_utils.parse_int(item.get('halfScore2')),
            'score_text': f'{score1}:{score2}' if score1 is not None and score2 is not None else '',
            'settled': football_utils.is_match_settled(item),
            'actual_spf': actual_spf,
            'actual_rqspf': actual_rqspf,
            'raw_item': item
        }

    def _build_event_record(self, match: dict, existing_meta: dict | None = None) -> dict:
        existing_meta = existing_meta or {}
        spf_sell_status = str(match.get('spf_sell_status') or '').strip()
        rqspf_sell_status = str(match.get('rqspf_sell_status') or '').strip()

        def merge_sell_status_snapshot(metric_key: str, current_status: str) -> str:
            existing_snapshot = str(existing_meta.get(f'{metric_key}_sell_status_snapshot') or '').strip()
            existing_status = str(existing_meta.get(f'{metric_key}_sell_status') or '').strip()
            candidates = [
                status
                for status in (existing_snapshot, existing_status, current_status)
                if status in {'1', '2'}
            ]
            if '2' in candidates:
                return '2'
            if '1' in candidates:
                return '1'
            return ''

        spf_sell_status_snapshot = merge_sell_status_snapshot('spf', spf_sell_status)
        rqspf_sell_status_snapshot = merge_sell_status_snapshot('rqspf', rqspf_sell_status)
        is_settled = bool(match.get('settled'))

        result_payload = {
            'score1': match.get('score1'),
            'score2': match.get('score2'),
            'half_score1': match.get('half_score1'),
            'half_score2': match.get('half_score2'),
            'actual_spf': match.get('actual_spf'),
            'actual_rqspf': match.get('actual_rqspf')
        }
        meta_payload = {
            'match_no': match.get('match_no'),
            'match_no_value': match.get('match_no_value'),
            'spf_sell_status': spf_sell_status,
            'spf_sell_status_snapshot': spf_sell_status_snapshot,
            'rqspf_sell_status': rqspf_sell_status,
            'rqspf_sell_status_snapshot': rqspf_sell_status_snapshot,
            'spf_odds': match.get('spf_odds'),
            'rqspf': match.get('rqspf'),
            'score_text': match.get('score_text'),
            'settled': match.get('settled'),
            'expired': False if is_settled else bool(existing_meta.get('expired')),
            'expired_at': None if is_settled else existing_meta.get('expired_at'),
            'expire_reason': None if is_settled else existing_meta.get('expire_reason')
        }
        return {
            'lottery_type': self.lottery_type,
            'event_key': match['event_key'],
            'batch_key': match.get('batch_key') or '',
            'event_date': str(match.get('match_time') or '').split(' ')[0] if match.get('match_time') else '',
            'event_time': match.get('match_time') or '',
            'event_name': match.get('event_name') or '',
            'league': match.get('league') or '',
            'home_team': match.get('home_team') or '',
            'away_team': match.get('away_team') or '',
            'status': match.get('show_sell_status') or '',
            'status_label': match.get('show_sell_status_label') or '',
            'source_provider': 'sina',
            'result_payload': football_utils.dump_json(result_payload),
            'meta_payload': football_utils.dump_json(meta_payload),
            'source_payload': football_utils.dump_json(match.get('raw_item') or {})
        }

    def _set_next_auto_run(self, now: datetime, interval_seconds: int, reason: str):
        safe_interval = max(60, int(interval_seconds))
        self._next_auto_run_at = now + timedelta(seconds=safe_interval)
        self._last_auto_reason = reason

    def _compute_next_poll_seconds(self, next_match_time: str | None, has_open_matches: bool, has_pending_runs: bool) -> int:
        if has_pending_runs and not has_open_matches:
            return config.JINGCAI_SETTLEMENT_POLL_INTERVAL
        if not has_open_matches:
            return config.JINGCAI_IDLE_POLL_INTERVAL

        kickoff_time = parse_beijing_time(next_match_time or '')
        if kickoff_time is None:
            return config.JINGCAI_ACTIVE_POLL_INTERVAL

        now = get_current_beijing_time()
        delta_seconds = int((kickoff_time - now).total_seconds())
        if delta_seconds <= config.JINGCAI_PREMATCH_WINDOW_MINUTES * 60:
            return config.JINGCAI_PREMATCH_POLL_INTERVAL
        return config.JINGCAI_ACTIVE_POLL_INTERVAL

    def _describe_poll_reason(self, has_open_matches: bool, next_match_time: str | None, has_pending_runs: bool) -> str:
        if has_pending_runs and not has_open_matches:
            return 'waiting settlement'
        if not has_open_matches:
            return 'idle no open matches'
        kickoff_time = parse_beijing_time(next_match_time or '')
        if kickoff_time is None:
            return 'active batch polling'
        now = get_current_beijing_time()
        delta_seconds = int((kickoff_time - now).total_seconds())
        if delta_seconds <= config.JINGCAI_PREMATCH_WINDOW_MINUTES * 60:
            return 'prematch fast polling'
        return 'active batch polling'

    def _is_detail_cache_fresh(self, cached: dict[str, dict]) -> bool:
        if not cached:
            return False

        now = datetime.utcnow()
        for item in cached.values():
            updated_at = str(item.get('updated_at') or '').strip()
            if not updated_at:
                return False
            try:
                updated_time = datetime.strptime(updated_at, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return False
            if now - updated_time > timedelta(seconds=DETAIL_CACHE_SECONDS):
                return False
        return True

    def _build_match_detail_summary(self, match: dict, detail_bundle: dict) -> str:
        detail = detail_bundle.get('detail') or {}
        battle_history = detail_bundle.get('battle_history') or []
        odds_euro = detail_bundle.get('odds_euro') or []
        odds_asia = detail_bundle.get('odds_asia') or []
        odds_totals = detail_bundle.get('odds_totals') or []
        odds_snapshots = detail_bundle.get('odds_snapshots') or football_utils.extract_odds_snapshots(odds_euro, odds_asia, odds_totals)
        team_table = detail_bundle.get('team_table') or {}
        recent_team1 = detail_bundle.get('recent_form_team1') or []
        recent_team2 = detail_bundle.get('recent_form_team2') or []
        injury = detail_bundle.get('injury') or {}
        intelligence = detail_bundle.get('intelligence') or {}
        recent_matches = detail_bundle.get('recent_matches') or {}

        lines = [
            f"- {match['event_key']} / {match['match_no']} / {match['home_team']} vs {match['away_team']}",
            f"  比赛详情：{self._build_detail_line(detail)}",
            f"  市场赔率：{self._build_market_odds_line(odds_euro, odds_asia, odds_totals)}",
            f"  快照摘要：{self._build_odds_snapshot_line(odds_snapshots)}",
            f"  积分排名：{self._build_table_line(team_table)}",
            f"  历史交锋：{self._build_battle_history_line(battle_history, match['home_team'], match['away_team'])}",
            f"  近期战绩：{self._build_recent_form_line(recent_team1, match['home_team'], match.get('team1_id'))}；{self._build_recent_form_line(recent_team2, match['away_team'], match.get('team2_id'))}",
            f"  伤停情况：{self._build_injury_line(injury)}",
            f"  情报摘要：{self._build_intelligence_line(intelligence)}",
            f"  后续赛程：{self._build_recent_matches_line(recent_matches)}"
        ]
        return '\n'.join(lines)

    def _build_match_detail_json(self, match: dict) -> dict:
        detail_bundle = match.get('detail_bundle') or {}
        odds_snapshots = detail_bundle.get('odds_snapshots') or football_utils.extract_odds_snapshots(
            detail_bundle.get('odds_euro') or [],
            detail_bundle.get('odds_asia') or [],
            detail_bundle.get('odds_totals') or []
        )
        return {
            'event_key': match.get('event_key'),
            'match_no': match.get('match_no'),
            'league': match.get('league'),
            'home_team': match.get('home_team'),
            'away_team': match.get('away_team'),
            'match_time': match.get('match_time'),
            'spf_odds': match.get('spf_odds'),
            'rqspf': match.get('rqspf'),
            'detail': detail_bundle.get('detail') or {},
            'battle_history': (detail_bundle.get('battle_history') or [])[:5],
            'odds_snapshots': odds_snapshots,
            'odds_euro': (detail_bundle.get('odds_euro') or [])[:6],
            'odds_asia': (detail_bundle.get('odds_asia') or [])[:6],
            'odds_totals': (detail_bundle.get('odds_totals') or [])[:6],
            'team_table': detail_bundle.get('team_table') or {},
            'recent_form_team1': (detail_bundle.get('recent_form_team1') or [])[:5],
            'recent_form_team2': (detail_bundle.get('recent_form_team2') or [])[:5],
            'injury': detail_bundle.get('injury') or {},
            'intelligence': detail_bundle.get('intelligence') or {}
        }

    def _build_detail_line(self, detail: dict) -> str:
        if not detail:
            return '暂无'
        pieces = [
            f"联赛={detail.get('league') or '--'}",
            f"轮次={detail.get('round') or '--'}",
            f"排名={detail.get('team1Position') or '--'} vs {detail.get('team2Position') or '--'}",
            f"场地={'中立场' if str(detail.get('isNeutral') or '0') == '1' else '非中立场'}"
        ]
        environment = str(detail.get('environment') or '').strip()
        if environment:
            pieces.append(f"环境={environment}")
        return '，'.join(pieces)

    def _build_table_line(self, team_table: dict) -> str:
        team1 = ((team_table.get('team1') or {}).get('items') or {})
        team2 = ((team_table.get('team2') or {}).get('items') or {})
        team1_all = (team1.get('all') or [{}])[0]
        team2_all = (team2.get('all') or [{}])[0]
        return (
            f"主队总排名{team1_all.get('position') or '--'} 积分{team1_all.get('points') or '--'} "
            f"主场{self._record_text((team1.get('home') or [{}])[0])}；"
            f"客队总排名{team2_all.get('position') or '--'} 积分{team2_all.get('points') or '--'} "
            f"客场{self._record_text((team2.get('away') or [{}])[0])}"
        )

    def _build_market_odds_line(self, odds_euro: list[dict], odds_asia: list[dict], odds_totals: list[dict]) -> str:
        euro_line = self._build_odds_euro_line(odds_euro)
        asia_line = self._build_odds_asia_line(odds_asia)
        totals_line = self._build_odds_totals_line(odds_totals)
        return f"{euro_line}；{asia_line}；{totals_line}"

    def _build_odds_snapshot_line(self, snapshots: dict) -> str:
        if not snapshots:
            return '暂无'

        euro = snapshots.get('euro') or {}
        asia = snapshots.get('asia') or {}
        totals = snapshots.get('totals') or {}

        euro_line = '欧赔暂无'
        if euro:
            euro_line = (
                f"欧赔 {euro.get('company') or '--'} 初赔"
                f"{self._format_snapshot_triplet(euro.get('initial') or {})} -> 即赔"
                f"{self._format_snapshot_triplet(euro.get('current') or {})}"
            )
            if euro.get('updated_at'):
                euro_line += f" @{euro['updated_at']}"

        asia_line = self._format_line_snapshot_text('亚盘', asia)
        totals_line = self._format_line_snapshot_text('大小球', totals)
        return f"{euro_line}；{asia_line}；{totals_line}"

    def _format_snapshot_triplet(self, payload: dict) -> str:
        return '/'.join(
            str(payload.get(key) if payload.get(key) is not None else '--')
            for key in ('win', 'draw', 'lose')
        )

    def _format_line_snapshot_text(self, label: str, payload: dict) -> str:
        if not payload:
            return f'{label}暂无'
        text = (
            f"{label} {payload.get('company') or '--'} 初盘{(payload.get('initial') or {}).get('line') or '--'} "
            f"({(payload.get('initial') or {}).get('home') or '--'}/{(payload.get('initial') or {}).get('away') or '--'}) -> 即盘"
            f"{(payload.get('current') or {}).get('line') or '--'} "
            f"({(payload.get('current') or {}).get('home') or '--'}/{(payload.get('current') or {}).get('away') or '--'})"
        )
        if payload.get('updated_at'):
            text += f" @{payload['updated_at']}"
        return text

    def _build_odds_euro_line(self, items: list[dict]) -> str:
        if not items:
            return '欧赔暂无'
        official = next((item for item in items if str(item.get('companyName') or '') == '竞彩官方'), items[0])
        return (
            f"欧赔 {official.get('companyName') or '--'} 初赔"
            f"{official.get('o1Ini') or '--'}/{official.get('o2Ini') or '--'}/{official.get('o3Ini') or '--'} -> 即赔"
            f"{official.get('o1New') or '--'}/{official.get('o2New') or '--'}/{official.get('o3New') or '--'}"
        )

    def _build_odds_asia_line(self, items: list[dict]) -> str:
        if not items:
            return '亚盘暂无'
        top = items[0]
        return (
            f"亚盘 {top.get('companyName') or '--'} 初盘{top.get('o3IniCn') or top.get('o3IniStr') or '--'} "
            f"({top.get('o1Ini') or '--'}/{top.get('o2Ini') or '--'}) -> 即盘{top.get('o3NewCn') or top.get('o3NewStr') or '--'} "
            f"({top.get('o1New') or '--'}/{top.get('o2New') or '--'})"
        )

    def _build_odds_totals_line(self, items: list[dict]) -> str:
        if not items:
            return '大小球暂无'
        top = items[0]
        return (
            f"大小球 {top.get('companyName') or '--'} 初盘{top.get('o3IniCn') or top.get('o3IniStr') or '--'} "
            f"({top.get('o1Ini') or '--'}/{top.get('o2Ini') or '--'}) -> 即盘{top.get('o3NewCn') or top.get('o3NewStr') or '--'} "
            f"({top.get('o1New') or '--'}/{top.get('o2New') or '--'})"
        )

    def _build_battle_history_line(self, items: list[dict], home_team: str, away_team: str) -> str:
        if not items:
            return '暂无'
        recent = items[:5]
        summary = []
        for item in recent:
            summary.append(f"{item.get('team1')} {item.get('score1')}:{item.get('score2')} {item.get('team2')}")
        return ' | '.join(summary)

    def _build_recent_form_line(self, items: list[dict], team_name: str, team_id: str | None) -> str:
        if not items:
            return f"{team_name} 近期暂无"
        recent = items[:5]
        win = draw = loss = goals_for = goals_against = 0
        samples = []
        for item in recent:
            score1 = football_utils.parse_int(item.get('score1'))
            score2 = football_utils.parse_int(item.get('score2'))
            team1 = str(item.get('team1') or '')
            team2 = str(item.get('team2') or '')
            team1_id = str(item.get('team1Id') or '').strip()
            team2_id = str(item.get('team2Id') or '').strip()
            if score1 is None or score2 is None:
                continue
            if team_id and team1_id == str(team_id):
                gf, ga = score1, score2
            elif team_id and team2_id == str(team_id):
                gf, ga = score2, score1
            elif team1 == team_name:
                gf, ga = score1, score2
            elif team2 == team_name:
                gf, ga = score2, score1
            else:
                continue
            goals_for += gf
            goals_against += ga
            if gf > ga:
                win += 1
            elif gf == ga:
                draw += 1
            else:
                loss += 1
            samples.append(f"{team1} {score1}:{score2} {team2}")
        return f"{team_name} 近5场{win}胜{draw}平{loss}负，进{goals_for}失{goals_against}，样本：{' | '.join(samples[:3])}"

    def _build_injury_line(self, injury: dict) -> str:
        team1 = injury.get('team1') or []
        team2 = injury.get('team2') or []
        def _format(items):
            if not items:
                return '无'
            top = items[:3]
            return '、'.join(f"{item.get('playerShortName') or item.get('playerName')}({item.get('typeCn') or '--'})" for item in top)
        return f"主队={_format(team1)}；客队={_format(team2)}"

    def _build_intelligence_line(self, intelligence: dict) -> str:
        if not intelligence:
            return '暂无'
        def _top_text(section: dict, key: str):
            values = (section.get(key) or [])[:2]
            if not values:
                return '无'
            return ' | '.join(str(item.get('content') or '').strip() for item in values if str(item.get('content') or '').strip())
        team1 = intelligence.get('team1') or {}
        team2 = intelligence.get('team2') or {}
        neutral = intelligence.get('neutral') or []
        neutral_text = ' | '.join(str(item.get('content') or '').strip() for item in neutral[:2] if str(item.get('content') or '').strip()) or '无'
        return f"主队利好={_top_text(team1, 'good')}；主队隐患={_top_text(team1, 'bad')}；客队利好={_top_text(team2, 'good')}；客队隐患={_top_text(team2, 'bad')}；中性={neutral_text}"

    def _build_recent_matches_line(self, recent_matches: dict) -> str:
        if not recent_matches:
            return '暂无'
        team1 = recent_matches.get('team1') or []
        team2 = recent_matches.get('team2') or []
        def _format(items):
            if not items:
                return '无'
            top = items[:2]
            return ' | '.join(f"{item.get('matchTimeFormat') or '--'} {item.get('team1')} vs {item.get('team2')}" for item in top)
        return f"主队后续={_format(team1)}；客队后续={_format(team2)}"

    def _record_text(self, item: dict) -> str:
        if not item:
            return '--'
        return f"{item.get('won') or 0}胜{item.get('draw') or 0}平{item.get('loss') or 0}负"

    def _normalize_prediction_items(
        self,
        run_id: int,
        predictor: dict,
        run_key: str,
        matches: list[dict],
        items_payload: list[dict],
        raw_response: str,
        item_order_offset: int = 0
    ) -> list[dict]:
        item_by_key = {}
        item_by_match_no = {}
        for item in items_payload:
            if not isinstance(item, dict):
                continue
            event_key = str(item.get('event_key') or '').strip()
            match_no = str(item.get('match_no') or '').strip()
            if event_key:
                item_by_key[event_key] = item
            if match_no:
                item_by_match_no[match_no] = item

        normalized_items = []
        requested_targets = predictor.get('prediction_targets') or []
        for index, match in enumerate(matches):
            raw_item = item_by_key.get(match['event_key']) or item_by_match_no.get(match.get('match_no') or '') or {}
            prediction_payload = {
                'spf': football_utils.normalize_prediction_outcome(raw_item.get('predicted_spf')),
                'rqspf': football_utils.normalize_prediction_outcome(raw_item.get('predicted_rqspf'))
            }
            status = 'pending' if any(prediction_payload.get(target) for target in requested_targets) else 'failed'
            item_order = int(match.get('prediction_item_order') or (item_order_offset + index))
            retry_count = int(match.get('prediction_next_retry_count') or 0)
            last_retry_at = match.get('prediction_last_retry_at') if retry_count else None
            normalized_items.append({
                'run_id': run_id,
                'predictor_id': predictor['id'],
                'lottery_type': self.lottery_type,
                'run_key': run_key,
                'event_key': match['event_key'],
                'item_order': item_order,
                'issue_no': match.get('match_no') or match['event_key'],
                'title': match.get('event_name') or '',
                'requested_targets': requested_targets,
                'prediction_payload': prediction_payload,
                'actual_payload': {},
                'hit_payload': {},
                'confidence': football_utils.clamp_confidence(raw_item.get('confidence')),
                'reasoning_summary': str(raw_item.get('reasoning_summary') or '').strip(),
                'raw_response': raw_response,
                'status': status,
                'error_message': None if status == 'pending' else 'AI 未返回该场次的有效预测',
                'retry_count': retry_count,
                'last_retry_at': last_retry_at,
                'last_retry_error': None if status == 'pending' else ('AI 未返回该场次的有效预测' if retry_count else None),
                'settled_at': None
            })
        return normalized_items

    def _build_failed_prediction_items(
        self,
        run_id: int,
        predictor: dict,
        run_key: str,
        matches: list[dict],
        raw_response: str,
        error_message: str,
        batch_label: str = '',
        item_order_offset: int = 0
    ) -> list[dict]:
        requested_targets = predictor.get('prediction_targets') or []
        formatted_error_message = self._format_failed_batch_error_message(
            batch_label=batch_label,
            matches=matches,
            error_message=error_message
        )
        return [
            {
                'run_id': run_id,
                'predictor_id': predictor['id'],
                'lottery_type': self.lottery_type,
                'run_key': run_key,
                'event_key': match['event_key'],
                'item_order': int(match.get('prediction_item_order') or (item_order_offset + index)),
                'issue_no': match.get('match_no') or match['event_key'],
                'title': match.get('event_name') or '',
                'requested_targets': requested_targets,
                'prediction_payload': {},
                'actual_payload': {},
                'hit_payload': {},
                'confidence': None,
                'reasoning_summary': '',
                'raw_response': raw_response,
                'status': 'failed',
                'error_message': formatted_error_message,
                'retry_count': int(match.get('prediction_next_retry_count') or 0),
                'last_retry_at': match.get('prediction_last_retry_at') if int(match.get('prediction_next_retry_count') or 0) else None,
                'last_retry_error': error_message if int(match.get('prediction_next_retry_count') or 0) else None,
                'settled_at': None
            }
            for index, match in enumerate(matches)
        ]

    def _format_failed_batch_error_message(self, batch_label: str, matches: list[dict], error_message: str) -> str:
        label_text = str(batch_label or '--').strip()
        normalized_error = str(error_message or 'AI 子批次预测失败').strip()
        match_names = [str(match.get('match_no') or match.get('event_key') or '').strip() for match in matches]
        match_names = [item for item in match_names if item]
        match_text = '、'.join(match_names) if match_names else '未知场次'
        match_text = f'{match_text}（共{len(matches)}场）'
        return f'子批次 {label_text} 失败；场次：{match_text}；原因：{normalized_error}'

    def _parse_failed_batch_error_message(self, error_message: str) -> dict | None:
        text = str(error_message or '').strip()
        if not text:
            return None
        match = re.match(r'^子批次\s*(?P<label>[^；]+)\s*失败；场次：(?P<matches>.+?)；原因：(?P<reason>.+)$', text)
        if not match:
            return None

        match_text = str(match.group('matches') or '').strip()
        matches_text = match_text.split('（共', 1)[0].strip()
        match_names = [item.strip() for item in matches_text.split('、') if item.strip()]
        return {
            'batch_label': str(match.group('label') or '').strip(),
            'match_text': match_text,
            'match_names': match_names,
            'reason': str(match.group('reason') or '').strip(),
            'message': text
        }

    def _build_failed_batch_summaries(self, failed_items: list[dict]) -> list[dict]:
        summaries: list[dict] = []
        seen: set[str] = set()
        for item in failed_items:
            parsed = self._parse_failed_batch_error_message(item.get('error_message') or '')
            if not parsed:
                continue
            key = f"{parsed['batch_label']}|{parsed['match_text']}|{parsed['reason']}"
            if key in seen:
                continue
            seen.add(key)
            summaries.append({
                **parsed,
                'issue_no': item.get('issue_no'),
                'title': item.get('title')
            })
        return summaries

    def _build_actual_payload(self, match: dict, requested_targets: list[str]) -> dict:
        payload = {}
        if 'spf' in requested_targets:
            payload['spf'] = match.get('actual_spf')
        if 'rqspf' in requested_targets:
            payload['rqspf'] = match.get('actual_rqspf')
        payload['score_text'] = match.get('score_text') or ''
        payload['home_team'] = match.get('home_team') or ''
        payload['away_team'] = match.get('away_team') or ''
        payload['league'] = match.get('league') or ''
        return payload

    def _build_hit_payload(self, prediction_payload: dict, actual_payload: dict, requested_targets: list[str]) -> dict:
        hit_payload = {}
        for target in requested_targets:
            predicted = prediction_payload.get(target)
            actual = actual_payload.get(target)
            if predicted is None or actual is None:
                hit_payload[target] = None
            else:
                hit_payload[target] = 1 if predicted == actual else 0
        return hit_payload

    def _refresh_run_summary(self, db, run_id: int):
        run = db.get_prediction_run(run_id)
        if not run:
            return
        items = db.get_prediction_run_items(run_id)
        total_items = len(items)
        settled_items = [item for item in items if item['status'] == 'settled']
        pending_items = [item for item in items if item['status'] == 'pending']
        expired_items = [item for item in items if item['status'] == 'expired']
        failed_items = [item for item in items if item['status'] == 'failed']
        attempted_hits = []
        for item in settled_items:
            hit_payload = item.get('hit_payload') or {}
            attempted_hits.extend(value for value in hit_payload.values() if value is not None)
        hit_items = sum(attempted_hits)
        run_status = run.get('status', 'pending')
        settled_at = run.get('settled_at')
        if total_items and len(settled_items) == total_items:
            run_status = 'settled'
            settled_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        elif total_items and not pending_items:
            if failed_items and not settled_items and not expired_items:
                run_status = 'failed'
            else:
                run_status = 'expired'
            settled_at = settled_at or datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        elif run_status != 'failed':
            run_status = 'pending'

        db.upsert_prediction_run({
            **run,
            'status': run_status,
            'total_items': total_items,
            'settled_items': len(settled_items),
            'hit_items': hit_items,
            'settled_at': settled_at
        })

    def _build_run_view_model(self, db, run: dict | None) -> dict | None:
        if not run:
            return None
        items = self._decorate_prediction_items(db, db.get_prediction_run_items(run['id']))
        ticket_plan = self._build_recommended_ticket_plan(items)
        failed_batches = self._build_failed_batch_summaries([item for item in items if item.get('status') == 'failed'])
        return {
            **run,
            'lottery_type': self.lottery_type,
            'items': items,
            'failed_batches': failed_batches,
            'recommended_parlay': self._build_two_match_recommendation(items),
            'recommended_tickets': ticket_plan['tickets'],
            'recommended_ticket_warnings': ticket_plan['warnings']
        }

    def _build_two_match_recommendation(self, items: list[dict]) -> list[dict]:
        top_items: list[dict] = []
        for item in football_utils.rank_prediction_items(items):
            market_snapshot = item.get('market_snapshot') or {}
            meta_payload = self._build_market_snapshot_payload(market_snapshot)
            prediction_payload = item.get('prediction_payload') or {}
            can_use_spf = self._is_metric_on_sale('spf', meta_payload, prediction_payload.get('spf'), play_mode='parlay')
            can_use_rqspf = self._is_metric_on_sale('rqspf', meta_payload, prediction_payload.get('rqspf'), play_mode='parlay')
            if not (can_use_spf or can_use_rqspf):
                continue
            top_items.append(item)
            if len(top_items) >= 2:
                break
        return [
            {
                'issue_no': item.get('issue_no'),
                'title': item.get('title'),
                'prediction_payload': item.get('prediction_payload') or {},
                'market_snapshot': item.get('market_snapshot') or {},
                'confidence': item.get('confidence')
            }
            for item in top_items
        ]

    def _decorate_prediction_items(self, db, items: list[dict]) -> list[dict]:
        if not items:
            return []

        event_map = db.get_lottery_event_map(
            self.lottery_type,
            [item.get('event_key') for item in items if item.get('event_key')]
        )
        detail_map = {
            event_key: db.get_lottery_event_details(self.lottery_type, event_key, source_provider='sina')
            for event_key in {item.get('event_key') for item in items if item.get('event_key')}
        }
        decorated = []
        for item in items:
            failed_batch = self._parse_failed_batch_error_message(item.get('error_message') or '')
            event = event_map.get(item.get('event_key')) or {}
            meta_payload = event.get('meta_payload') or {}
            detail_payload = detail_map.get(item.get('event_key')) or {}
            odds_snapshots = ((detail_payload.get('odds_snapshots') or {}).get('payload') or {})
            decorated.append({
                **item,
                'batch_failure_label': (failed_batch or {}).get('batch_label'),
                'batch_failure_match_text': (failed_batch or {}).get('match_text'),
                'batch_failure_matches': (failed_batch or {}).get('match_names') or [],
                'batch_failure_reason': (failed_batch or {}).get('reason'),
                'market_snapshot': {
                    'event_time': event.get('event_time') or '',
                    'status': event.get('status') or '',
                    'status_label': event.get('status_label') or '',
                    'spf_odds': meta_payload.get('spf_odds') or {},
                    'rqspf': meta_payload.get('rqspf') or {},
                    'odds_snapshots': odds_snapshots,
                    'spf_sell_status': meta_payload.get('spf_sell_status') or '',
                    'rqspf_sell_status': meta_payload.get('rqspf_sell_status') or '',
                    'spf_single_sellable': self._is_metric_on_sale('spf', meta_payload, play_mode='single'),
                    'rqspf_single_sellable': self._is_metric_on_sale('rqspf', meta_payload, play_mode='single'),
                    'spf_parlay_sellable': self._is_metric_on_sale('spf', meta_payload, play_mode='parlay'),
                    'rqspf_parlay_sellable': self._is_metric_on_sale('rqspf', meta_payload, play_mode='parlay'),
                    'spf_sellable': self._is_metric_on_sale('spf', meta_payload, play_mode='parlay'),
                    'rqspf_sellable': self._is_metric_on_sale('rqspf', meta_payload, play_mode='parlay'),
                    'spf_single_availability_label': football_utils.metric_availability_label('spf', meta_payload, play_mode='single'),
                    'rqspf_single_availability_label': football_utils.metric_availability_label('rqspf', meta_payload, play_mode='single'),
                    'spf_parlay_availability_label': football_utils.metric_availability_label('spf', meta_payload, play_mode='parlay'),
                    'rqspf_parlay_availability_label': football_utils.metric_availability_label('rqspf', meta_payload, play_mode='parlay'),
                    'spf_availability_label': football_utils.metric_availability_label('spf', meta_payload, play_mode='parlay'),
                    'rqspf_availability_label': football_utils.metric_availability_label('rqspf', meta_payload, play_mode='parlay'),
                    'odds_source_label': '预测批次赔率快照'
                }
            })
        return decorated

    def _build_recommended_ticket_plan(self, items: list[dict]) -> dict:
        tickets = []
        warnings = []
        for metric in ('spf', 'rqspf'):
            ranked_items = football_utils.rank_prediction_items(items, metric_key=metric)
            legs = []
            for item in ranked_items:
                market_snapshot = item.get('market_snapshot') or {}
                meta_payload = self._build_market_snapshot_payload(market_snapshot)
                prediction_payload = item.get('prediction_payload') or {}
                outcome = prediction_payload.get(metric)
                if not outcome or not self._is_metric_on_sale(metric, meta_payload, outcome, play_mode='parlay'):
                    continue
                odds = football_utils.resolve_snapshot_odds(meta_payload, metric, outcome)
                if odds is None or odds <= 0:
                    continue
                legs.append({
                    'issue_no': item.get('issue_no') or '--',
                    'title': item.get('title') or '--',
                    'outcome': outcome,
                    'display_outcome': self._format_ticket_outcome(metric, outcome, meta_payload),
                    'odds': float(odds)
                })
                if len(legs) == 2:
                    break

            if len(legs) < 2:
                warnings.append({
                    'metric': metric,
                    'metric_label': football_utils.TARGET_LABELS.get(f'{metric}_parlay', metric),
                    'message': f'{football_utils.TARGET_LABELS.get(metric, metric)} 仅有 {len(legs)} 场可参与二串一，未生成推荐票面'
                })
                continue

            tickets.append({
                'metric': f'{metric}_parlay',
                'metric_label': football_utils.TARGET_LABELS.get(f'{metric}_parlay', metric),
                'ticket_text': ' + '.join(f"{leg['issue_no']} {leg['display_outcome']}" for leg in legs),
                'odds': round(legs[0]['odds'] * legs[1]['odds'], 4),
                'legs': legs,
                'odds_source_label': '预测批次赔率快照'
            })

        return {
            'tickets': tickets,
            'warnings': warnings
        }

    def _build_market_snapshot_payload(self, market_snapshot: dict) -> dict:
        return {
            'spf_odds': market_snapshot.get('spf_odds') or {},
            'rqspf': market_snapshot.get('rqspf') or {},
            'settled': str(market_snapshot.get('status') or '').strip() == '3',
            'spf_sell_status': str(market_snapshot.get('spf_sell_status') or '').strip(),
            'rqspf_sell_status': str(market_snapshot.get('rqspf_sell_status') or '').strip()
        }

    def _is_metric_on_sale(self, metric_key: str, meta_payload: dict, outcome: str | None = None, play_mode: str = 'parlay') -> bool:
        return football_utils.is_metric_sellable(metric_key, meta_payload, outcome, play_mode=play_mode)

    def _format_ticket_outcome(self, metric: str, outcome: str, meta_payload: dict) -> str:
        if metric != 'rqspf':
            return outcome
        handicap_text = str(((meta_payload.get('rqspf') or {}).get('handicap_text') or '')).strip()
        if not handicap_text:
            return outcome
        return f'{outcome}(让{handicap_text})'

    def _build_overview_from_matches(self, matches: list[dict], batch_key: str | None, limit: int = 20, warning: str | None = None) -> dict:
        sorted_matches = sorted(matches, key=lambda item: item.get('match_time') or '')
        sale_open_matches = [item for item in sorted_matches if football_utils.is_match_sale_open(item)]
        awaiting_result_matches = [item for item in sorted_matches if football_utils.is_match_awaiting_result(item)]
        settled_matches = [
            item for item in sorted_matches
            if football_utils.is_match_prized(item) or item.get('settled')
        ]
        next_match = sale_open_matches[0] if sale_open_matches else None
        payload = {
            'lottery_type': self.lottery_type,
            'batch_key': batch_key,
            'match_count': len(sorted_matches),
            'open_match_count': len(sale_open_matches),
            'sale_open_match_count': len(sale_open_matches),
            'awaiting_result_match_count': len(awaiting_result_matches),
            'settled_match_count': len(settled_matches),
            'next_match_time': next_match.get('match_time') if next_match else None,
            'next_match_name': next_match.get('event_name') if next_match else None,
            'recent_events': sorted_matches[:limit]
        }
        if warning:
            payload['warning'] = warning
        return payload

    def _build_overview_from_events(self, events: list[dict], batch_key: str | None, limit: int = 20, warning: str | None = None) -> dict:
        sorted_events = sorted(events, key=lambda item: item.get('event_time') or '')
        active_events = [item for item in sorted_events if not (item.get('meta_payload') or {}).get('expired')]
        sale_open_events = [item for item in active_events if football_utils.is_match_sale_open(item)]
        awaiting_result_events = [item for item in active_events if football_utils.is_match_awaiting_result(item)]
        settled_events = [
            item for item in sorted_events
            if football_utils.is_match_prized(item) or (item.get('meta_payload') or {}).get('settled')
        ]
        next_event = sale_open_events[0] if sale_open_events else None
        payload = {
            'lottery_type': self.lottery_type,
            'batch_key': batch_key,
            'match_count': len(sorted_events),
            'open_match_count': len(sale_open_events),
            'sale_open_match_count': len(sale_open_events),
            'awaiting_result_match_count': len(awaiting_result_events),
            'settled_match_count': len(settled_events),
            'next_match_time': next_event.get('event_time') if next_event else None,
            'next_match_name': next_event.get('event_name') if next_event else None,
            'recent_events': sorted_events[:limit]
        }
        if warning:
            payload['warning'] = warning
        return payload

    def _is_history_batch(self, date: str) -> bool:
        normalized_date = str(date or '').strip()
        return bool(normalized_date) and normalized_date < self.current_sale_date()

    def _has_overdue_unsettled_matches(self, matches: list[dict]) -> bool:
        if not matches:
            return False

        grace_deadline = get_current_beijing_time() - timedelta(minutes=HISTORY_RESULT_REFRESH_GRACE_MINUTES)
        for match in matches:
            if match.get('settled'):
                continue
            kickoff_time = parse_beijing_time(match.get('match_time') or '')
            if kickoff_time and kickoff_time <= grace_deadline:
                return True
        return False

    def _sync_matches_best_effort(self, db, date: str, prefer_is_prized: str | None = None) -> tuple[dict, str]:
        candidates = []
        is_history = self._is_history_batch(date)
        if prefer_is_prized is not None:
            candidates.append(str(prefer_is_prized))
        else:
            candidates.extend(['1', ''] if is_history else ['', '1'])

        last_error = None
        seen = set()
        for is_prized in candidates:
            if is_prized in seen:
                continue
            seen.add(is_prized)
            try:
                payload = self.sync_matches(db, date=date, is_prized=is_prized, cache_bust=False)
                if is_history and self._has_overdue_unsettled_matches(payload.get('matches') or []):
                    try:
                        payload = self.sync_matches(db, date=date, is_prized=is_prized, cache_bust=True)
                    except Exception:
                        pass
                return payload, is_prized
            except Exception as exc:
                last_error = exc

        cached_events = db.get_recent_lottery_events(self.lottery_type, limit=200, batch_key=date)
        if cached_events:
            return {
                'lottery_type': self.lottery_type,
                'batch_key': date,
                'matches': [self._build_match_from_cached_event(item) for item in cached_events],
                'warning': f'新浪接口不可用，已回退本地缓存：{last_error}' if last_error else None
            }, 'cache'

        if last_error:
            raise last_error
        raise ValueError('无法同步竞彩足球批次数据')

    def _settle_run_with_payload(self, db, run: dict, payload: dict) -> dict:
        match_map = {item['event_key']: item for item in payload['matches']}
        items = db.get_prediction_run_items(run['id'])
        changed = False
        settled_item_count = 0
        for item in items:
            if item['status'] != 'pending':
                continue

            match = match_map.get(item['event_key'])
            if not match or not match.get('settled'):
                continue

            actual_payload = self._build_actual_payload(match, item.get('requested_targets') or [])
            hit_payload = self._build_hit_payload(
                prediction_payload=item.get('prediction_payload') or {},
                actual_payload=actual_payload,
                requested_targets=item.get('requested_targets') or []
            )
            db.upsert_prediction_items([{
                **item,
                'actual_payload': actual_payload,
                'hit_payload': hit_payload,
                'status': 'settled',
                'error_message': None,
                'settled_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            }])
            changed = True
            settled_item_count += 1

        if changed:
            self._refresh_run_summary(db, run['id'])

        refreshed_run = db.get_prediction_run(run['id'])
        refreshed_items = db.get_prediction_run_items(run['id'])
        return {
            'run': refreshed_run,
            'items': refreshed_items,
            'settled_item_count': settled_item_count
        }

    def _expire_run_overdue_items(self, db, run: dict, payload: dict) -> dict:
        items = db.get_prediction_run_items(run['id'])
        pending_items = [item for item in items if item.get('status') == 'pending']
        if not pending_items:
            return {
                'run': db.get_prediction_run(run['id']),
                'items': items,
                'expired_item_count': 0
            }

        match_map = {
            item['event_key']: item
            for item in (payload.get('matches') or [])
            if item.get('event_key')
        }
        event_keys = [item['event_key'] for item in pending_items if item.get('event_key')]
        event_map = db.get_lottery_event_map(self.lottery_type, event_keys, source_provider='sina')
        now_beijing = get_current_beijing_time()
        expire_before = now_beijing - timedelta(hours=UNSETTLED_EXPIRE_HOURS)
        settled_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        reason = f'超过 {UNSETTLED_EXPIRE_HOURS} 小时未获取到官方赛果，自动过期关闭'
        expired_event_keys = []
        expired_count = 0

        for item in pending_items:
            event_key = item.get('event_key')
            match = match_map.get(event_key)
            if match and match.get('settled'):
                continue

            event = event_map.get(event_key) or {}
            kickoff_time = parse_beijing_time((match or {}).get('match_time') or event.get('event_time') or '')
            if kickoff_time is None or kickoff_time > expire_before:
                continue

            db.upsert_prediction_items([{
                **item,
                'actual_payload': {},
                'hit_payload': {},
                'status': 'expired',
                'error_message': reason,
                'settled_at': settled_at
            }])
            expired_count += 1
            if event_key:
                expired_event_keys.append(event_key)

        if expired_count:
            self._mark_lottery_events_expired(db, expired_event_keys, settled_at, reason)
            self._refresh_run_summary(db, run['id'])

        refreshed_run = db.get_prediction_run(run['id'])
        refreshed_items = db.get_prediction_run_items(run['id'])
        return {
            'run': refreshed_run,
            'items': refreshed_items,
            'expired_item_count': expired_count
        }

    def _mark_lottery_events_expired(self, db, event_keys: list[str], expired_at: str, reason: str):
        unique_event_keys = [key for key in dict.fromkeys(event_keys) if key]
        if not unique_event_keys:
            return

        event_map = db.get_lottery_event_map(self.lottery_type, unique_event_keys, source_provider='sina')
        records = []
        for event in event_map.values():
            meta_payload = dict(event.get('meta_payload') or {})
            if meta_payload.get('settled'):
                continue
            meta_payload.update({
                'expired': True,
                'expired_at': expired_at,
                'expire_reason': reason
            })
            records.append({
                'lottery_type': self.lottery_type,
                'event_key': event.get('event_key'),
                'batch_key': event.get('batch_key') or '',
                'event_date': event.get('event_date') or '',
                'event_time': event.get('event_time') or '',
                'event_name': event.get('event_name') or '',
                'league': event.get('league') or '',
                'home_team': event.get('home_team') or '',
                'away_team': event.get('away_team') or '',
                'status': event.get('status') or '',
                'status_label': event.get('status_label') or '',
                'source_provider': event.get('source_provider') or 'sina',
                'result_payload': football_utils.dump_json(event.get('result_payload') or {}),
                'meta_payload': football_utils.dump_json(meta_payload),
                'source_payload': football_utils.dump_json(event.get('source_payload') or {})
            })

        db.upsert_lottery_events(records)

    def _build_match_from_cached_event(self, event: dict) -> dict:
        meta_payload = event.get('meta_payload') or {}
        result_payload = event.get('result_payload') or {}
        return {
            'lottery_type': self.lottery_type,
            'event_key': event.get('event_key'),
            'batch_key': event.get('batch_key') or '',
            'match_no': meta_payload.get('match_no') or meta_payload.get('match_no_value') or event.get('issue_no') or '',
            'match_no_value': meta_payload.get('match_no_value') or '',
            'league': event.get('league') or '',
            'home_team': event.get('home_team') or '',
            'away_team': event.get('away_team') or '',
            'event_name': event.get('event_name') or '',
            'match_time': event.get('event_time') or '',
            'show_sell_status': event.get('status') or '',
            'show_sell_status_label': event.get('status_label') or '',
            'spf_sell_status': meta_payload.get('spf_sell_status') or '',
            'rqspf_sell_status': meta_payload.get('rqspf_sell_status') or '',
            'spf_odds': meta_payload.get('spf_odds') or {},
            'rqspf': meta_payload.get('rqspf') or {},
            'score1': result_payload.get('score1'),
            'score2': result_payload.get('score2'),
            'half_score1': result_payload.get('half_score1'),
            'half_score2': result_payload.get('half_score2'),
            'score_text': meta_payload.get('score_text') or '',
            'settled': bool(meta_payload.get('settled')),
            'actual_spf': result_payload.get('actual_spf'),
            'actual_rqspf': result_payload.get('actual_rqspf'),
            'raw_item': {}
        }

    def _build_metric_stats(self, items: list[dict], metric_key: str) -> dict:
        outcomes = [
            (item.get('hit_payload') or {}).get(metric_key)
            for item in items
        ]
        attempted = [item for item in outcomes if item is not None]
        hit_count = sum(attempted) if attempted else 0
        sample_count = len(attempted)
        hit_rate = round(hit_count / sample_count * 100, 2) if sample_count else None
        return {
            'hit_count': hit_count,
            'sample_count': sample_count,
            'hit_rate': hit_rate,
            'ratio_text': f'{hit_count}/{sample_count}' if sample_count else '--'
        }

    def _build_streak_stats(self, items: list[dict], metric_key: str) -> dict:
        outcomes = [
            (item.get('hit_payload') or {}).get(metric_key)
            for item in items
            if (item.get('hit_payload') or {}).get(metric_key) is not None
        ]
        recent_100 = outcomes[:100]

        current_hit_streak = 0
        current_miss_streak = 0
        for outcome in outcomes:
            if outcome == 1:
                if current_miss_streak == 0:
                    current_hit_streak += 1
                else:
                    break
            else:
                if current_hit_streak == 0:
                    current_miss_streak += 1
                else:
                    break

        return {
            'current_hit_streak': current_hit_streak,
            'current_miss_streak': current_miss_streak,
            'recent_100_max_hit_streak': self._max_streak(recent_100, 1),
            'recent_100_max_miss_streak': self._max_streak(recent_100, 0),
            'historical_max_hit_streak': self._max_streak(outcomes, 1),
            'historical_max_miss_streak': self._max_streak(outcomes, 0)
        }

    def _max_streak(self, outcomes: list[int], expected: int) -> int:
        best = 0
        current = 0
        for outcome in outcomes:
            if outcome == expected:
                current += 1
                best = max(best, current)
            else:
                current = 0
        return best

    def _compute_score_percentage(self, item: dict) -> float | None:
        hit_payload = item.get('hit_payload') or {}
        attempted = [value for value in hit_payload.values() if value is not None]
        if not attempted:
            return None
        return round(sum(attempted) / len(attempted) * 100, 2)

    def _average_confidence(self, items_payload: list[dict]) -> float | None:
        values = [
            football_utils.clamp_confidence(item.get('confidence'))
            for item in items_payload
            if isinstance(item, dict)
        ]
        numbers = [item for item in values if item is not None]
        if not numbers:
            return None
        return round(sum(numbers) / len(numbers), 4)

    def _format_odds_text(self, odds: dict[str, object]) -> str:
        return ' / '.join(
            f'{key}:{value:.2f}' if isinstance(value, (float, int)) else f'{key}:--'
            for key, value in odds.items()
        )
