"""
竞彩足球数据与预测服务
"""
from __future__ import annotations

from datetime import datetime, timedelta

import requests

from ai_trader import AIPredictor
import config
from utils import jingcai_football as football_utils
from utils.timezone import get_current_beijing_time, parse_beijing_time


SINA_JINGCAI_URL = 'https://alpha.lottery.sina.com.cn/gateway/index/entry'
SINA_JINGCAI_DETAIL_URL = 'https://mix.lottery.sina.com.cn/gateway/index/entry'
DEFAULT_TIMEOUT = getattr(config, 'JINGCAI_REQUEST_TIMEOUT', 15)
DETAIL_CACHE_SECONDS = getattr(config, 'JINGCAI_DETAIL_CACHE_SECONDS', 6 * 60 * 60)
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Referer': 'https://alpha.lottery.sina.com.cn/lottery/jczq/'
}
DETAIL_HEADERS = {
    'User-Agent': DEFAULT_HEADERS['User-Agent'],
    'Referer': 'https://mix.lottery.sina.com.cn/'
}


class JingcaiFootballService:
    lottery_type = 'jingcai_football'

    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        self.timeout = timeout
        self._next_auto_run_at = None
        self._last_auto_reason = ''

    def _request_payload(self, date: str = '', is_prized: str = '', game_types: str = 'spf') -> dict:
        response = requests.get(
            SINA_JINGCAI_URL,
            params={
                'format': 'json',
                '__caller__': 'wap',
                '__version__': '1.0.0',
                '__verno__': '10000',
                'cat1': 'jczqMatches',
                'gameTypes': game_types,
                'date': date,
                'isPrized': is_prized,
                'isAll': 1,
                'dpc': 1
            },
            headers=DEFAULT_HEADERS,
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
        headers = DETAIL_HEADERS if host == 'mix' else DEFAULT_HEADERS
        response = requests.get(
            base_url,
            params={
                'format': 'json',
                '__caller__': 'wap',
                '__version__': '1.0.0',
                '__verno__': '10000',
                'cat1': cat1,
                'dpc': 1,
                **params
            },
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

    def fetch_matches(self, date: str = '', is_prized: str = '', game_types: str = 'spf') -> dict:
        result = self._request_payload(date=date, is_prized=is_prized, game_types=game_types)
        batch_key = str(result.get('date') or date or '').strip()
        matches = [self._normalize_match(item, batch_key=batch_key) for item in (result.get('data') or [])]
        matches = [item for item in matches if item]
        return {
            'lottery_type': self.lottery_type,
            'batch_key': batch_key,
            'dates': result.get('dates') or [],
            'matches': matches
        }

    def sync_matches(self, db, date: str = '', is_prized: str = '', game_types: str = 'spf') -> dict:
        payload = self.fetch_matches(date=date, is_prized=is_prized, game_types=game_types)
        events = [self._build_event_record(item) for item in payload['matches']]
        db.upsert_lottery_events(events)
        return payload

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
        return bundle

    def _safe_detail_request(self, cat1: str, params: dict, host: str = 'mix'):
        try:
            return self._request_detail_payload(cat1, params, host=host)
        except Exception:
            return {} if cat1 not in {'footballMatchTeamBattleHistory', 'footballMatchTeamRecentZhanJi', 'footballMatchOddsEuro', 'footballMatchOddsAsia', 'footballMatchOddsTotals'} else []

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
        matches = payload['matches']
        sorted_matches = sorted(matches, key=lambda item: item.get('match_time') or '')
        open_matches = [item for item in sorted_matches if not item.get('settled')]
        settled_matches = [item for item in sorted_matches if item.get('settled')]
        next_match = open_matches[0] if open_matches else None

        return {
            'lottery_type': self.lottery_type,
            'batch_key': payload.get('batch_key'),
            'match_count': len(sorted_matches),
            'open_match_count': len(open_matches),
            'settled_match_count': len(settled_matches),
            'next_match_time': next_match.get('match_time') if next_match else None,
            'next_match_name': next_match.get('event_name') if next_match else None,
            'recent_events': sorted_matches[:limit]
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
            if meta_payload.get('settled'):
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
        overview = self.build_overview(db, limit=20)
        stats = self.build_predictor_stats(db, predictor['id'], predictor.get('primary_metric', 'spf'))
        recent_items = self.get_recent_prediction_items(db, predictor['id'], limit=100)
        recent_runs = db.get_recent_prediction_runs(predictor['id'], lottery_type=self.lottery_type, limit=20)
        current_run = next((item for item in recent_runs if item['status'] == 'pending'), None)
        current_prediction = self._build_run_view_model(db, current_run) if current_run else None
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

    def generate_prediction(self, db, predictor: dict, auto_mode: bool = False, batch_payload: dict | None = None) -> dict:
        batch_payload = batch_payload or self.sync_matches(db, is_prized='')
        run_key = batch_payload.get('batch_key') or datetime.now().strftime('%Y-%m-%d')
        upcoming_matches = [item for item in batch_payload['matches'] if not item.get('settled')]
        if not upcoming_matches:
            raise ValueError('当前没有可预测的竞彩足球比赛')

        existing_run = db.get_prediction_run_by_key(predictor['id'], run_key)
        if existing_run and existing_run['status'] in {'pending', 'settled'}:
            return existing_run

        history_matches = self._fetch_recent_history(
            db,
            batch_payload.get('dates') or [],
            run_key,
            predictor.get('history_window') or 40
        )
        enriched_matches = self.enrich_matches_for_prediction(db, upcoming_matches)
        prompt = self._build_prediction_prompt(
            predictor,
            run_key,
            enriched_matches,
            history_matches,
            predictor.get('data_injection_mode') or 'summary'
        )
        predictor_client = AIPredictor(
            api_key=predictor['api_key'],
            api_url=predictor['api_url'],
            model_name=predictor['model_name'],
            api_mode=predictor.get('api_mode', 'auto'),
            temperature=predictor['temperature']
        )

        raw_response = ''
        prompt_snapshot = prompt
        try:
            llm_result = predictor_client.run_json_task(
                prompt=prompt,
                system_prompt='你是中国竞彩足球预测助手。你必须只输出单个 JSON 对象。',
                max_output_tokens=3200
            )
            raw_response = llm_result['raw_response']
            payload = llm_result['payload']
            items_payload = payload.get('predictions') if isinstance(payload, dict) else None
            if not isinstance(items_payload, list):
                raise ValueError('AI 返回 JSON 缺少 predictions 数组')
            run_payload = {
                'predictor_id': predictor['id'],
                'lottery_type': self.lottery_type,
                'run_key': run_key,
                'title': f'{run_key} 竞彩足球批次预测',
                'requested_targets': predictor.get('prediction_targets') or [],
                'status': 'pending',
                'total_items': len(upcoming_matches),
                'settled_items': 0,
                'hit_items': 0,
                'confidence': self._average_confidence(items_payload),
                'reasoning_summary': '',
                'raw_response': raw_response,
                'prompt_snapshot': prompt_snapshot,
                'error_message': None,
                'settled_at': None
            }
        except Exception as exc:
            run_payload = {
                'predictor_id': predictor['id'],
                'lottery_type': self.lottery_type,
                'run_key': run_key,
                'title': f'{run_key} 竞彩足球批次预测',
                'requested_targets': predictor.get('prediction_targets') or [],
                'status': 'failed',
                'total_items': 0,
                'settled_items': 0,
                'hit_items': 0,
                'confidence': None,
                'reasoning_summary': '',
                'raw_response': raw_response,
                'prompt_snapshot': prompt_snapshot,
                'error_message': str(exc),
                'settled_at': None
            }
            run_id = db.upsert_prediction_run(run_payload)
            return db.get_prediction_run(run_id) or run_payload

        run_id = db.upsert_prediction_run(run_payload)
        normalized_items = self._normalize_prediction_items(
            run_id=run_id,
            predictor=predictor,
            run_key=run_key,
            matches=enriched_matches,
            items_payload=items_payload,
            raw_response=raw_response
        )
        db.upsert_prediction_items(normalized_items)
        self._refresh_run_summary(db, run_id)
        return db.get_prediction_run(run_id) or run_payload

    def settle_pending_predictions(self, db) -> list[dict]:
        settled_items: list[dict] = []
        pending_runs = db.get_pending_prediction_runs(self.lottery_type)
        for run in pending_runs:
            batch_key = run.get('run_key')
            if not batch_key:
                continue

            try:
                payload = self.sync_matches(db, date=batch_key, is_prized='1')
            except Exception:
                payload = self.sync_matches(db, date=batch_key, is_prized='')

            match_map = {item['event_key']: item for item in payload['matches']}
            items = db.get_prediction_run_items(run['id'])
            changed = False
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

            if changed:
                self._refresh_run_summary(db, run['id'])
                settled_items.extend(db.get_prediction_run_items(run['id']))

        return [item for item in settled_items if item.get('status') == 'settled']

    def run_auto_cycle(self, db) -> dict:
        now = get_current_beijing_time()
        if self._next_auto_run_at and now < self._next_auto_run_at:
            return {
                'settled_count': 0,
                'predictions': []
            }

        settled_items = self.settle_pending_predictions(db)
        predictors = db.get_enabled_predictors(lottery_type=self.lottery_type, include_secret=True)
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
            open_matches = [item for item in batch_payload['matches'] if not item.get('settled')]
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
                if existing_run and existing_run.get('status') in {'pending', 'settled'}:
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

    def get_recent_prediction_items(self, db, predictor_id: int, limit: int = 100) -> list[dict]:
        items = db.get_recent_prediction_items(predictor_id, lottery_type=self.lottery_type, limit=limit)
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
            'expired_predictions': 0,
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
        target_labels = '、'.join(football_utils.TARGET_LABELS.get(item, item) for item in requested_targets)
        upcoming_lines = []
        detail_lines = []
        detail_json_items = []
        for match in upcoming_matches:
            spf_text = self._format_odds_text(match.get('spf_odds') or {})
            rqspf = match.get('rqspf') or {}
            rq_text = self._format_odds_text(rqspf.get('odds') or {})
            upcoming_lines.append(
                f"- event_key={match['event_key']} | {match['match_no']} | {match['league']} | {match['home_team']} vs {match['away_team']} | 开赛={match['match_time']} | SPF={spf_text} | RQSPF={rqspf.get('handicap_text') or '--'} [{rq_text}]"
            )
            if match.get('detail_summary'):
                detail_lines.append(match['detail_summary'])
            detail_json_items.append(self._build_match_detail_json(match))

        history_lines = []
        for match in history_matches[:60]:
            history_lines.append(
                f"- {match['match_no']} | {match['league']} | {match['home_team']} {match.get('score_text') or '--'} {match['away_team']} | SPF={match.get('actual_spf') or '--'} | RQSPF={match.get('actual_rqspf') or '--'}"
            )

        custom_prompt = (predictor.get('system_prompt') or '').strip()
        method_name = predictor.get('prediction_method') or '自定义策略'
        detail_block = '\n'.join(detail_lines) if detail_lines else '- 暂无可用详情摘要'
        detail_json_block = football_utils.dump_json(detail_json_items)
        is_raw_mode = str(data_injection_mode).strip().lower() == 'raw'
        extra_label = '待预测比赛详情 JSON' if is_raw_mode else '待预测比赛详情摘要'
        extra_context = detail_json_block if is_raw_mode else detail_block
        return f"""你正在为中国竞彩足球生成一批比赛预测。\n\n基础要求：\n1. 只能预测我提供的待售比赛。\n2. 当前批次：{run_key}。\n3. 预测玩法：{target_labels}。\n4. 不能编造比赛，也不能输出列表外的 event_key。\n5. 只输出一个 JSON 对象，不要 Markdown，不要额外解释。\n6. predicted_spf / predicted_rqspf 只能输出“胜”“平”“负”或 null。\n7. confidence 取值 0-1。\n\n方案信息：\n- 方案名称：{predictor.get('name', '')}\n- 预测方法：{method_name}\n- 历史窗口：最近 {len(history_matches)} 场已结束比赛\n\n用户自定义策略：\n{custom_prompt or '无，按赔率、让球、联赛、历史交锋、近期战绩、积分排名与伤停信息做稳健分析'}\n\n待预测比赛：\n{chr(10).join(upcoming_lines)}\n\n{extra_label}：\n{extra_context}\n\n近期已结束比赛：\n{chr(10).join(history_lines) if history_lines else '- 暂无可用历史比赛'}\n\n输出格式：\n{{\n  \"batch_key\": \"{run_key}\",\n  \"predictions\": [\n    {{\n      \"event_key\": \"示例event_key\",\n      \"match_no\": \"周日024\",\n      \"predicted_spf\": \"胜\",\n      \"predicted_rqspf\": \"平\",\n      \"confidence\": 0.68,\n      \"reasoning_summary\": \"一句简短依据\"\n    }}\n  ]\n}}\n\n现在开始，只输出 JSON。"""

    def _normalize_match(self, item: dict, batch_key: str) -> dict | None:
        event_key = football_utils.resolve_event_key(item)
        if not event_key:
            return None

        score1 = football_utils.parse_int(item.get('score1'))
        score2 = football_utils.parse_int(item.get('score2'))
        rqspf = football_utils.parse_rqspf_odds(item.get('rqspf'))
        actual_spf = football_utils.derive_spf_result(score1, score2)
        actual_rqspf = football_utils.derive_rqspf_result(score1, score2, rqspf.get('handicap'))

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
            'show_sell_status': str(item.get('showSellStatus') or '').strip(),
            'show_sell_status_label': str(item.get('showSellStatusCn') or '').strip(),
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

    def _build_event_record(self, match: dict) -> dict:
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
            'spf_sell_status': match.get('spf_sell_status'),
            'rqspf_sell_status': match.get('rqspf_sell_status'),
            'spf_odds': match.get('spf_odds'),
            'rqspf': match.get('rqspf'),
            'score_text': match.get('score_text'),
            'settled': match.get('settled')
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

    def _normalize_prediction_items(self, run_id: int, predictor: dict, run_key: str, matches: list[dict], items_payload: list[dict], raw_response: str) -> list[dict]:
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
            normalized_items.append({
                'run_id': run_id,
                'predictor_id': predictor['id'],
                'lottery_type': self.lottery_type,
                'run_key': run_key,
                'event_key': match['event_key'],
                'item_order': index,
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
                'settled_at': None
            })
        return normalized_items

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
        items = db.get_prediction_run_items(run['id'])
        return {
            **run,
            'lottery_type': self.lottery_type,
            'items': items,
            'recommended_parlay': self._build_two_match_recommendation(items)
        }

    def _build_two_match_recommendation(self, items: list[dict]) -> list[dict]:
        candidates = [
            item for item in items
            if item.get('status') in {'pending', 'settled'} and (item.get('prediction_payload') or {})
        ]
        ranked = sorted(
            candidates,
            key=lambda item: (
                item.get('confidence') if item.get('confidence') is not None else -1,
                item.get('issue_no') or ''
            ),
            reverse=True
        )
        top_items = ranked[:2]
        return [
            {
                'issue_no': item.get('issue_no'),
                'title': item.get('title'),
                'prediction_payload': item.get('prediction_payload') or {},
                'confidence': item.get('confidence')
            }
            for item in top_items
        ]

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
