"""
PC28 预测执行引擎
"""
from __future__ import annotations

import threading
from typing import Optional

import config
from ai_trader import AIPredictor
from utils.pc28 import next_issue_no
from utils.timezone import get_current_utc_time_str


class PredictionEngine:
    def __init__(self, db, pc28_service):
        self.db = db
        self.pc28_service = pc28_service
        self._lock = threading.RLock()

    def generate_prediction(self, predictor_id: int) -> dict:
        with self._lock:
            predictor = self.db.get_predictor(predictor_id, include_secret=True)
            if not predictor:
                raise ValueError('预测方案不存在')

            draws = self.pc28_service.sync_recent_draws(
                self.db,
                limit=max(config.PC28_SYNC_HISTORY, predictor['history_window'] + 5)
            )
            context = self._build_context(history_window=predictor['history_window'], draws=draws)
            return self._generate_prediction_locked(predictor, context)

    def settle_pending_predictions(self) -> list[dict]:
        with self._lock:
            sync_limit = config.PC28_SYNC_HISTORY
            oldest_pending_issue = self.db.get_oldest_pending_issue('pc28')
            if oldest_pending_issue:
                try:
                    latest_draw = self.pc28_service.fetch_recent_draws(limit=1)
                    latest_issue = int(latest_draw[0]['issue_no']) if latest_draw else int(oldest_pending_issue)
                    oldest_issue = int(oldest_pending_issue)
                    backlog_window = max(latest_issue - oldest_issue + 5, config.PC28_SYNC_HISTORY)
                    sync_limit = min(max(backlog_window, config.PC28_SYNC_HISTORY), 2000)
                except Exception:
                    sync_limit = config.PC28_SYNC_HISTORY

            synced_draws = self.pc28_service.sync_recent_draws(self.db, limit=sync_limit)
            pending_predictions = self.db.get_pending_predictions('pc28')
            settled = []
            oldest_synced_issue = None
            if synced_draws:
                try:
                    oldest_synced_issue = int(synced_draws[-1]['issue_no'])
                except (TypeError, ValueError, KeyError):
                    oldest_synced_issue = None

            for prediction in pending_predictions:
                draw = self.db.get_draw_by_issue('pc28', prediction['issue_no'])
                if not draw:
                    if oldest_synced_issue is not None:
                        try:
                            prediction_issue = int(prediction['issue_no'])
                        except (TypeError, ValueError):
                            prediction_issue = None
                        if prediction_issue is not None and prediction_issue < oldest_synced_issue:
                            expired_payload = {
                                **prediction,
                                'status': 'expired',
                                'error_message': '开奖已超出官方接口可追溯窗口，无法自动补结算',
                                'settled_at': get_current_utc_time_str()
                            }
                            self.db.upsert_prediction(expired_payload)
                    continue

                payload = {
                    **prediction,
                    'status': 'settled',
                    'actual_number': draw['result_number'],
                    'actual_big_small': draw['big_small'],
                    'actual_odd_even': draw['odd_even'],
                    'actual_combo': draw['combo'],
                    'hit_number': self._evaluate_hit(prediction.get('prediction_number'), draw['result_number'], prediction['requested_targets'], 'number'),
                    'hit_big_small': self._evaluate_hit(prediction.get('prediction_big_small'), draw['big_small'], prediction['requested_targets'], 'big_small'),
                    'hit_odd_even': self._evaluate_hit(prediction.get('prediction_odd_even'), draw['odd_even'], prediction['requested_targets'], 'odd_even'),
                    'hit_combo': self._evaluate_hit(prediction.get('prediction_combo'), draw['combo'], prediction['requested_targets'], 'combo'),
                    'settled_at': get_current_utc_time_str(),
                    'error_message': None
                }
                self.db.upsert_prediction(payload)
                settled_record = self.db.get_prediction_by_issue(prediction['predictor_id'], prediction['issue_no'])
                if settled_record:
                    settled.append(settled_record)

            return settled

    def run_auto_cycle(self) -> dict:
        with self._lock:
            settled = self.settle_pending_predictions()
            predictors = self.db.get_enabled_predictors(include_secret=True)
            if not predictors:
                return {
                    'settled_count': len(settled),
                    'predictions': []
                }

            max_window = max(predictor['history_window'] for predictor in predictors)
            draws = self.pc28_service.sync_recent_draws(
                self.db,
                limit=max(config.PC28_SYNC_HISTORY, max_window + 5)
            )
            shared_context = self._build_context(history_window=max_window, draws=draws)

            prediction_results = []
            for predictor in predictors:
                try:
                    result = self._generate_prediction_locked(
                        predictor,
                        {**shared_context, 'recent_draws': shared_context['recent_draws'][:predictor['history_window']]}
                    )
                    prediction_results.append({
                        'predictor_id': predictor['id'],
                        'issue_no': result.get('issue_no'),
                        'status': result.get('status')
                    })
                except Exception as exc:
                    prediction_results.append({
                        'predictor_id': predictor['id'],
                        'status': 'failed',
                        'error': str(exc)
                    })

            return {
                'settled_count': len(settled),
                'predictions': prediction_results
            }

    def _build_context(self, history_window: int, draws: Optional[list[dict]] = None) -> dict:
        recent_draws = draws or self.db.get_recent_draws('pc28', limit=history_window)
        keno_snapshot = self.pc28_service.fetch_keno_snapshot()
        omission_stats = self.pc28_service.fetch_omission_stats()
        today_stats = self.pc28_service.fetch_today_stats()

        preview = {}
        try:
            preview = self.pc28_service.fetch_preview()
        except Exception:
            preview = {}

        latest_draw = recent_draws[0] if recent_draws else None
        next_issue = keno_snapshot.get('next_issue_no')
        if not next_issue and latest_draw:
            next_issue = next_issue_no(latest_draw['issue_no'])

        return {
            'latest_draw': latest_draw,
            'next_issue_no': next_issue,
            'countdown': keno_snapshot.get('countdown', '00:00:00'),
            'recent_draws': recent_draws[:history_window],
            'omission_preview': self.pc28_service._build_omission_preview(omission_stats),
            'today_preview': self.pc28_service._build_today_preview(today_stats),
            'preview': preview
        }

    def _generate_prediction_locked(self, predictor: dict, context: dict) -> dict:
        issue_no = context.get('next_issue_no')
        if not issue_no:
            raise ValueError('无法确定下一期期号')

        existing_prediction = self.db.get_prediction_by_issue(predictor['id'], issue_no)
        if existing_prediction and existing_prediction['status'] in {'pending', 'settled'}:
            return existing_prediction

        predictor_client = AIPredictor(
            api_key=predictor['api_key'],
            api_url=predictor['api_url'],
            model_name=predictor['model_name'],
            api_mode=predictor.get('api_mode', 'auto'),
            temperature=predictor['temperature']
        )

        prompt_snapshot = ''
        raw_response = ''
        try:
            prediction, raw_response, prompt_snapshot = predictor_client.predict_next_issue(context, predictor)
            payload = {
                'predictor_id': predictor['id'],
                'lottery_type': predictor.get('lottery_type', 'pc28'),
                'issue_no': issue_no,
                'requested_targets': predictor['prediction_targets'],
                'prediction_number': prediction.get('prediction_number'),
                'prediction_big_small': prediction.get('prediction_big_small'),
                'prediction_odd_even': prediction.get('prediction_odd_even'),
                'prediction_combo': prediction.get('prediction_combo'),
                'confidence': prediction.get('confidence'),
                'reasoning_summary': prediction.get('reasoning_summary'),
                'raw_response': raw_response,
                'prompt_snapshot': prompt_snapshot,
                'status': 'pending',
                'error_message': None,
                'actual_number': None,
                'actual_big_small': None,
                'actual_odd_even': None,
                'actual_combo': None,
                'hit_number': None,
                'hit_big_small': None,
                'hit_odd_even': None,
                'hit_combo': None,
                'settled_at': None
            }
        except Exception as exc:
            payload = {
                'predictor_id': predictor['id'],
                'lottery_type': predictor.get('lottery_type', 'pc28'),
                'issue_no': issue_no,
                'requested_targets': predictor['prediction_targets'],
                'prediction_number': None,
                'prediction_big_small': None,
                'prediction_odd_even': None,
                'prediction_combo': None,
                'confidence': None,
                'reasoning_summary': '',
                'raw_response': raw_response,
                'prompt_snapshot': prompt_snapshot,
                'status': 'failed',
                'error_message': str(exc),
                'actual_number': None,
                'actual_big_small': None,
                'actual_odd_even': None,
                'actual_combo': None,
                'hit_number': None,
                'hit_big_small': None,
                'hit_odd_even': None,
                'hit_combo': None,
                'settled_at': None
            }

        self.db.upsert_prediction(payload)
        return self.db.get_prediction_by_issue(predictor['id'], issue_no) or payload

    def _evaluate_hit(self, predicted_value, actual_value, targets: list[str], target_name: str):
        if target_name not in targets:
            return None
        if predicted_value is None:
            return None
        return 1 if predicted_value == actual_value else 0
