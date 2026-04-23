"""
通知全局配置与 Telegram 发送服务
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime

import requests

import config
from services.bet_strategy import build_bet_strategy_label
from services.notification_rule_engine import PERFORMANCE_EVENT_TYPE


NOTIFICATION_ENABLED_KEY = 'notifications.enabled'
TELEGRAM_BOT_TOKEN_KEY = 'notifications.telegram_bot_token'
TELEGRAM_BOT_NAME_KEY = 'notifications.telegram_bot_name'
DEFAULT_NOTIFICATION_ENABLED = False


class TelegramRateLimitError(Exception):
    def __init__(self, message: str, retry_after: int = 1):
        super().__init__(message)
        self.retry_after = max(1, int(retry_after or 1))


@dataclass
class NotificationSettings:
    enabled: bool = DEFAULT_NOTIFICATION_ENABLED
    telegram_bot_token: str = ''
    telegram_bot_name: str = ''

    def to_dict(self) -> dict:
        return {
            'enabled': bool(self.enabled),
            'telegram_bot_token': str(self.telegram_bot_token or ''),
            'telegram_bot_name': str(self.telegram_bot_name or '')
        }


class NotificationService:
    def __init__(self, db, timeout: int = 15):
        self.db = db
        self.timeout = max(3, int(timeout or 15))
        self._global_send_times = deque()
        self._last_chat_send_at: dict[str, float] = {}

    def get_settings(self) -> dict:
        raw_settings = self.db.get_system_settings([
            NOTIFICATION_ENABLED_KEY,
            TELEGRAM_BOT_TOKEN_KEY,
            TELEGRAM_BOT_NAME_KEY
        ])
        return NotificationSettings(
            enabled=self._parse_bool(raw_settings.get(NOTIFICATION_ENABLED_KEY), DEFAULT_NOTIFICATION_ENABLED),
            telegram_bot_token=str(raw_settings.get(TELEGRAM_BOT_TOKEN_KEY) or ''),
            telegram_bot_name=str(raw_settings.get(TELEGRAM_BOT_NAME_KEY) or '')
        ).to_dict()

    def update_settings(self, enabled: bool, telegram_bot_token: str, telegram_bot_name: str) -> dict:
        self.db.set_system_settings({
            NOTIFICATION_ENABLED_KEY: '1' if enabled else '0',
            TELEGRAM_BOT_TOKEN_KEY: str(telegram_bot_token or '').strip(),
            TELEGRAM_BOT_NAME_KEY: str(telegram_bot_name or '').strip()
        })
        return self.get_settings()

    def send_test_message(
        self,
        chat_id: str,
        text: str,
        sender_mode: str = 'platform',
        sender_account: dict | None = None,
        bot_token_override: str | None = None,
        existing_sender_id: int | None = None,
        user_id: int | None = None
    ) -> dict:
        if not str(chat_id or '').strip():
            raise ValueError('测试接收端标识不能为空')
        resolved_sender = self._resolve_sender_context(
            sender_mode=sender_mode,
            sender_account=sender_account,
            existing_sender_id=existing_sender_id,
            user_id=user_id,
            bot_token_override=bot_token_override
        )
        message_text = str(text or '').strip() or 'AITradingSimulator Telegram 测试消息'
        return self._send_telegram_message(
            bot_token=resolved_sender['bot_token'],
            chat_id=str(chat_id).strip(),
            text=message_text
        )

    def retry_delivery(self, delivery_id: int, user_id: int) -> dict:
        delivery = self.db.get_notification_delivery_by_id(delivery_id, user_id=user_id)
        if not delivery:
            raise ValueError('通知投递记录不存在')

        settings = self.get_settings()
        resolved_sender = self._resolve_sender_context(
            sender_mode=str(delivery.get('sender_mode') or 'platform'),
            sender_account=delivery if delivery.get('sender_mode') == 'user_sender' else None,
            existing_sender_id=int(delivery.get('sender_account_id') or 0) if delivery.get('sender_account_id') else None,
            user_id=user_id
        )
        if str(delivery.get('channel_type') or '').strip().lower() != 'telegram':
            raise ValueError('当前仅支持 Telegram 渠道重发')
        if not delivery.get('subscription_enabled'):
            raise ValueError('关联通知订阅已停用，无法重发')
        if str(delivery.get('endpoint_status') or '').strip().lower() != 'active':
            raise ValueError('通知接收端未启用，无法重发')

        payload = delivery.get('payload') or {}
        message_text = str(payload.get('message_text') or '').strip()
        if not message_text:
            message_text = self._build_message_text(delivery, payload, settings)

        response_payload = self._send_telegram_message(
            bot_token=resolved_sender['bot_token'],
            chat_id=str(delivery.get('endpoint_key') or '').strip(),
            text=message_text
        )
        update_payload = {
            'subscription_id': int(delivery['subscription_id']),
            'user_id': int(delivery['user_id']),
            'predictor_id': int(delivery['predictor_id']),
            'endpoint_id': int(delivery['endpoint_id']),
            'event_type': str(delivery.get('event_type') or delivery.get('subscription_event_type') or 'prediction_created'),
            'record_key': str(delivery.get('record_key') or ''),
            'status': 'delivered',
            'payload': {
                **payload,
                'message_text': message_text,
                'telegram_response': response_payload,
                'manual_retry': True
            },
            'error_message': None,
            'sent_at': self._utc_now_str()
        }
        self.db.upsert_notification_delivery(update_payload)
        return self.db.get_notification_delivery_by_id(delivery_id, user_id=user_id) or update_payload

    def notify_prediction_created(self, predictor: dict, prediction: dict, lottery_type: str, detail_builder=None) -> list[dict]:
        settings = self.get_settings()
        if not settings.get('enabled'):
            return []

        predictor_id = int(predictor.get('id') or 0)
        if predictor_id <= 0:
            return []

        subscriptions = self.db.list_active_notification_subscriptions_by_predictor(
            predictor_id,
            event_type='prediction_created'
        )
        if not subscriptions:
            return []

        event_payload = self._build_prediction_event_payload(
            predictor=predictor,
            prediction=prediction,
            lottery_type=lottery_type,
            detail_builder=detail_builder
        )
        record_key = str(event_payload.get('record_key') or '')
        if not record_key:
            return []

        results = []
        for subscription in subscriptions:
            result = self._enqueue_prediction_created(
                subscription=subscription,
                event_payload=event_payload
            )
            results.append(result)
        return results

    def notify_performance_threshold(self, predictor: dict, evaluation: dict) -> dict | None:
        settings = self.get_settings()
        if not settings.get('enabled'):
            return None
        if str((evaluation or {}).get('status') or '') != 'triggered':
            return None

        subscription = (evaluation or {}).get('subscription') or {}
        event_payload = (evaluation or {}).get('event_payload') or {}
        record_key = str(event_payload.get('record_key') or '').strip()
        if not subscription or not record_key:
            return None

        return self._enqueue_notification_event(
            subscription=subscription,
            event_payload={
                **event_payload,
                'event_type': PERFORMANCE_EVENT_TYPE
            },
            record_key=record_key
        )

    def build_delivery_payload(self, subscription: dict, event_payload: dict, record_key: str) -> dict:
        return {
            'subscription_id': int(subscription['id']),
            'user_id': int(subscription['user_id']),
            'predictor_id': int(subscription['predictor_id']),
            'endpoint_id': int(subscription['endpoint_id']),
            'event_type': str(subscription.get('event_type') or 'prediction_created'),
            'record_key': str(record_key or ''),
            'status': 'pending',
            'payload': event_payload or {},
            'error_message': None,
            'sent_at': None
        }

    def _enqueue_prediction_created(self, subscription: dict, event_payload: dict) -> dict:
        record_key = str(event_payload.get('record_key') or '')
        delivery = self.db.get_notification_delivery(
            int(subscription['id']),
            str(subscription.get('event_type') or 'prediction_created'),
            record_key
        )
        if delivery and delivery.get('status') == 'delivered':
            return delivery

        confidence_gte = self._parse_float((subscription.get('filter') or {}).get('confidence_gte'))
        confidence = self._parse_float(event_payload.get('confidence'))
        if confidence_gte is not None and confidence is not None and confidence < confidence_gte:
            payload = self.build_delivery_payload(subscription, event_payload, record_key)
            payload.update({
                'status': 'skipped',
                'error_message': f'置信度 {confidence:.2f} 低于阈值 {confidence_gte:.2f}'
            })
            delivery_id = self.db.upsert_notification_delivery(payload)
            return self.db.get_notification_delivery(payload['subscription_id'], payload['event_type'], payload['record_key']) or {
                **payload,
                'id': delivery_id
            }

        return self._enqueue_notification_event(subscription, event_payload, record_key)

    def _enqueue_notification_event(self, subscription: dict, event_payload: dict, record_key: str) -> dict:
        delivery = self.db.get_notification_delivery(
            int(subscription['id']),
            str(subscription.get('event_type') or event_payload.get('event_type') or 'prediction_created'),
            record_key
        )
        if delivery and delivery.get('status') == 'delivered':
            return delivery

        endpoint_key = str(subscription.get('endpoint_key') or '').strip()
        if not endpoint_key:
            payload = self.build_delivery_payload(subscription, event_payload, record_key)
            payload.update({
                'status': 'failed',
                'error_message': '通知接收端缺少 endpoint_key'
            })
            delivery_id = self.db.upsert_notification_delivery(payload)
            return self.db.get_notification_delivery(payload['subscription_id'], payload['event_type'], payload['record_key']) or {
                **payload,
                'id': delivery_id
            }

        if str(subscription.get('channel_type') or '').strip().lower() != 'telegram':
            payload = self.build_delivery_payload(subscription, event_payload, record_key)
            payload.update({
                'status': 'failed',
                'error_message': '当前仅支持 Telegram 渠道'
            })
            delivery_id = self.db.upsert_notification_delivery(payload)
            return self.db.get_notification_delivery(payload['subscription_id'], payload['event_type'], payload['record_key']) or {
                **payload,
                'id': delivery_id
            }
        payload = self.build_delivery_payload(subscription, event_payload, record_key)
        payload.update({'status': 'pending'})
        delivery_id = self.db.upsert_notification_delivery(payload)
        self.db.upsert_notification_delivery_job({
            'delivery_id': delivery_id,
            'subscription_id': payload['subscription_id'],
            'user_id': payload['user_id'],
            'predictor_id': payload['predictor_id'],
            'endpoint_id': payload['endpoint_id'],
            'sender_mode': subscription.get('sender_mode') or 'platform',
            'sender_account_id': subscription.get('sender_account_id'),
            'channel_type': subscription.get('channel_type') or 'telegram',
            'status': 'queued',
            'attempt_count': 0,
            'available_at': self._utc_now_str(),
            'locked_at': None,
            'last_error_message': None,
            'last_response': {}
        })
        return self.db.get_notification_delivery(payload['subscription_id'], payload['event_type'], payload['record_key']) or {
            **payload,
            'id': delivery_id
        }

    def process_delivery_jobs(self, limit: int | None = None) -> dict:
        batch_size = max(1, int(limit or config.NOTIFICATION_WORKER_BATCH_SIZE))
        jobs = self.db.claim_notification_delivery_jobs(limit=batch_size)
        processed = 0
        delivered = 0
        retrying = 0
        failed = 0
        for job in jobs:
            processed += 1
            result = self._process_delivery_job(job)
            status = str(result.get('status') or '').strip().lower()
            if status == 'delivered':
                delivered += 1
            elif status == 'retrying':
                retrying += 1
            elif status == 'failed':
                failed += 1
        return {
            'processed_count': processed,
            'delivered_count': delivered,
            'retrying_count': retrying,
            'failed_count': failed
        }

    def _process_delivery_job(self, job: dict) -> dict:
        delivery = self.db.get_notification_delivery_by_id(int(job['delivery_id']))
        if not delivery:
            self.db.update_notification_delivery_job(int(job['id']), {
                'status': 'failed',
                'last_error_message': '关联投递记录不存在'
            })
            return {'status': 'failed'}

        payload = delivery.get('payload') or {}
        try:
            resolved_sender = self._resolve_sender_context(
                sender_mode=str(job.get('sender_mode') or 'platform'),
                sender_account=delivery if str(job.get('sender_mode') or 'platform') == 'user_sender' else None,
                existing_sender_id=int(job.get('sender_account_id') or 0) if job.get('sender_account_id') else None,
                user_id=int(delivery.get('user_id') or 0)
            )
            settings = self.get_settings()
            message_text = str(payload.get('message_text') or '').strip()
            if not message_text:
                message_text = self._build_message_text(delivery, payload, settings, resolved_sender)
            chat_id = str(delivery.get('endpoint_key') or '').strip()
            self._respect_rate_limits(chat_id)
            response_payload = self._send_telegram_message(
                bot_token=resolved_sender['bot_token'],
                chat_id=chat_id,
                text=message_text
            )
            self.db.upsert_notification_delivery({
                'subscription_id': int(delivery['subscription_id']),
                'user_id': int(delivery['user_id']),
                'predictor_id': int(delivery['predictor_id']),
                'endpoint_id': int(delivery['endpoint_id']),
                'event_type': str(delivery.get('event_type') or delivery.get('subscription_event_type') or 'prediction_created'),
                'record_key': str(delivery.get('record_key') or ''),
                'status': 'delivered',
                'payload': {
                    **payload,
                    'message_text': message_text,
                    'telegram_response': response_payload
                },
                'error_message': None,
                'sent_at': self._utc_now_str()
            })
            self.db.update_notification_delivery_job(int(job['id']), {
                'status': 'delivered',
                'locked_at': None,
                'last_error_message': None,
                'last_response': response_payload
            })
            return {'status': 'delivered'}
        except TelegramRateLimitError as exc:
            next_time = self._utc_after_seconds(exc.retry_after)
            self.db.upsert_notification_delivery({
                'subscription_id': int(delivery['subscription_id']),
                'user_id': int(delivery['user_id']),
                'predictor_id': int(delivery['predictor_id']),
                'endpoint_id': int(delivery['endpoint_id']),
                'event_type': str(delivery.get('event_type') or delivery.get('subscription_event_type') or 'prediction_created'),
                'record_key': str(delivery.get('record_key') or ''),
                'status': 'pending',
                'payload': payload,
                'error_message': str(exc),
                'sent_at': delivery.get('sent_at')
            })
            self.db.update_notification_delivery_job(int(job['id']), {
                'status': 'retrying',
                'locked_at': None,
                'available_at': next_time,
                'last_error_message': str(exc)
            })
            return {'status': 'retrying'}
        except Exception as exc:
            attempt_count = int(job.get('attempt_count') or 1)
            max_attempts = max(1, int(config.NOTIFICATION_MAX_RETRY_ATTEMPTS))
            if attempt_count >= max_attempts:
                self.db.upsert_notification_delivery({
                    'subscription_id': int(delivery['subscription_id']),
                    'user_id': int(delivery['user_id']),
                    'predictor_id': int(delivery['predictor_id']),
                    'endpoint_id': int(delivery['endpoint_id']),
                    'event_type': str(delivery.get('event_type') or delivery.get('subscription_event_type') or 'prediction_created'),
                    'record_key': str(delivery.get('record_key') or ''),
                    'status': 'failed',
                    'payload': payload,
                    'error_message': str(exc),
                    'sent_at': delivery.get('sent_at')
                })
                self.db.update_notification_delivery_job(int(job['id']), {
                    'status': 'failed',
                    'locked_at': None,
                    'last_error_message': str(exc)
                })
                return {'status': 'failed'}

            retry_seconds = min(
                max(1, int(config.NOTIFICATION_RETRY_BASE_SECONDS)) * (2 ** max(0, attempt_count - 1)),
                max(1, int(config.NOTIFICATION_RETRY_MAX_SECONDS))
            )
            self.db.upsert_notification_delivery({
                'subscription_id': int(delivery['subscription_id']),
                'user_id': int(delivery['user_id']),
                'predictor_id': int(delivery['predictor_id']),
                'endpoint_id': int(delivery['endpoint_id']),
                'event_type': str(delivery.get('event_type') or delivery.get('subscription_event_type') or 'prediction_created'),
                'record_key': str(delivery.get('record_key') or ''),
                'status': 'pending',
                'payload': payload,
                'error_message': str(exc),
                'sent_at': delivery.get('sent_at')
            })
            self.db.update_notification_delivery_job(int(job['id']), {
                'status': 'retrying',
                'locked_at': None,
                'available_at': self._utc_after_seconds(retry_seconds),
                'last_error_message': str(exc)
            })
            return {'status': 'retrying'}

    def _build_prediction_event_payload(self, predictor: dict, prediction: dict, lottery_type: str, detail_builder=None) -> dict:
        normalized_lottery = str(lottery_type or predictor.get('lottery_type') or 'pc28').strip().lower()
        if normalized_lottery == 'jingcai_football':
            detail = detail_builder(prediction) if callable(detail_builder) else prediction
            run_key = str((detail or {}).get('run_key') or prediction.get('run_key') or '')
            items = list((detail or {}).get('items') or [])
            top_items = items[:3]
            return {
                'record_key': run_key,
                'lottery_type': normalized_lottery,
                'predictor_id': predictor.get('id'),
                'predictor_name': predictor.get('name'),
                'title': str((detail or {}).get('title') or f'{run_key} 竞彩足球批次预测').strip(),
                'confidence': prediction.get('confidence'),
                'summary': str((detail or {}).get('reasoning_summary') or '').strip(),
                'top_items': [
                    {
                        'issue_no': item.get('issue_no'),
                        'title': item.get('title'),
                        'prediction_payload': item.get('prediction_payload') or {},
                        'confidence': item.get('confidence')
                    }
                    for item in top_items
                ]
            }

        issue_no = str(prediction.get('issue_no') or '').strip()
        return {
            'record_key': issue_no,
            'lottery_type': normalized_lottery,
            'predictor_id': predictor.get('id'),
            'predictor_name': predictor.get('name'),
            'title': f"{predictor.get('name') or '预测方案'} · 第 {issue_no} 期",
            'confidence': prediction.get('confidence'),
            'prediction_number': prediction.get('prediction_number'),
            'prediction_big_small': prediction.get('prediction_big_small'),
            'prediction_odd_even': prediction.get('prediction_odd_even'),
            'prediction_combo': prediction.get('prediction_combo'),
            'summary': str(prediction.get('reasoning_summary') or '').strip()
        }

    def _build_message_text(self, subscription: dict, event_payload: dict, settings: dict, sender_context: dict | None = None) -> str:
        bot_name = str((sender_context or {}).get('bot_name') or settings.get('telegram_bot_name') or 'AITradingSimulator').strip() or 'AITradingSimulator'
        event_type = str(event_payload.get('event_type') or subscription.get('event_type') or 'prediction_created').strip().lower()
        title = '表现告警' if event_type == PERFORMANCE_EVENT_TYPE else '新预测通知'
        lines = [f'[{bot_name}] {title}']
        lottery_type = str(event_payload.get('lottery_type') or '').strip().lower()
        lines.append(f"方案：{event_payload.get('predictor_name') or '--'}")

        if event_type == PERFORMANCE_EVENT_TYPE:
            lines.append(f"规则：{event_payload.get('rule_name') or event_payload.get('rule_id') or '--'}")
            lines.append(f"玩法：{event_payload.get('metric_label') or event_payload.get('metric') or '--'}")
            lines.append(
                f"窗口：最近 {event_payload.get('window_size') or '--'} 期，"
                f"样本 {event_payload.get('sample_count') or 0} 条，"
                f"命中 {event_payload.get('hit_count') or 0} 条"
            )
            lines.append(
                f"命中率：{self._format_percent(event_payload.get('hit_rate'))}，"
                f"{event_payload.get('operator_label') or '--'} {self._format_percent(event_payload.get('threshold'))}"
            )
            lines.append(f"最新结算期号：{event_payload.get('latest_issue') or '--'}")
            lines.append(
                f"断档：最大 {event_payload.get('max_missing_count') or 0} 期，"
                f"累计 {event_payload.get('total_missing_count') or 0} 期"
            )
        elif lottery_type == 'jingcai_football':
            lines.append(f"批次：{event_payload.get('record_key') or '--'}")
            top_items = event_payload.get('top_items') or []
            if top_items:
                lines.append('推荐场次：')
                for item in top_items:
                    prediction_payload = item.get('prediction_payload') or {}
                    spf = prediction_payload.get('spf') or '--'
                    rqspf = prediction_payload.get('rqspf') or '--'
                    confidence = self._format_confidence(item.get('confidence'))
                    lines.append(
                        f"- {item.get('issue_no') or '--'} {item.get('title') or '--'} | 胜平负 {spf} | 让球胜平负 {rqspf} | 置信度 {confidence}"
                    )
            else:
                lines.append(f"标题：{event_payload.get('title') or '--'}")
            summary = str(event_payload.get('summary') or '').strip()
            if summary:
                lines.append(f"摘要：{summary}")
        else:
            lines.append(f"期号：{event_payload.get('record_key') or '--'}")
            lines.append(
                "预测："
                f" 单点 {self._display_value(event_payload.get('prediction_number'))}"
                f" | 大小 {self._display_value(event_payload.get('prediction_big_small'))}"
                f" | 单双 {self._display_value(event_payload.get('prediction_odd_even'))}"
                f" | 组合 {self._display_value(event_payload.get('prediction_combo'))}"
            )
            lines.append(f"置信度：{self._format_confidence(event_payload.get('confidence'))}")
            summary = str(event_payload.get('summary') or '').strip()
            if summary:
                lines.append(f"摘要：{summary}")

        if str(subscription.get('delivery_mode') or '') == 'follow_bet' and subscription.get('bet_profile_name'):
            lines.append(
                f"下注策略：{subscription.get('bet_profile_name')} · "
                f"{build_bet_strategy_label(self._subscription_bet_strategy(subscription))}"
            )

        return '\n'.join(lines)

    def _subscription_bet_strategy(self, subscription: dict) -> dict:
        return {
            'mode': subscription.get('bet_profile_mode'),
            'base_stake': subscription.get('bet_profile_base_stake'),
            'multiplier': subscription.get('bet_profile_multiplier'),
            'max_steps': subscription.get('bet_profile_max_steps')
        }

    def _respect_rate_limits(self, chat_id: str):
        global_rate = max(1.0, float(config.NOTIFICATION_GLOBAL_MESSAGES_PER_SECOND))
        min_chat_interval = max(0.0, float(config.NOTIFICATION_CHAT_MIN_INTERVAL_SECONDS))
        now = time.monotonic()
        cutoff = now - 1.0
        while self._global_send_times and self._global_send_times[0] < cutoff:
            self._global_send_times.popleft()
        if len(self._global_send_times) >= int(global_rate):
            sleep_seconds = max(0.0, 1.0 - (now - self._global_send_times[0]))
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        last_chat_at = self._last_chat_send_at.get(str(chat_id))
        if last_chat_at is not None:
            remaining = min_chat_interval - (time.monotonic() - last_chat_at)
            if remaining > 0:
                time.sleep(remaining)

    def _send_telegram_message(self, bot_token: str, chat_id: str, text: str) -> dict:
        response = requests.post(
            f'https://api.telegram.org/bot{bot_token}/sendMessage',
            json={
                'chat_id': chat_id,
                'text': text
            },
            timeout=self.timeout
        )
        if response.status_code == 429:
            try:
                payload = response.json()
            except Exception:
                payload = {}
            retry_after = (
                ((payload.get('parameters') or {}).get('retry_after'))
                or payload.get('retry_after')
                or 1
            )
            raise TelegramRateLimitError(
                str(payload.get('description') or 'Telegram 限流'),
                retry_after=int(retry_after)
            )
        response.raise_for_status()
        payload = response.json()
        if not payload.get('ok'):
            raise ValueError(str(payload.get('description') or 'Telegram 发送失败'))
        now = time.monotonic()
        self._global_send_times.append(now)
        self._last_chat_send_at[str(chat_id)] = now
        return payload

    def _resolve_sender_context(
        self,
        sender_mode: str,
        sender_account: dict | None = None,
        existing_sender_id: int | None = None,
        user_id: int | None = None,
        bot_token_override: str | None = None
    ) -> dict:
        normalized_mode = str(sender_mode or 'platform').strip().lower() or 'platform'
        if normalized_mode == 'user_sender':
            resolved_sender = dict(sender_account or {})
            if existing_sender_id and not resolved_sender.get('bot_token') and not resolved_sender.get('sender_bot_token'):
                resolved_sender = self.db.get_notification_sender_account(existing_sender_id) or {}
            if not resolved_sender:
                raise ValueError('通知发送方不存在')
            if user_id is not None and resolved_sender.get('user_id') is not None and int(resolved_sender.get('user_id') or 0) != int(user_id):
                raise ValueError('通知发送方不存在或无权访问')
            sender_status = resolved_sender.get('sender_account_status')
            if sender_status is None:
                sender_status = resolved_sender.get('status')
            if sender_status is not None and str(sender_status or '').strip().lower() != 'active':
                raise ValueError('通知发送方未启用')
            bot_token = str(
                bot_token_override
                or resolved_sender.get('bot_token')
                or resolved_sender.get('sender_bot_token')
                or ''
            ).strip()
            if not bot_token:
                raise ValueError('用户机器人缺少 Bot Token')
            return {
                'mode': 'user_sender',
                'bot_token': bot_token,
                'bot_name': str(
                    resolved_sender.get('bot_name')
                    or resolved_sender.get('sender_bot_name')
                    or resolved_sender.get('sender_name')
                    or resolved_sender.get('sender_account_name')
                    or ''
                ).strip(),
                'sender_name': str(
                    resolved_sender.get('sender_name')
                    or resolved_sender.get('sender_account_name')
                    or resolved_sender.get('bot_name')
                    or resolved_sender.get('sender_bot_name')
                    or ''
                ).strip()
            }

        settings = self.get_settings()
        bot_token = str(bot_token_override or settings.get('telegram_bot_token') or '').strip()
        if not bot_token:
            raise ValueError('平台尚未配置 Telegram Bot Token')
        return {
            'mode': 'platform',
            'bot_token': bot_token,
            'bot_name': str(settings.get('telegram_bot_name') or '').strip(),
            'sender_name': str(settings.get('telegram_bot_name') or '').strip() or '平台机器人'
        }

    def _format_confidence(self, value) -> str:
        parsed = self._parse_float(value)
        if parsed is None:
            return '--'
        return f'{parsed * 100:.1f}%'

    def _format_percent(self, value) -> str:
        parsed = self._parse_float(value)
        if parsed is None:
            return '--'
        return f'{parsed:.2f}%'.replace('.00%', '%')

    def _display_value(self, value) -> str:
        if value in {None, ''}:
            return '--'
        return str(value)

    def _parse_bool(self, value, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}

    def _parse_float(self, value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _utc_now_str(self) -> str:
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    def _utc_after_seconds(self, seconds: int) -> str:
        return datetime.utcfromtimestamp(time.time() + max(1, int(seconds or 1))).strftime('%Y-%m-%d %H:%M:%S')
