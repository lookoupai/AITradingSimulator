from __future__ import annotations

import json
import unittest
from unittest.mock import Mock, patch

from tests.support import create_predictor, fresh_app_harness


class NotificationServiceTests(unittest.TestCase):
    def test_pc28_prediction_notification_is_delivered_once(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'pc28',
                name='PC28 通知方案',
                prediction_targets=['big_small'],
                primary_metric='big_small',
                profit_default_metric='big_small'
            )
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            endpoint_id = harness.db.create_notification_endpoint(
                user_id=user_id,
                channel_type='telegram',
                endpoint_key='123456789',
                endpoint_label='我的 TG',
                config={'chat_type': 'private'},
                status='active',
                is_default=True
            )
            harness.db.create_notification_subscription(
                user_id=user_id,
                predictor_id=predictor_id,
                endpoint_id=endpoint_id,
                sender_mode='platform',
                sender_account_id=None,
                bet_profile_id=None,
                event_type='prediction_created',
                delivery_mode='notify_only',
                filters={},
                enabled=True
            )
            harness.module.notification_service.update_settings(
                enabled=True,
                telegram_bot_token='123456:abcdef',
                telegram_bot_name='predictor_bot'
            )

            prediction = {
                'predictor_id': predictor_id,
                'lottery_type': 'pc28',
                'issue_no': '20260411001',
                'requested_targets': ['big_small'],
                'prediction_big_small': '大',
                'confidence': 0.88,
                'reasoning_summary': '测试',
                'status': 'pending'
            }

            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = {'ok': True, 'result': {'message_id': 1001}}

            with patch('services.notification_service.requests.post', return_value=response) as mocked_post:
                first_results = harness.module.notification_service.notify_prediction_created(
                    predictor=predictor,
                    prediction=prediction,
                    lottery_type='pc28'
                )
                process_result = harness.module.notification_service.process_delivery_jobs()
                second_results = harness.module.notification_service.notify_prediction_created(
                    predictor=predictor,
                    prediction=prediction,
                    lottery_type='pc28'
                )
                second_process_result = harness.module.notification_service.process_delivery_jobs()

            self.assertEqual(mocked_post.call_count, 1)
            self.assertEqual(first_results[0]['status'], 'pending')
            self.assertEqual(process_result['delivered_count'], 1)
            self.assertEqual(second_results[0]['status'], 'delivered')
            self.assertEqual(second_process_result['delivered_count'], 0)
            deliveries = harness.db.list_notification_deliveries(user_id)
            self.assertEqual(len(deliveries), 1)
            self.assertEqual(deliveries[0]['status'], 'delivered')

    def test_prediction_notification_respects_confidence_filter(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'pc28', prediction_targets=['big_small'])
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            endpoint_id = harness.db.create_notification_endpoint(
                user_id=user_id,
                channel_type='telegram',
                endpoint_key='123456789',
                endpoint_label='我的 TG',
                config={'chat_type': 'private'},
                status='active',
                is_default=True
            )
            harness.db.create_notification_subscription(
                user_id=user_id,
                predictor_id=predictor_id,
                endpoint_id=endpoint_id,
                sender_mode='platform',
                sender_account_id=None,
                bet_profile_id=None,
                event_type='prediction_created',
                delivery_mode='notify_only',
                filters={'confidence_gte': 0.9},
                enabled=True
            )
            harness.module.notification_service.update_settings(
                enabled=True,
                telegram_bot_token='123456:abcdef',
                telegram_bot_name='predictor_bot'
            )

            with patch('services.notification_service.requests.post') as mocked_post:
                results = harness.module.notification_service.notify_prediction_created(
                    predictor=predictor,
                    prediction={
                        'predictor_id': predictor_id,
                        'lottery_type': 'pc28',
                        'issue_no': '20260411002',
                        'requested_targets': ['big_small'],
                        'prediction_big_small': '小',
                        'confidence': 0.65,
                        'status': 'pending'
                    },
                    lottery_type='pc28'
                )
                process_result = harness.module.notification_service.process_delivery_jobs()

            self.assertEqual(mocked_post.call_count, 0)
            self.assertEqual(results[0]['status'], 'skipped')
            self.assertEqual(process_result['processed_count'], 0)
            deliveries = harness.db.list_notification_deliveries(user_id)
            self.assertEqual(deliveries[0]['status'], 'skipped')

    def test_prediction_notification_can_use_user_sender_account(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'pc28', prediction_targets=['big_small'])
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            endpoint_id = harness.db.create_notification_endpoint(
                user_id=user_id,
                channel_type='telegram',
                endpoint_key='123456789',
                endpoint_label='我的 TG',
                config={'chat_type': 'private'},
                status='active',
                is_default=True
            )
            sender_id = harness.db.create_notification_sender_account(
                user_id=user_id,
                channel_type='telegram',
                sender_name='我的机器人',
                bot_name='my_fast_bot',
                bot_token='999999:sender-token',
                status='active',
                is_default=True
            )
            harness.db.create_notification_subscription(
                user_id=user_id,
                predictor_id=predictor_id,
                endpoint_id=endpoint_id,
                sender_mode='user_sender',
                sender_account_id=sender_id,
                bet_profile_id=None,
                event_type='prediction_created',
                delivery_mode='notify_only',
                filters={},
                enabled=True
            )
            harness.module.notification_service.update_settings(
                enabled=True,
                telegram_bot_token='123456:platform-token',
                telegram_bot_name='predictor_bot'
            )

            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = {'ok': True, 'result': {'message_id': 1002}}

            with patch('services.notification_service.requests.post', return_value=response) as mocked_post:
                results = harness.module.notification_service.notify_prediction_created(
                    predictor=predictor,
                    prediction={
                        'predictor_id': predictor_id,
                        'lottery_type': 'pc28',
                        'issue_no': '20260411004',
                        'requested_targets': ['big_small'],
                        'prediction_big_small': '大',
                        'confidence': 0.91,
                        'status': 'pending'
                    },
                    lottery_type='pc28'
                )
                process_result = harness.module.notification_service.process_delivery_jobs()

            self.assertEqual(results[0]['status'], 'pending')
            self.assertEqual(process_result['delivered_count'], 1)
            self.assertEqual(mocked_post.call_args.kwargs['json']['chat_id'], '123456789')
            self.assertIn('999999:sender-token', mocked_post.call_args.args[0])

    def test_football_prediction_notification_uses_run_view_model(self):
        with fresh_app_harness() as harness:
            _, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'jingcai_football',
                name='竞彩足球通知方案',
                prediction_targets=['spf', 'rqspf'],
                primary_metric='spf',
                profit_default_metric='spf'
            )
            predictor = harness.db.get_predictor(predictor_id, include_secret=True)
            endpoint_id = harness.db.create_notification_endpoint(
                user_id=user_id,
                channel_type='telegram',
                endpoint_key='@demo_channel',
                endpoint_label='竞彩频道',
                config={'chat_type': 'channel'},
                status='active',
                is_default=True
            )
            harness.db.create_notification_subscription(
                user_id=user_id,
                predictor_id=predictor_id,
                endpoint_id=endpoint_id,
                sender_mode='platform',
                sender_account_id=None,
                bet_profile_id=None,
                event_type='prediction_created',
                delivery_mode='notify_only',
                filters={},
                enabled=True
            )
            harness.module.notification_service.update_settings(
                enabled=True,
                telegram_bot_token='123456:abcdef',
                telegram_bot_name='predictor_bot'
            )

            harness.db.upsert_lottery_events([{
                'lottery_type': 'jingcai_football',
                'event_key': 'event-1',
                'batch_key': '2026-04-11',
                'event_date': '2026-04-11',
                'event_time': '2026-04-11 19:30:00',
                'event_name': '主队A vs 客队A',
                'league': '测试联赛',
                'home_team': '主队A',
                'away_team': '客队A',
                'status': '1',
                'status_label': '已开售',
                'source_provider': 'sina',
                'result_payload': '{}',
                'meta_payload': json.dumps({
                    'match_no': '周六001',
                    'spf_sell_status': '2',
                    'rqspf_sell_status': '1',
                    'spf_odds': {'胜': 1.8, '平': 3.2, '负': 4.2},
                    'rqspf': {'handicap': -1, 'handicap_text': '-1', 'odds': {'胜': 4.1, '平': 3.4, '负': 1.7}},
                    'settled': False
                }, ensure_ascii=False),
                'source_payload': '{}'
            }])
            run_id = harness.db.upsert_prediction_run({
                'predictor_id': predictor_id,
                'lottery_type': 'jingcai_football',
                'run_key': '2026-04-11',
                'title': '2026-04-11 竞彩足球批次预测',
                'requested_targets': ['spf', 'rqspf'],
                'status': 'pending',
                'total_items': 1,
                'settled_items': 0,
                'hit_items': 0,
                'confidence': 0.74,
                'reasoning_summary': ''
            })
            harness.db.upsert_prediction_items([{
                'run_id': run_id,
                'predictor_id': predictor_id,
                'lottery_type': 'jingcai_football',
                'run_key': '2026-04-11',
                'event_key': 'event-1',
                'item_order': 0,
                'issue_no': '周六001',
                'title': '主队A vs 客队A',
                'requested_targets': ['spf', 'rqspf'],
                'prediction_payload': {'spf': '胜', 'rqspf': '平'},
                'actual_payload': {},
                'hit_payload': {},
                'confidence': 0.74,
                'reasoning_summary': 'test',
                'raw_response': '{}',
                'status': 'pending',
                'error_message': None,
                'settled_at': None
            }])
            run = harness.db.get_prediction_run(run_id)

            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = {'ok': True, 'result': {'message_id': 2001}}

            with patch('services.notification_service.requests.post', return_value=response) as mocked_post:
                results = harness.module.notification_service.notify_prediction_created(
                    predictor=predictor,
                    prediction=run,
                    lottery_type='jingcai_football',
                    detail_builder=lambda saved_run: harness.module.jingcai_football_service.build_run_view_model(harness.db, saved_run)
                )
                process_result = harness.module.notification_service.process_delivery_jobs()

            self.assertEqual(mocked_post.call_count, 1)
            self.assertEqual(results[0]['status'], 'pending')
            self.assertEqual(process_result['delivered_count'], 1)
            deliveries = harness.db.list_notification_deliveries(user_id)
            self.assertEqual(len(deliveries), 1)
            self.assertEqual(deliveries[0]['status'], 'delivered')


if __name__ == '__main__':
    unittest.main()
