from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from tests.support import create_predictor, fresh_app_harness


class UserSettingsApiTests(unittest.TestCase):
    def test_dashboard_and_settings_pages_split_user_settings_sections(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'pc28')

            dashboard_response = client.get('/dashboard')
            settings_response = client.get('/settings')
            history_response = client.get(f'/predictors/{predictor_id}/history?tab=ai')
            history_api_response = client.get(f'/api/predictors/{predictor_id}/history')
            guest_settings_response = harness.app.test_client().get('/settings')

            self.assertEqual(dashboard_response.status_code, 200)
            self.assertEqual(settings_response.status_code, 200)
            self.assertEqual(history_response.status_code, 200)
            self.assertEqual(history_api_response.status_code, 200)
            self.assertEqual(guest_settings_response.status_code, 302)
            self.assertIn('/login', guest_settings_response.headers.get('Location', ''))

            dashboard_html = dashboard_response.get_data(as_text=True)
            settings_html = settings_response.get_data(as_text=True)
            history_html = history_response.get_data(as_text=True)
            history_payload = history_api_response.get_json()

            self.assertIn('通知与自动化入口', dashboard_html)
            self.assertIn('通知订阅', dashboard_html)
            self.assertIn('当前方案订阅数', dashboard_html)
            self.assertIn('查看全部', dashboard_html)
            self.assertNotIn('保存通知发送方', dashboard_html)
            self.assertNotIn('保存通知接收端', dashboard_html)
            self.assertNotIn('最近通知投递', dashboard_html)

            self.assertIn('通知与下注设置', settings_html)
            self.assertIn('通知发送方', settings_html)
            self.assertIn('通知接收端', settings_html)
            self.assertIn('最近通知投递', settings_html)

            self.assertIn('全部预测记录', history_html)
            self.assertIn('全部官方开奖', history_html)
            self.assertIn('全部 AI 原始输出', history_html)
            self.assertEqual(history_payload['predictor']['id'], predictor_id)

    def test_bet_profile_crud_and_simulation_supports_bet_profile(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(
                harness,
                user_id,
                'pc28',
                primary_metric='big_small',
                profit_default_metric='big_small',
                prediction_targets=['big_small']
            )

            create_response = client.post(
                '/api/bet-profiles',
                json={
                    'name': '倍投方案',
                    'lottery_type': 'pc28',
                    'mode': 'martingale',
                    'base_stake': 12,
                    'multiplier': 2,
                    'max_steps': 5,
                    'is_default': True
                }
            )
            create_payload = create_response.get_json()

            self.assertEqual(create_response.status_code, 200)
            self.assertEqual(create_payload['item']['mode'], 'martingale')
            self.assertEqual(create_payload['item']['base_stake'], 12.0)
            profile_id = create_payload['item']['id']

            list_response = client.get('/api/bet-profiles')
            list_payload = list_response.get_json()
            self.assertEqual(list_response.status_code, 200)
            self.assertEqual(len(list_payload), 1)
            self.assertTrue(list_payload[0]['is_default'])

            simulation_response = client.get(f'/api/predictors/{predictor_id}/simulation?bet_profile_id={profile_id}')
            simulation_payload = simulation_response.get_json()
            self.assertEqual(simulation_response.status_code, 200)
            self.assertEqual(simulation_payload['bet_profile']['id'], profile_id)
            self.assertEqual(simulation_payload['simulation']['bet_mode'], 'martingale')
            self.assertEqual(simulation_payload['simulation']['bet_config']['base_stake'], 12.0)
            self.assertEqual(simulation_payload['simulation']['bet_config']['max_steps'], 5)

            update_response = client.put(
                f'/api/bet-profiles/{profile_id}',
                json={
                    'name': '均注方案',
                    'mode': 'flat',
                    'base_stake': 20,
                    'is_default': True
                }
            )
            update_payload = update_response.get_json()
            self.assertEqual(update_response.status_code, 200)
            self.assertEqual(update_payload['item']['mode'], 'flat')
            self.assertEqual(update_payload['item']['base_stake'], 20.0)

            delete_response = client.delete(f'/api/bet-profiles/{profile_id}')
            self.assertEqual(delete_response.status_code, 200)
            self.assertEqual(client.get('/api/bet-profiles').get_json(), [])

    def test_notification_endpoint_and_subscription_crud(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'jingcai_football')

            endpoint_response = client.post(
                '/api/notification-endpoints',
                json={
                    'channel_type': 'telegram',
                    'endpoint_key': '123456789',
                    'endpoint_label': '我的 Telegram',
                    'config': {'chat_type': 'private'},
                    'is_default': True
                }
            )
            endpoint_payload = endpoint_response.get_json()
            self.assertEqual(endpoint_response.status_code, 200)
            endpoint_id = endpoint_payload['item']['id']

            bet_profile_response = client.post(
                '/api/bet-profiles',
                json={
                    'name': '竞彩足球均注',
                    'lottery_type': 'jingcai_football',
                    'mode': 'flat',
                    'base_stake': 30
                }
            )
            bet_profile_payload = bet_profile_response.get_json()
            self.assertEqual(bet_profile_response.status_code, 200)
            bet_profile_id = bet_profile_payload['item']['id']

            subscription_response = client.post(
                '/api/notification-subscriptions',
                json={
                    'predictor_id': predictor_id,
                    'endpoint_id': endpoint_id,
                    'bet_profile_id': bet_profile_id,
                    'event_type': 'prediction_created',
                    'delivery_mode': 'follow_bet',
                    'filter': {'confidence_gte': 0.6}
                }
            )
            subscription_payload = subscription_response.get_json()
            self.assertEqual(subscription_response.status_code, 200)
            self.assertEqual(subscription_payload['item']['delivery_mode'], 'follow_bet')
            self.assertEqual(subscription_payload['item']['bet_profile_id'], bet_profile_id)
            subscription_id = subscription_payload['item']['id']

            list_response = client.get('/api/notification-subscriptions')
            list_payload = list_response.get_json()
            self.assertEqual(list_response.status_code, 200)
            self.assertEqual(len(list_payload), 1)
            self.assertEqual(list_payload[0]['predictor_id'], predictor_id)
            self.assertEqual(list_payload[0]['endpoint_id'], endpoint_id)

            update_response = client.put(
                f'/api/notification-subscriptions/{subscription_id}',
                json={
                    'delivery_mode': 'notify_only',
                    'bet_profile_id': None,
                    'enabled': False
                }
            )
            update_payload = update_response.get_json()
            self.assertEqual(update_response.status_code, 200)
            self.assertEqual(update_payload['item']['delivery_mode'], 'notify_only')
            self.assertIsNone(update_payload['item']['bet_profile_id'])
            self.assertFalse(update_payload['item']['enabled'])

            self.assertEqual(client.get('/api/notification-deliveries').get_json(), [])

            delete_response = client.delete(f'/api/notification-subscriptions/{subscription_id}')
            self.assertEqual(delete_response.status_code, 200)
            self.assertEqual(client.get('/api/notification-subscriptions').get_json(), [])

    def test_notification_sender_crud_and_test_route(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()

            create_response = client.post(
                '/api/notification-senders',
                json={
                    'channel_type': 'telegram',
                    'sender_name': '我的快彩 Bot',
                    'bot_name': 'my_fast_bot',
                    'bot_token': '999999:sender-token',
                    'is_default': True
                }
            )
            create_payload = create_response.get_json()
            self.assertEqual(create_response.status_code, 200)
            sender_id = create_payload['item']['id']

            list_response = client.get('/api/notification-senders')
            list_payload = list_response.get_json()
            self.assertEqual(list_response.status_code, 200)
            self.assertEqual(len(list_payload), 1)
            self.assertEqual(list_payload[0]['sender_name'], '我的快彩 Bot')

            response_payload = {'ok': True, 'result': {'message_id': 789}}
            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = response_payload
            with patch('services.notification_service.requests.post', return_value=response) as mocked_post:
                test_response = client.post(
                    '/api/notification-senders/test',
                    json={
                        'sender_id': sender_id,
                        'chat_id': '123456789',
                        'message': '用户发送方测试'
                    }
                )
            test_payload = test_response.get_json()
            self.assertEqual(test_response.status_code, 200)
            self.assertEqual(test_payload['message'], '测试消息发送成功')
            self.assertEqual(mocked_post.call_count, 1)

            update_response = client.put(
                f'/api/notification-senders/{sender_id}',
                json={
                    'sender_name': '我的备用 Bot',
                    'bot_name': 'my_backup_bot',
                    'bot_token': '',
                    'status': 'active'
                }
            )
            update_payload = update_response.get_json()
            self.assertEqual(update_response.status_code, 200)
            self.assertEqual(update_payload['item']['sender_name'], '我的备用 Bot')

            delete_response = client.delete(f'/api/notification-senders/{sender_id}')
            self.assertEqual(delete_response.status_code, 200)
            self.assertEqual(client.get('/api/notification-senders').get_json(), [])

    def test_admin_notification_settings_crud(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client(username='admin', is_admin=True)

            update_response = client.post(
                '/api/admin/settings/notifications',
                json={
                    'enabled': True,
                    'telegram_bot_token': '123456:abcdef',
                    'telegram_bot_name': 'predictor_bot'
                }
            )
            update_payload = update_response.get_json()
            self.assertEqual(update_response.status_code, 200)
            self.assertTrue(update_payload['settings']['enabled'])
            self.assertEqual(update_payload['settings']['telegram_bot_name'], 'predictor_bot')

            get_response = client.get('/api/admin/settings/notifications')
            get_payload = get_response.get_json()
            self.assertEqual(get_response.status_code, 200)
            self.assertTrue(get_payload['settings']['enabled'])
            self.assertEqual(get_payload['settings']['telegram_bot_name'], 'predictor_bot')
            self.assertTrue(get_payload['settings']['telegram_bot_token_masked'])

            keep_token_response = client.post(
                '/api/admin/settings/notifications',
                json={
                    'enabled': False,
                    'telegram_bot_token': '',
                    'telegram_bot_name': 'predictor_bot_v2'
                }
            )
            keep_token_payload = keep_token_response.get_json()
            self.assertEqual(keep_token_response.status_code, 200)
            self.assertFalse(keep_token_payload['settings']['enabled'])
            self.assertEqual(keep_token_payload['settings']['telegram_bot_name'], 'predictor_bot_v2')
            self.assertEqual(keep_token_payload['settings']['telegram_bot_token'], '123456:abcdef')

    def test_admin_notification_test_route(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client(username='admin', is_admin=True)
            client.post(
                '/api/admin/settings/notifications',
                json={
                    'enabled': True,
                    'telegram_bot_token': '123456:abcdef',
                    'telegram_bot_name': 'predictor_bot'
                }
            )

            response_payload = {'ok': True, 'result': {'message_id': 123}}
            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = response_payload

            with patch('services.notification_service.requests.post', return_value=response) as mocked_post:
                test_response = client.post(
                    '/api/admin/settings/notifications/test',
                    json={
                        'chat_id': '123456789',
                        'message': '后台测试消息'
                    }
                )
            test_payload = test_response.get_json()
            self.assertEqual(test_response.status_code, 200)
            self.assertEqual(test_payload['message'], '测试消息发送成功')
            self.assertEqual(test_payload['result'], response_payload)
            self.assertEqual(mocked_post.call_count, 1)

    def test_user_can_retry_failed_notification_delivery(self):
        with fresh_app_harness() as harness:
            client, user_id = harness.make_client()
            predictor_id = create_predictor(harness, user_id, 'pc28', prediction_targets=['big_small'])
            endpoint_id = harness.db.create_notification_endpoint(
                user_id=user_id,
                channel_type='telegram',
                endpoint_key='123456789',
                endpoint_label='我的 TG',
                config={'chat_type': 'private'},
                status='active',
                is_default=True
            )
            subscription_id = harness.db.create_notification_subscription(
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
            delivery_id = harness.db.upsert_notification_delivery({
                'subscription_id': subscription_id,
                'user_id': user_id,
                'predictor_id': predictor_id,
                'endpoint_id': endpoint_id,
                'event_type': 'prediction_created',
                'record_key': '20260411003',
                'status': 'failed',
                'payload': {
                    'lottery_type': 'pc28',
                    'predictor_name': 'pc28-predictor',
                    'record_key': '20260411003',
                    'message_text': '测试重发消息'
                },
                'error_message': 'network error',
                'sent_at': None
            })

            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = {'ok': True, 'result': {'message_id': 321}}

            with patch('services.notification_service.requests.post', return_value=response) as mocked_post:
                retry_response = client.post(f'/api/notification-deliveries/{delivery_id}/retry')
            retry_payload = retry_response.get_json()
            self.assertEqual(retry_response.status_code, 200)
            self.assertEqual(retry_payload['item']['status'], 'delivered')
            self.assertEqual(mocked_post.call_count, 1)

    def test_user_can_test_notification_endpoint(self):
        with fresh_app_harness() as harness:
            client, _ = harness.make_client()
            harness.module.notification_service.update_settings(
                enabled=True,
                telegram_bot_token='123456:abcdef',
                telegram_bot_name='predictor_bot'
            )

            response_payload = {'ok': True, 'result': {'message_id': 456}}
            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = response_payload

            with patch('services.notification_service.requests.post', return_value=response) as mocked_post:
                test_response = client.post(
                    '/api/notification-endpoints/test',
                    json={
                        'channel_type': 'telegram',
                        'endpoint_key': '123456789',
                        'endpoint_label': '我的 Telegram',
                        'config': {'chat_type': 'private'},
                        'message': '用户侧测试消息'
                    }
                )
            test_payload = test_response.get_json()
            self.assertEqual(test_response.status_code, 200)
            self.assertEqual(test_payload['message'], '测试消息发送成功')
            self.assertEqual(test_payload['result'], response_payload)
            self.assertEqual(mocked_post.call_count, 1)


if __name__ == '__main__':
    unittest.main()
