from __future__ import annotations

import unittest

from tests.support import create_predictor, fresh_app_harness


class PublicPagesTests(unittest.TestCase):
    def test_index_and_public_lottery_pages_render_independent_entry_points(self):
        with fresh_app_harness() as harness:
            client = harness.app.test_client()

            index_response = client.get('/')
            pc28_response = client.get('/pc28')
            football_response = client.get('/jingcai-football')

            self.assertEqual(index_response.status_code, 200)
            self.assertEqual(pc28_response.status_code, 200)
            self.assertEqual(football_response.status_code, 200)

            index_html = index_response.get_data(as_text=True)
            pc28_html = pc28_response.get_data(as_text=True)
            football_html = football_response.get_data(as_text=True)

            self.assertIn('选择你要查看的彩种', index_html)
            self.assertIn('/pc28', index_html)
            self.assertIn('/jingcai-football', index_html)

            self.assertIn('data-page="public-lottery"', pc28_html)
            self.assertIn('data-lottery-type="pc28"', pc28_html)
            self.assertIn('加拿大28 AI 预测', pc28_html)

            self.assertIn('data-page="public-lottery"', football_html)
            self.assertIn('data-lottery-type="jingcai_football"', football_html)
            self.assertIn('竞彩足球 AI 预测', football_html)

    def test_public_predictor_page_links_back_to_lottery_page(self):
        with fresh_app_harness() as harness:
            user_id = harness.db.create_user('public-user', harness.module.hash_password('password'))
            pc28_predictor_id = create_predictor(harness, user_id, 'pc28', share_level='records')
            football_predictor_id = create_predictor(harness, user_id, 'jingcai_football', share_level='records')
            client = harness.app.test_client()

            pc28_response = client.get(f'/public/predictors/{pc28_predictor_id}')
            football_response = client.get(f'/public/predictors/{football_predictor_id}')

            self.assertEqual(pc28_response.status_code, 200)
            self.assertEqual(football_response.status_code, 200)
            self.assertIn('href="/pc28"', pc28_response.get_data(as_text=True))
            self.assertIn('返回PC28', pc28_response.get_data(as_text=True))
            self.assertIn('href="/jingcai-football"', football_response.get_data(as_text=True))
            self.assertIn('返回竞彩足球', football_response.get_data(as_text=True))

    def test_auto_paused_predictor_is_hidden_from_public_entries(self):
        with fresh_app_harness() as harness:
            user_id = harness.db.create_user('public-user', harness.module.hash_password('password'))
            visible_predictor_id = create_predictor(harness, user_id, 'pc28', name='visible-predictor', share_level='records')
            paused_predictor_id = create_predictor(harness, user_id, 'pc28', name='paused-predictor', share_level='records')
            harness.db.update_predictor_runtime_state(paused_predictor_id, {
                'auto_paused': True,
                'auto_paused_at': '2026-04-14 10:00:00',
                'auto_pause_reason': 'AI 连续失败 3 次'
            })

            client = harness.app.test_client()
            list_response = client.get('/api/public/predictors?lottery_type=pc28')
            self.assertEqual(list_response.status_code, 200)
            list_data = list_response.get_json()
            listed_ids = [item['predictor_id'] for item in list_data['items']]
            self.assertIn(visible_predictor_id, listed_ids)
            self.assertNotIn(paused_predictor_id, listed_ids)

            detail_response = client.get(f'/api/public/predictors/{paused_predictor_id}')
            self.assertEqual(detail_response.status_code, 404)

            page_response = client.get(f'/public/predictors/{paused_predictor_id}')
            self.assertEqual(page_response.status_code, 404)

            with client.session_transaction() as session:
                session['user_id'] = user_id
                session['username'] = 'public-user'
                session['is_admin'] = 0
            predictors_response = client.get('/api/predictors')
            self.assertEqual(predictors_response.status_code, 200)
            predictors = predictors_response.get_json()
            paused_predictor = next(item for item in predictors if item['id'] == paused_predictor_id)
            self.assertFalse(paused_predictor['public_page_available'])


if __name__ == '__main__':
    unittest.main()
