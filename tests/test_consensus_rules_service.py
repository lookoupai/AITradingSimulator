from __future__ import annotations

import unittest

from services.consensus_rules_service import score_today_against_rules


class ConsensusRulesServiceTests(unittest.TestCase):
    def test_n_agree_does_not_match_all_agree_distribution(self):
        rules = [
            {
                'id': 'all_away',
                'title': '警惕全员一致陷阱',
                'field': 'spf',
                'condition_match': {'type': 'all_agree', 'field': 'spf', 'value': '负'},
                'action': '警惕',
                'confidence': 'high',
                'rationale': '全员一致历史偏低'
            },
            {
                'id': 'five_away',
                'title': '五方案客胜共识',
                'field': 'spf',
                'condition_match': {'type': 'n_agree', 'n': 5, 'field': 'spf', 'value': '负'},
                'action': '参考',
                'confidence': 'medium',
                'rationale': '五方案一致历史较好'
            }
        ]
        recommendations = [{
            'event_key': 'lens-psg',
            'title': '[法甲] 朗斯 vs 巴黎圣曼',
            'fields': [{
                'field': 'spf',
                'all_predictions': {
                    '负': {'count': 5, 'predictors': [1, 2, 3, 4, 5]}
                }
            }]
        }]

        # 5 of 5 predictors all predict 负 => all_agree triggers
        scored = score_today_against_rules(
            rules=rules,
            today_recommendations=recommendations,
            today_matches_detail=[],
            pool_predictor_ids=[1, 2, 3, 4, 5]
        )

        matched_ids = {rule['rule_id'] for rule in scored[0]['matched_rules']}
        self.assertEqual(matched_ids, {'all_away'})

    def test_all_agree_does_not_trigger_when_some_predictors_missing(self):
        """all_agree 必须是方案池全员都出了预测且一致才触发，
        不能因为某些方案没出预测就忽略它们。"""
        rules = [{
            'id': 'all_trap',
            'title': 'RQ全员一致陷阱',
            'field': 'rqspf',
            'condition_match': {'type': 'all_agree', 'field': 'rqspf'},
            'action': '警惕',
            'confidence': 'high',
            'rationale': '全员一致陷阱'
        }]
        # 只有 4/6 方案出了 rqspf 预测，全部预测"胜"
        # 但方案池有 6 个方案，所以不算"全员一致"
        recommendations = [{
            'event_key': 'barca-betis',
            'title': '[西甲] 巴萨 vs 贝蒂斯',
            'fields': [{
                'field': 'rqspf',
                'all_predictions': {
                    '胜': {'count': 4, 'predictors': [1, 2, 3, 4]}
                }
            }]
        }]

        scored = score_today_against_rules(
            rules=rules,
            today_recommendations=recommendations,
            today_matches_detail=[],
            pool_predictor_ids=[1, 2, 3, 4, 5, 6]
        )

        # 不应该触发 all_agree，因为只有 4/6 出了预测
        self.assertEqual(len(scored), 0)

    def test_all_agree_triggers_when_all_pool_predictors_predict(self):
        """方案池全员都出了预测且一致时，all_agree 应该触发。"""
        rules = [{
            'id': 'all_trap',
            'title': 'RQ全员一致陷阱',
            'field': 'rqspf',
            'condition_match': {'type': 'all_agree', 'field': 'rqspf'},
            'action': '警惕',
            'confidence': 'high',
            'rationale': '全员一致陷阱'
        }]
        recommendations = [{
            'event_key': 'match1',
            'title': 'Test',
            'fields': [{
                'field': 'rqspf',
                'all_predictions': {
                    '胜': {'count': 6, 'predictors': [1, 2, 3, 4, 5, 6]}
                }
            }]
        }]

        scored = score_today_against_rules(
            rules=rules,
            today_recommendations=recommendations,
            today_matches_detail=[],
            pool_predictor_ids=[1, 2, 3, 4, 5, 6]
        )

        self.assertEqual(len(scored), 1)
        self.assertEqual(scored[0]['matched_rules'][0]['consensus_value'], '胜')

    def test_majority_agree_uses_pool_size_as_denominator(self):
        """majority_agree 应以方案池总数为分母，而非仅出预测的方案数。"""
        rules = [{
            'id': 'majority',
            'title': '多数共识',
            'field': 'rqspf',
            'condition_match': {'type': 'majority_agree', 'threshold': 0.6, 'field': 'rqspf'},
            'action': '参考',
            'confidence': 'medium',
            'rationale': '多数共识'
        }]
        # 4/6 方案预测"胜"，4/4=100% > 60%，但 4/6=66.7% 也 > 60%，所以两种计算都会触发
        # 换一种：3/5 方案预测"胜"，3/3=100% > 60% 但 3/5=60% 刚好等于 60%
        # 用 3/6 = 50% < 60% 来测试
        recommendations = [{
            'event_key': 'match2',
            'title': 'Test',
            'fields': [{
                'field': 'rqspf',
                'all_predictions': {
                    '胜': {'count': 3, 'predictors': [1, 2, 3]},
                    '负': {'count': 2, 'predictors': [4, 5]}
                }
            }]
        }]

        # 3/5=0.6 >= 0.6 会触发（旧逻辑），但 3/6=0.5 < 0.6 不应触发（新逻辑）
        scored = score_today_against_rules(
            rules=rules,
            today_recommendations=recommendations,
            today_matches_detail=[],
            pool_predictor_ids=[1, 2, 3, 4, 5, 6]
        )

        self.assertEqual(len(scored), 0, "3/6=50% 不应触发 60% 阈值的 majority_agree")

    def test_n_agree_still_matches_partial_five_of_six_consensus(self):
        rules = [{
            'id': 'five_away',
            'title': '五方案客胜共识',
            'field': 'spf',
            'condition_match': {'type': 'n_agree', 'n': 5, 'field': 'spf', 'value': '负'},
            'action': '参考',
            'confidence': 'medium',
            'rationale': '五方案一致历史较好'
        }]
        recommendations = [{
            'event_key': 'partial-consensus',
            'title': '[测试] A vs B',
            'fields': [{
                'field': 'spf',
                'all_predictions': {
                    '负': {'count': 5, 'predictors': [1, 2, 3, 4, 5]},
                    '平': {'count': 1, 'predictors': [6]}
                }
            }]
        }]

        scored = score_today_against_rules(
            rules=rules,
            today_recommendations=recommendations,
            today_matches_detail=[]
        )

        self.assertEqual(scored[0]['matched_rules'][0]['rule_id'], 'five_away')
        self.assertEqual(scored[0]['matched_rules'][0]['consensus_value'], '负')


if __name__ == '__main__':
    unittest.main()
