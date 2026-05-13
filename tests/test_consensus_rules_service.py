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

        scored = score_today_against_rules(
            rules=rules,
            today_recommendations=recommendations,
            today_matches_detail=[]
        )

        matched_ids = {rule['rule_id'] for rule in scored[0]['matched_rules']}
        self.assertEqual(matched_ids, {'all_away'})

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
