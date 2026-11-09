"""独立跨项目金额对照：只导入领域函数，不连接数据库或启动应用。

用法：python -m tests.check_roll_execution_parity --execution-src /path/to/pc28touzhu/src
"""
from __future__ import annotations

import argparse
import itertools
import json
import sys
from unittest.mock import patch

from services.profit_simulator import PC28_PROFIT_RULES, ProfitSimulator
from tests.test_pc28_roll_simulation import NOW, ReplayDB


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--execution-src', required=True)
    args = parser.parse_args()
    sys.path.insert(0, args.execution_src)
    from pc28touzhu.domain.win_streak_roll import create_cycle_state, normalize_win_streak_roll_policy, settle_period

    cases = 0
    db = ReplayDB([5])
    simulator = ProfitSimulator(db)
    with patch('services.profit_simulator.get_current_beijing_time', return_value=NOW):
        for rule_id, rule in PC28_PROFIT_RULES.items():
            for profile in rule['metrics']['odd_even']:
                for structure in 'ABCD':
                    for mode in ('chain', 'all_in'):
                        for amount, triplets in ((100, itertools.product(range(10), repeat=3)),
                                                 (101.75, [(0, 0, 0), (9, 9, 9), (1, 2, 2), (4, 4, 5), (4, 5, 5)]),
                                                 (3000, [(4, 4, 5), (4, 5, 5), (1, 2, 2), (9, 9, 9)])):
                            policy = normalize_win_streak_roll_policy({'structure': structure, 'roll_mode': mode, 'base_amount': amount})
                            for digits in triplets:
                                total = sum(digits)
                                odd = '单' if total % 2 else '双'
                                draw = db.draws['1000']
                                draw.update(result_number=total, odd_even=odd, combo=('大' if total >= 14 else '小') + odd,
                                            source_payload=json.dumps({'number': '+'.join(map(str, digits))}))
                                actual = simulator.build_profit_simulation(1, profit_rule_id=rule_id, odds_profile=profile,
                                    bet_mode='win_streak_roll', roll_config={'structure': structure, 'roll_mode': mode, 'base_amount': amount})['records'][0]
                                expected = settle_period(policy, create_cycle_state(policy), {'triplet': list(digits)},
                                                         settlement_rule_id=rule_id + '_' + profile)
                                fields = (actual['payout_amount'], actual['net_profit'], actual['balance_after'], actual['win_streak'],
                                          [leg['stake_amount'] for leg in actual['legs']], [leg['payout_amount'] for leg in actual['legs']])
                                wanted = (expected['payout_total'], expected['net_pnl'], expected['state']['balance'], expected['win_streak'],
                                          [expected['main_result']['stake'], expected['aux_result']['stake']],
                                          [expected['main_result']['payout'], expected['aux_result']['payout']])
                                assert fields == wanted, (rule_id, profile, structure, mode, amount, digits, fields, wanted)
                                cases += 1
    print(json.dumps({'cases': cases, 'differences': 0, 'scope': '全三球开奖 × 四结构 × 两滚动方式 × 全部赔率档，另含尾差与降赔金额'}, ensure_ascii=False))


if __name__ == '__main__':
    main()
