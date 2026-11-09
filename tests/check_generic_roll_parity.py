"""PC28 通用过关与投注端纯函数对照，不触及数据库或发送接口。"""
from __future__ import annotations
import argparse
import itertools
import json
import sys
from unittest.mock import patch
from services.profit_simulator import ProfitSimulator, PC28_PROFIT_RULES
from tests.test_pc28_roll_simulation import ReplayDB, NOW


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--execution-src',required=True)
    args=parser.parse_args()
    sys.path.insert(0,args.execution_src)
    from pc28touzhu.domain.win_streak_roll import normalize_win_streak_roll_policy,create_cycle_state,settle_period
    metrics={'big_small':'大','odd_even':'单','combo':'小单','number':'0','double_group':'大单+小双','kill_group':'小双'}
    db=ReplayDB([5]); engine=ProfitSimulator(db); cases=0
    db.predictor['prediction_targets']=['number','big_small','odd_even','combo']
    with patch('services.profit_simulator.get_current_beijing_time',return_value=NOW):
        for rule,catalog in PC28_PROFIT_RULES.items():
            for profile in catalog['metrics']['odd_even']:
                for metric,value in metrics.items():
                    for mode in ('chain','all_in'):
                        for amount,digits_set in ((101.75,itertools.product(range(10),repeat=3)),(3000,[(4,4,5),(4,5,5),(0,0,0),(9,9,9)])):
                            config={'structure':'single_play','play_type':metric,'selection_mode':'fixed','fixed_value':value,
                                    'base_amount':amount,'roll_mode':mode,'target_streak':10}
                            policy=normalize_win_streak_roll_policy(config)
                            for digits in digits_set:
                                total=sum(digits); odd='单' if total%2 else '双'
                                db.draws['1000'].update(result_number=total,odd_even=odd,combo=('大' if total>=14 else '小')+odd,
                                    source_payload=json.dumps({'number':'+'.join(map(str,digits))}))
                                actual=engine.build_profit_simulation(1,bet_mode='win_streak_roll',profit_rule_id=rule,
                                    odds_profile=profile,roll_config=config)['records'][0]
                                expected=settle_period(policy,create_cycle_state(policy),{'triplet':list(digits)},
                                    signal={'bet_type':metric,'bet_value':value},settlement_rule_id=rule+'_'+profile)
                                got=(actual['stake_amount'],actual['payout_amount'],actual['net_profit'],actual['balance_after'],actual['win_streak'])
                                want=(expected['plan']['stake_total'],expected['payout_total'],expected['net_pnl'],expected['state']['balance'],expected['win_streak'])
                                assert got==want,(rule,profile,metric,mode,amount,digits,got,want)
                                cases+=1
    print(json.dumps({'cases':cases,'differences':0,'scope':'6种玩法 × 8赔率档 × 两种滚动方式 × 全三球开奖与金额边界'},ensure_ascii=False))

if __name__=='__main__':
    main()
