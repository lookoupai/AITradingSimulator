"""临时数据库上的收益模拟浏览器测试服务；无预测、采集或通知后台线程。"""
from __future__ import annotations

import json
import signal
from datetime import timedelta
from wsgiref.simple_server import make_server

from tests.support import create_predictor, fresh_app_harness
from tests.test_pc28_roll_simulation import ReplayDB


def main():
    with fresh_app_harness() as harness:
        from utils.timezone import get_current_beijing_time
        client, user_id = harness.make_client()
        ids = {}
        now = get_current_beijing_time()
        for label, share in (('records', 'records'), ('summary', 'stats_only')):
            pid = create_predictor(harness, user_id, 'pc28', name='过关浏览器验证 ' + label,
                                   profit_rule_id='pc28_fullpay_netdisk', share_level=share)
            ids[label] = pid
            sample = ReplayDB([5, 7, 9, 16], times=[now-timedelta(minutes=20-index) for index in range(4)])
            for draw in sample.draws.values():
                draw['result_number_text'] = str(draw['result_number'])
                draw['draw_date'] = now.strftime('%Y-%m-%d')
                draw['draw_time'] = draw['open_time'][11:19]
            harness.db.upsert_draws('pc28', list(sample.draws.values()))
            for row in sample.predictions:
                harness.db.upsert_prediction({**row, 'predictor_id': pid, 'lottery_type': 'pc28'})
                conn = harness.db.get_connection()
                conn.execute('UPDATE predictions SET created_at = ? WHERE predictor_id = ? AND issue_no = ?',
                             (row['created_at'], pid, row['issue_no']))
                conn.commit()
                conn.close()
        ids['football'] = create_predictor(harness, user_id, 'jingcai_football', name='竞彩足球回归')
        ids['no_combo'] = create_predictor(harness, user_id, 'pc28', name='只有大小的方案', prediction_targets=['big_small'])
        server = make_server('127.0.0.1', 0, harness.app)
        print(json.dumps({'url': f'http://127.0.0.1:{server.server_port}', 'cookie': client.get_cookie('session').value,
                          'ids': ids}), flush=True)
        signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()


if __name__ == '__main__':
    main()
