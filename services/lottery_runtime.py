"""
多彩种运行时
"""
from __future__ import annotations

import threading


class LotteryRuntime:
    def __init__(self, db, pc28_engine, handlers: dict[str, object]):
        self.db = db
        self.pc28_engine = pc28_engine
        self.handlers = handlers
        self._lock = threading.RLock()

    def get_handler(self, lottery_type: str):
        return self.handlers.get(lottery_type)

    def generate_prediction(self, predictor_id: int):
        predictor = self.db.get_predictor(predictor_id, include_secret=True)
        if not predictor:
            raise ValueError('预测方案不存在')

        lottery_type = predictor.get('lottery_type', 'pc28')
        if lottery_type == 'pc28':
            return self.pc28_engine.generate_prediction(predictor_id, auto_mode=False)

        handler = self.get_handler(lottery_type)
        if handler is None:
            raise ValueError(f'暂不支持的彩种: {lottery_type}')

        with self._lock:
            return handler.generate_prediction(self.db, predictor, auto_mode=False)

    def run_auto_cycle(self) -> dict:
        with self._lock:
            base_result = self.run_pc28_cycle()
            prediction_results = list(base_result.get('predictions') or [])
            settled_count = int(base_result.get('settled_count') or 0)

            for lottery_type, handler in self.handlers.items():
                if lottery_type == 'pc28':
                    continue

                result = self.run_lottery_cycle(lottery_type)
                settled_count += int(result.get('settled_count') or 0)
                prediction_results.extend(result.get('predictions') or [])

            return {
                'settled_count': settled_count,
                'predictions': prediction_results
            }

    def run_pc28_cycle(self) -> dict:
        return self.pc28_engine.run_auto_cycle()

    def run_lottery_cycle(self, lottery_type: str) -> dict:
        if lottery_type == 'pc28':
            return self.run_pc28_cycle()

        handler = self.get_handler(lottery_type)
        if handler is None:
            raise ValueError(f'暂不支持的彩种: {lottery_type}')

        if hasattr(handler, 'run_auto_cycle'):
            return handler.run_auto_cycle(self.db)

        settled_items = handler.settle_pending_predictions(self.db)
        prediction_results = []
        predictors = self.db.get_enabled_predictors(
            lottery_type=lottery_type,
            include_secret=True,
            exclude_auto_paused=True
        )
        for predictor in predictors:
            try:
                result = handler.generate_prediction(self.db, predictor, auto_mode=True)
                prediction_results.append({
                    'predictor_id': predictor['id'],
                    'lottery_type': lottery_type,
                    'issue_no': result.get('issue_no') or result.get('run_key'),
                    'status': result.get('status', 'pending')
                })
            except Exception as exc:
                prediction_results.append({
                    'predictor_id': predictor['id'],
                    'lottery_type': lottery_type,
                    'status': 'failed',
                    'error': str(exc)
                })

        return {
            'settled_count': len(settled_items),
            'predictions': prediction_results
        }

    def build_dashboard_data(self, predictor_id: int, pc28_dashboard_builder):
        predictor = self.db.get_predictor(predictor_id, include_secret=True)
        if not predictor:
            raise ValueError('预测方案不存在')

        lottery_type = predictor.get('lottery_type', 'pc28')
        if lottery_type == 'pc28':
            return pc28_dashboard_builder(predictor_id)

        handler = self.get_handler(lottery_type)
        if handler is None:
            raise ValueError(f'暂不支持的彩种: {lottery_type}')
        return handler.build_predictor_dashboard(self.db, predictor)
