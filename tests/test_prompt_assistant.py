from __future__ import annotations

import unittest

from utils.prompt_assistant import analyze_prompt, build_external_prompt_template, build_optimizer_prompt


BALANCE_STYLE_BAD_PROMPT = """
你是找平衡策略助手。请输出以下 JSON：
{
  "target": "给出建议方向",
  "missing_rate": "冷热缺口",
  "suggestion": "下注建议",
  "next_issue": "下一期期号",
  "countdown": "倒计时"
}
不要写死具体号码，不预测号码。
输出格式如下：
第一部分：预测过程
第二部分：字段说明
"""


class PromptAssistantPc28GovernanceTests(unittest.TestCase):
    def test_analyze_prompt_flags_balance_schema_and_style_conflicts(self):
        analysis = analyze_prompt(
            prompt=BALANCE_STYLE_BAD_PROMPT,
            prediction_targets=['number', 'big_small', 'odd_even', 'combo'],
            data_injection_mode='summary',
            primary_metric='number',
            lottery_type='pc28'
        )

        titles = {item['title'] for item in analysis['issues']}
        self.assertEqual(analysis['risk_level'], 'high')
        self.assertIn('自定义JSON字段与平台协议冲突', titles)
        self.assertIn('提示词覆盖平台输出协议', titles)
        self.assertIn('号码输出要求冲突', titles)
        self.assertIn('说明文/元说明风格风险', titles)

    def test_optimizer_prompt_contains_protocol_guardrails_and_rewrite_requirement(self):
        analysis = analyze_prompt(
            prompt=BALANCE_STYLE_BAD_PROMPT,
            prediction_targets=['number', 'big_small', 'odd_even', 'combo'],
            data_injection_mode='summary',
            primary_metric='number',
            lottery_type='pc28'
        )

        optimized_prompt_instruction = build_optimizer_prompt(
            current_prompt=BALANCE_STYLE_BAD_PROMPT,
            analysis=analysis,
            predictor_payload={'history_window': 80},
            lottery_type='pc28'
        )

        self.assertIn('自定义提示词不能重定义平台最终输出协议', optimized_prompt_instruction)
        self.assertIn('必须重写为可执行指令，不要沿用原结构和措辞', optimized_prompt_instruction)
        self.assertIn(
            'issue_no、predicted_number、predicted_big_small、predicted_odd_even、predicted_combo、confidence、reasoning_summary',
            optimized_prompt_instruction
        )

    def test_external_template_reuses_same_pc28_protocol_guardrails(self):
        external_template = build_external_prompt_template(
            predictor_payload={
                'name': '找平衡',
                'prediction_method': '找平衡',
                'prediction_targets': ['number', 'big_small', 'odd_even', 'combo'],
                'primary_metric': 'number',
                'data_injection_mode': 'summary',
                'history_window': 80,
                'system_prompt': BALANCE_STYLE_BAD_PROMPT
            },
            lottery_type='pc28'
        )

        self.assertIn('自定义提示词不能重定义平台最终输出协议', external_template)
        self.assertIn('不要包含变量释义、项目背景、回复格式说明或“第一部分/第二部分”等元说明结构', external_template)
        self.assertIn(
            'issue_no、predicted_number、predicted_big_small、predicted_odd_even、predicted_combo、confidence、reasoning_summary',
            external_template
        )

