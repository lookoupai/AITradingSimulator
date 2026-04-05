"""
提示词检查与优化辅助
"""
from __future__ import annotations

import re

from utils.pc28 import TARGET_LABELS, normalize_injection_mode, normalize_primary_metric, normalize_target_list

PLACEHOLDER_PATTERN = re.compile(r'\{\{\s*([a-zA-Z0-9_]+)\s*\}\}')

PLACEHOLDER_DEFINITIONS = [
    {
        'name': 'recent_draws_summary',
        'description': '最近若干期的自然语言开奖摘要，适合摘要模式。'
    },
    {
        'name': 'recent_draws_csv',
        'description': '最近若干期的 CSV 结构化开奖数据，适合原始模式。'
    },
    {
        'name': 'recent_draws_json',
        'description': '最近若干期的 JSON 数组数据，适合需要结构化输入的提示词。'
    },
    {
        'name': 'omission_summary',
        'description': '当前遗漏、冷号、热号的摘要。'
    },
    {
        'name': 'today_summary',
        'description': '今日统计摘要。'
    },
    {
        'name': 'preview_summary',
        'description': '聚合走势与趋势预览摘要。'
    },
    {
        'name': 'latest_draw_summary',
        'description': '最近一期开奖的单条摘要。'
    },
    {
        'name': 'current_time_beijing',
        'description': '当前北京时间完整字符串。'
    },
    {
        'name': 'current_year',
        'description': '当前北京时间的年份。'
    },
    {
        'name': 'current_month',
        'description': '当前北京时间的月份。'
    },
    {
        'name': 'current_day',
        'description': '当前北京时间的日期。'
    },
    {
        'name': 'current_hour',
        'description': '当前北京时间的小时。'
    },
    {
        'name': 'current_minute',
        'description': '当前北京时间的分钟。'
    },
    {
        'name': 'next_issue_no',
        'description': '下一期期号。'
    },
    {
        'name': 'countdown',
        'description': '下一期开奖倒计时。'
    },
    {
        'name': 'prediction_targets',
        'description': '当前方案勾选的预测目标文本。'
    },
    {
        'name': 'history_window',
        'description': '当前方案使用的历史窗口期数。'
    }
]

KNOWN_PLACEHOLDERS = {item['name'] for item in PLACEHOLDER_DEFINITIONS}


def get_prompt_placeholder_catalog() -> list[dict]:
    return [
        {
            'name': item['name'],
            'token': _placeholder_token(item['name']),
            'description': item['description']
        }
        for item in PLACEHOLDER_DEFINITIONS
    ]


def analyze_prompt(
    prompt: str,
    prediction_targets: list[str] | None,
    data_injection_mode: str,
    primary_metric: str
) -> dict:
    prompt_text = (prompt or '').strip()
    targets = normalize_target_list(prediction_targets)
    injection_mode = normalize_injection_mode(data_injection_mode)
    metric = normalize_primary_metric(primary_metric)
    issues = []

    placeholders = _extract_placeholders(prompt_text)
    unknown_placeholders = [item for item in placeholders if item not in KNOWN_PLACEHOLDERS]

    if not prompt_text:
        issues.append(_issue('error', '提示词为空', '当前还没有填写任何提示词。', '先用方案示例一键填充，再基于示例修改。'))

    if len(prompt_text) > 2200:
        issues.append(_issue('warning', '提示词偏长', f'当前提示词约 {len(prompt_text)} 个字符，过长会增加模型跑偏和耗时。', '优先删掉重复说明、冗余示例和无关背景。'))

    if _has_fixed_output_example(prompt_text):
        issues.append(_issue('error', '存在固定答案示例', '提示词里检测到固定号码、大小、单双或置信度示例，模型可能机械照抄。', '只保留字段名和输出结构，不要写死具体号码和概率。'))

    if 'json' not in prompt_text.lower() and '结构化' not in prompt_text and '字段' not in prompt_text:
        issues.append(_issue('warning', '缺少结构化输出约束', '提示词里没有明显要求输出 JSON 或结构化字段。', '补上“只输出 JSON”以及字段说明，减少解析失败。'))

    if 'recent_draws_csv' in placeholders and injection_mode != 'raw':
        issues.append(_issue('info', '变量与注入模式不一致', '你在提示词里使用了原始开奖 CSV 变量，但当前模式不是原始模式。', '若你希望固定格式输入，建议切换为“原始模式”。'))

    if 'recent_draws_summary' in placeholders and injection_mode != 'summary':
        issues.append(_issue('info', '变量与注入模式不一致', '你在提示词里使用了开奖摘要变量，但当前模式不是摘要模式。', '若你偏向自然语言摘要输入，建议切换为“摘要模式”。'))

    if unknown_placeholders:
        issues.append(_issue('warning', '存在未知变量', f'检测到未识别变量：{", ".join(sorted(unknown_placeholders))}', '改成页面提示的标准变量名，避免模型拿不到数据。'))

    if 'number' not in targets and _mentions_number_prediction(prompt_text):
        issues.append(_issue('warning', '提示词与目标玩法不一致', '当前没有勾选“号码”，但提示词里要求主号、和值或精确数字预测。', '如果你只看大小单双，请删掉精确和值要求；否则勾选“号码”。'))

    if 'combo' not in targets and _mentions_combo_prediction(prompt_text):
        issues.append(_issue('warning', '提示词与目标玩法不一致', '当前没有勾选“组合”，但提示词里要求输出大单/大双/小单/小双。', '如果确实要看组合，请勾选“组合”目标。'))

    if metric in {'double_group', 'kill_group'} and 'combo' not in targets:
        issues.append(_issue('error', '主玩法依赖组合预测', '双组/杀组统计必须基于组合预测。', '请先勾选“组合”，再使用双组或杀组作为主玩法。'))

    if not placeholders:
        issues.append(_issue('info', '未使用变量占位符', '当前提示词完全依赖平台默认注入数据。', '这不是错误；如果你想精确控制输入格式，可手动加入变量占位符。'))

    recommendations = _build_variable_recommendations(prompt_text, placeholders, injection_mode)
    risk_level = _risk_level(issues)
    summary = _build_summary(risk_level, issues, targets, injection_mode, metric)

    return {
        'risk_level': risk_level,
        'summary': summary,
        'issues': issues,
        'detected_placeholders': placeholders,
        'unknown_placeholders': unknown_placeholders,
        'recommended_variables': recommendations['variables'],
        'recommended_snippets': recommendations['snippets'],
        'prediction_targets': targets,
        'data_injection_mode': injection_mode,
        'primary_metric': metric
    }


def build_optimizer_prompt(
    current_prompt: str,
    analysis: dict,
    predictor_payload: dict
) -> str:
    issues_text = '\n'.join(
        f"- [{item['level']}] {item['title']}：{item['detail']}"
        for item in analysis.get('issues', [])
    ) or '- 暂未发现明显问题'

    placeholders_text = ', '.join(item['token'] for item in get_prompt_placeholder_catalog())
    targets_text = ', '.join(TARGET_LABELS.get(item, item) for item in analysis.get('prediction_targets', []))

    return f"""你是 PC28 提示词优化助手。你的任务是帮助新手把提示词改得更稳定、更容易命中目标玩法。

当前方案配置：
- 目标玩法：{targets_text or '未设置'}
- 数据注入模式：{analysis.get('data_injection_mode')}
- 主玩法统计口径：{analysis.get('primary_metric')}
- 历史窗口：{predictor_payload.get('history_window')}

可用变量：
{placeholders_text}

静态检查发现的问题：
{issues_text}

当前提示词：
{current_prompt or '（空）'}

请输出 JSON，字段如下：
{{
  "summary": "一句话总结当前提示词的主要问题",
  "issues": ["问题1", "问题2"],
  "why": ["为什么这样改1", "为什么这样改2"],
  "optimized_prompt": "给出一版可直接替换的新提示词"
}}

要求：
1. 新提示词必须兼容当前目标玩法。
2. 不要写死具体号码、概率或开奖结果示例。
3. 尽量保留原有方法论风格，例如统计、小六壬、回归等。
4. 如果适合，主动使用变量占位符。
5. 只输出 JSON。"""


def build_external_prompt_template(predictor_payload: dict) -> str:
    targets = normalize_target_list(predictor_payload.get('prediction_targets'))
    target_labels = ', '.join(TARGET_LABELS.get(item, item) for item in targets) or '未设置'
    injection_mode = normalize_injection_mode(predictor_payload.get('data_injection_mode'))
    metric = normalize_primary_metric(predictor_payload.get('primary_metric'))
    prediction_method = predictor_payload.get('prediction_method') or '自定义策略'
    current_prompt = (predictor_payload.get('system_prompt') or '').strip()
    placeholders_text = '\n'.join(
        f"- {item['token']}：{item['description']}"
        for item in get_prompt_placeholder_catalog()
    )

    recommendation_lines = [
        f"- 当前数据注入模式是“{'原始模式' if injection_mode == 'raw' else '摘要模式'}”，优先考虑使用 "
        f"{_placeholder_token('recent_draws_csv') if injection_mode == 'raw' else _placeholder_token('recent_draws_summary')}。"
    ]

    if any(keyword in prediction_method for keyword in ['小六壬', '起课', '时间', '奇门', '梅花']):
        recommendation_lines.append(
            f"- 当前方法偏时间起课，建议补充 {_placeholder_token('current_time_beijing')} 或年月日时分拆变量。"
        )

    if 'combo' in targets:
        recommendation_lines.append('- 当前方案包含组合预测，可以要求模型分析大单/大双/小单/小双。')

    if 'number' not in targets:
        recommendation_lines.append('- 当前方案未勾选单点，不要要求模型强制输出精确和值或主号。')

    if 'number' in targets:
        recommendation_lines.append('- 当前方案包含单点，可要求模型输出和值方向或主预测号码，但不要写死示例答案。')

    recommendation_lines.append(
        f"- 如果你不想手写变量，平台也会按“{'原始模式' if injection_mode == 'raw' else '摘要模式'}”自动注入数据；"
        '但如果你想固定输入格式，请显式写变量占位符。'
    )

    recommendations_text = '\n'.join(recommendation_lines)

    return f"""你是一个资深提示词工程师。请帮我为“AITradingSimulator”的 PC28 预测项目编写一版可直接使用的“自定义提示词”。

重要背景：
- 你写出的内容不是完整系统提示词，而是项目里“自定义提示词”输入框的内容。
- 平台外层已经会补充固定规则、方案信息，并要求模型最终输出 JSON。
- 你的任务重点是：帮我写好分析方法、变量使用方式、推理步骤、风险控制和输出约束细节。

当前方案配置：
- 方案名称：{predictor_payload.get('name') or '未命名方案'}
- 预测方法：{prediction_method}
- 预测目标：{target_labels}
- 主玩法：{TARGET_LABELS.get(metric, metric)}
- 数据注入模式：{'原始模式' if injection_mode == 'raw' else '摘要模式'}
- 历史窗口：最近 {predictor_payload.get('history_window') or '未设置'} 期

这个项目允许在提示词中使用以下变量，占位格式必须原样保留，只能使用这些变量，不要发明新变量：
{placeholders_text}

结合当前配置，优先注意：
{recommendations_text}

编写要求：
1. 最终提示词必须服务于加拿大28（PC28）下一期预测，不要写成通用聊天助手。
2. 必须兼容当前预测目标；不要要求输出未勾选的玩法。
3. 只允许使用上面列出的变量名；不需要时可以不用，但不要自造变量。
4. 不要写死具体和值、号码、大小单双、组合、概率、置信度或开奖结果示例。
5. 提示词尽量简洁、稳定、可执行，避免空泛套话和重复要求。
6. 如果适合，明确告诉模型先看什么、再看什么、如何权衡冲突信号。
7. 如果我补充的方法偏统计、小六壬、回归、遗漏回补、趋势判断等，请保留该风格，但仍要保持基本风控。
8. 输出内容请使用中文简体。

请按下面格式回复：
第一部分：可直接粘贴到项目里的最终提示词正文，不要加代码块，不要加前后解释
第二部分：简短说明你用了哪些变量、为什么这样安排

我当前已有提示词（如有价值可继承其风格；如果质量差可以直接重写）：
{current_prompt or '（暂无）'}

我接下来会在下面补充自己的个性化需求，请基于项目规则和这些需求生成最终提示词：
【在这里补充你的需求，例如：偏重小六壬、只关心大小单双、不要激进建议、强调遗漏回补、输出理由更短等】"""


def _extract_placeholders(prompt: str) -> list[str]:
    if not prompt:
        return []
    seen = []
    for match in PLACEHOLDER_PATTERN.findall(prompt):
        if match not in seen:
            seen.append(match)
    return seen


def _has_fixed_output_example(prompt: str) -> bool:
    patterns = [
        r'主号\s*[:：]\s*\d{1,2}',
        r'备选\s*[:：]\s*\d{1,2}',
        r'predicted_number["\']?\s*[:：]\s*\d{1,2}',
        r'置信度\s*[:：]\s*\d{1,3}%',
        r'大小\s*[:：]\s*[大小单双]',
        r'单双\s*[:：]\s*[单双]'
    ]
    return any(re.search(pattern, prompt, re.IGNORECASE) for pattern in patterns)


def _mentions_number_prediction(prompt: str) -> bool:
    keywords = ['和值', '主号', '精确号码', 'predicted_number', '数字1', '数字2', '数字3']
    return any(keyword in prompt for keyword in keywords)


def _mentions_combo_prediction(prompt: str) -> bool:
    keywords = ['大单', '大双', '小单', '小双', '组合']
    return any(keyword in prompt for keyword in keywords)


def _issue(level: str, title: str, detail: str, suggestion: str) -> dict:
    return {
        'level': level,
        'title': title,
        'detail': detail,
        'suggestion': suggestion
    }


def _risk_level(issues: list[dict]) -> str:
    levels = [item['level'] for item in issues]
    if 'error' in levels:
        return 'high'
    if 'warning' in levels:
        return 'medium'
    return 'low'


def _build_summary(risk_level: str, issues: list[dict], targets: list[str], injection_mode: str, metric: str) -> str:
    if not issues:
        return f'提示词结构基本合理，当前按 {injection_mode} 模式注入数据，主玩法是 {metric}。'
    return f'当前风险等级为 {risk_level}，共发现 {len(issues)} 项提示词问题；目标玩法为 {", ".join(targets)}。'


def _build_variable_recommendations(prompt: str, placeholders: list[str], injection_mode: str) -> dict:
    recommendations = []
    snippets = []

    def add(variable: str, reason: str, snippet: str = ''):
        if any(item['name'] == variable for item in recommendations):
            return
        recommendations.append({
            'name': variable,
            'reason': reason
        })
        if snippet:
            snippets.append({
                'variable': variable,
                'snippet': snippet
            })

    if any(keyword in prompt for keyword in ['最近', '历史数据', '开奖数据', '输入格式', '数字1', '数字2', '数字3']):
        preferred = 'recent_draws_csv' if injection_mode == 'raw' else 'recent_draws_summary'
        add(preferred, '你提到了历史开奖或固定输入格式，适合把最近开奖数据显式写进提示词。', f'最近 {{history_window}} 期数据：\n{{{{{preferred}}}}}')

    if any(keyword in prompt for keyword in ['时间', '起课', '年月日时', '当前时刻', '北京时间']):
        add('current_time_beijing', '你提到了时间或起课，建议显式传入当前北京时间。', '当前北京时间：\n{{current_time_beijing}}')
        add('current_year', '如果你按年月日时拆分起课，可以直接用年。')
        add('current_month', '如果你按年月日时拆分起课，可以直接用月。')
        add('current_day', '如果你按年月日时拆分起课，可以直接用日。')
        add('current_hour', '如果你按年月日时拆分起课，可以直接用时。')

    if any(keyword in prompt for keyword in ['下一期', '期号', '本期', '下期']):
        add('next_issue_no', '你提到了下一期期号，建议显式注入。', '下一期期号：\n{{next_issue_no}}')
        add('countdown', '如果你关心开奖节奏，可补充倒计时。')

    if any(keyword in prompt for keyword in ['遗漏', '冷号', '热号']):
        add('omission_summary', '你提到了遗漏、冷号或热号，建议直接引用遗漏统计。', '遗漏统计：\n{{omission_summary}}')

    if any(keyword in prompt for keyword in ['今日统计', '今日走势', '当日统计']):
        add('today_summary', '你提到了今日统计，建议直接注入今日统计摘要。', '今日统计：\n{{today_summary}}')

    if any(keyword in prompt for keyword in ['走势', '预览', '趋势']):
        add('preview_summary', '你提到了走势或趋势，建议补充聚合走势摘要。', '走势预览：\n{{preview_summary}}')

    if 'history_window' not in placeholders:
        add('history_window', '如果你想让提示词和方案历史窗口保持一致，可以直接使用历史窗口变量。')

    return {
        'variables': recommendations,
        'snippets': snippets
    }


def _placeholder_token(name: str) -> str:
    return f'{{{{{name}}}}}'
