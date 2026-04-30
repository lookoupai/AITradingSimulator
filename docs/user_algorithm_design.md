# 用户自定义算法与 AI 方案设计

## 背景

当前项目已经支持多用户预测方案、AI 模型方案和内置机器算法方案。用户希望通过 AI 聊天把自己的预测思路转成平台可执行方案，例如：

- A 用户：进球率预测法
- B 用户：最近六场预测法
- C 用户：赔率变化 + 伤停修正
- D 用户：小六壬 + 统计校验

设计目标不是让用户直接上传任意代码，而是让 AI 把自然语言策略编译成平台受控的算法定义。平台负责校验、回测、执行和版本管理。

## 已有基础

已验证的现有能力：

- `predictors` 表已有 `user_id`、`engine_type`、`algorithm_key`，可以表达“某用户的某预测方案使用某执行引擎”。
- `utils/predictor_engine.py` 已维护内置机器算法目录，并负责算法 key 归一化和展示文案。
- `services/machine_prediction.py` 已负责 PC28 与竞彩足球机器算法执行。
- `services/prediction_engine.py` 与 `services/jingcai_football_service.py` 已按 `ai/machine` 分支执行预测。
- 提示词体检、提示词优化和外部 AI 模板已有接口，可复用为“AI 算法助手”的调用基础。
- 收益模拟与历史记录接口已经存在，可作为用户算法回测结果展示的基础。

推断：

- 当前 `algorithm_key` 只适合指向内置算法 key，不足以直接表达用户私有算法，需要新增用户算法表，并让 `algorithm_key` 支持引用用户算法。
- 现有 `machine_prediction.py` 的 `if/elif` 分发会随着算法数量增长而膨胀，需要增加注册/解释层。

## 目标

1. 用户可以用自然语言描述自己的预测方法。
2. AI 将用户描述转成平台 DSL 算法定义。
3. 平台对算法定义做静态校验、字段校验、边界校验。
4. 用户可以回测、调整、保存算法版本。
5. 创建预测方案时可以选择“内置算法”或“我的算法”。
6. 执行预测时不运行用户任意代码，只运行平台解释器。
7. 算法结果要可解释、可追踪、可复现。

## 非目标

MVP 不做这些能力：

- 不执行 AI 生成的 Python / JavaScript / Shell 代码。
- 不允许用户算法访问文件系统、网络、环境变量、数据库连接。
- 不做复杂插件市场。
- 不做跨用户共享算法售卖。
- 不自动承诺收益，不把回测结果当作未来结果保证。

这些能力可以后续做，但必须单独引入沙箱、资源限制、审核和审计。

## 总体架构

新增五个核心模块：

1. `services/user_algorithm_service.py`
   用户算法 CRUD、版本管理、权限校验。

2. `services/algorithm_chat_service.py`
   调用 OpenAI 兼容模型，把用户自然语言转成 DSL 草稿，并根据回测结果生成调整建议。

3. `services/algorithm_definition_validator.py`
   校验 DSL schema、彩种字段、操作符、阈值范围、目标玩法和危险结构。

4. `services/algorithm_executor.py`
   解释执行 DSL，输出标准预测 payload。它只处理平台允许的字段、函数和操作符。

5. `services/algorithm_backtester.py`
   在历史开奖/赛果上重放用户算法，输出命中率、收益模拟摘要、样本量、跳过率和风险指标。

执行链路调整：

```text
预测方案
  -> engine_type = machine
  -> algorithm_key = builtin:football_odds_baseline_v1 或 user:123
  -> AlgorithmRegistry.resolve()
  -> 内置算法函数 或 用户算法 DSL 解释器
  -> 标准 prediction / prediction_items payload
```

## 数据模型

新增 `user_algorithms`：

```sql
CREATE TABLE user_algorithms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    lottery_type TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    algorithm_type TEXT NOT NULL DEFAULT 'dsl',
    definition_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    active_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
```

新增 `user_algorithm_versions`：

```sql
CREATE TABLE user_algorithm_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    algorithm_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    version INTEGER NOT NULL,
    change_summary TEXT NOT NULL DEFAULT '',
    definition_json TEXT NOT NULL,
    validation_json TEXT NOT NULL DEFAULT '{}',
    backtest_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (algorithm_id) REFERENCES user_algorithms(id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    UNIQUE(algorithm_id, version)
);
```

新增 `algorithm_chat_sessions`：

```sql
CREATE TABLE algorithm_chat_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    lottery_type TEXT NOT NULL,
    algorithm_id INTEGER,
    title TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'open',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (algorithm_id) REFERENCES user_algorithms(id)
);
```

新增 `algorithm_chat_messages`：

```sql
CREATE TABLE algorithm_chat_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES algorithm_chat_sessions(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
);
```

`predictors.algorithm_key` 扩展引用规则：

- `builtin:pc28_frequency_v1`
- `builtin:football_odds_baseline_v1`
- `user:123`

兼容规则：

- 老数据中没有前缀的 key 继续按内置算法处理。
- 序列化输出增加 `algorithm_source`：`builtin` / `user`。

## DSL v1

DSL v1 只做“字段特征 + 过滤条件 + 评分规则 + 决策规则”，保持简单可验证。

顶层结构：

```json
{
  "schema_version": 1,
  "method_name": "最近六场预测法",
  "lottery_type": "jingcai_football",
  "targets": ["spf", "rqspf"],
  "data_window": {
    "recent_matches": 6,
    "history_matches": 30
  },
  "filters": [],
  "features": [],
  "score": [],
  "decision": {},
  "explain": {}
}
```

### 字段白名单

竞彩足球 v1 字段：

- 基础：`league`、`match_time`、`home_team`、`away_team`
- 赔率：`spf_odds.win`、`spf_odds.draw`、`spf_odds.lose`
- 让球：`rqspf.handicap`、`rqspf.odds.win`、`rqspf.odds.draw`、`rqspf.odds.lose`
- 欧赔：`euro.initial.win`、`euro.current.win`、`euro.drop.win`
- 亚盘：`asia.line_changed`、`asia.home_trend`
- 近期：`home_recent_wins_n`、`away_recent_wins_n`、`home_goals_per_match_n`、`away_goals_per_match_n`、`home_conceded_per_match_n`、`away_conceded_per_match_n`
- 排名：`home_rank`、`away_rank`、`rank_gap`
- 伤停：`home_injury_count`、`away_injury_count`、`injury_advantage`
- 市场：`implied_probability_spf_win`、`implied_probability_spf_draw`、`implied_probability_spf_lose`

PC28 v1 字段：

- 基础：`result_number`、`big_small`、`odd_even`、`combo`
- 窗口统计：`number_frequency_n`、`combo_frequency_n`、`big_small_frequency_n`、`odd_even_frequency_n`
- 遗漏：`number_omission`、`combo_omission`、`big_small_omission`、`odd_even_omission`
- 趋势：`number_moving_avg_n`、`number_stddev_n`、`combo_transition_score`
- 时间：`current_hour`、`current_minute`、`issue_index_today`

字段白名单由平台维护，AI 只能引用白名单字段。字段不存在时校验失败，不进入执行。

### 操作符白名单

过滤操作符：

- `eq`
- `neq`
- `gt`
- `gte`
- `lt`
- `lte`
- `in`
- `between`

评分函数：

- `linear`
- `inverse`
- `bucket`
- `bonus_if`
- `penalty_if`
- `normalize`

聚合方式：

- `weighted_sum`
- `max_score`
- `min_risk`

禁止表达式：

- 任意代码
- 函数名字符串执行
- 正则回溯型表达式
- 动态字段路径拼接
- 递归、循环、外部引用

## 示例：进球率预测法

用户输入：

```text
我想做进球率预测法。重点看主队近 6 场进球率、客队近 6 场失球率，再参考主胜赔率。太热的 1.35 以下不要选，信心低就跳过。
```

AI 输出 DSL 草稿：

```json
{
  "schema_version": 1,
  "method_name": "进球率预测法",
  "lottery_type": "jingcai_football",
  "targets": ["spf"],
  "data_window": {
    "recent_matches": 6,
    "history_matches": 30
  },
  "filters": [
    {"field": "spf_odds.win", "op": "gte", "value": 1.35},
    {"field": "home_goals_per_match_6", "op": "gte", "value": 1.2}
  ],
  "score": [
    {"feature": "home_goals_per_match_6", "transform": "linear", "weight": 0.32},
    {"feature": "away_conceded_per_match_6", "transform": "linear", "weight": 0.28},
    {"feature": "implied_probability_spf_win", "transform": "linear", "weight": 0.24},
    {"feature": "injury_advantage", "transform": "linear", "weight": 0.16}
  ],
  "decision": {
    "target": "spf",
    "pick": "胜",
    "min_confidence": 0.58,
    "allow_skip": true
  },
  "explain": {
    "template": "主队近6场进球率 {home_goals_per_match_6}，客队失球率 {away_conceded_per_match_6}，主胜赔率 {spf_odds.win}"
  }
}
```

## 示例：六场预测法

用户输入：

```text
我只看双方最近六场，胜负平走势、净胜球、主客场状态都要算进去。赔率只作为校验，如果赔率和六场状态冲突就降低信心。
```

AI 输出 DSL 草稿：

```json
{
  "schema_version": 1,
  "method_name": "六场预测法",
  "lottery_type": "jingcai_football",
  "targets": ["spf", "rqspf"],
  "data_window": {
    "recent_matches": 6,
    "history_matches": 30
  },
  "score": [
    {"feature": "home_recent_wins_6", "transform": "linear", "weight": 0.22},
    {"feature": "away_recent_wins_6", "transform": "inverse", "weight": 0.18},
    {"feature": "home_goal_diff_per_match_6", "transform": "linear", "weight": 0.24},
    {"feature": "home_away_adjusted_form_6", "transform": "linear", "weight": 0.22},
    {"feature": "market_odds_consistency", "transform": "linear", "weight": 0.14}
  ],
  "decision": {
    "target": "spf",
    "pick": "max_score",
    "min_confidence": 0.6,
    "allow_skip": true,
    "confidence_penalty_when": [
      {"field": "market_odds_consistency", "op": "lt", "value": 0.45, "penalty": 0.12}
    ]
  },
  "explain": {
    "template": "近6场状态评分 {home_away_adjusted_form_6}，净胜球评分 {home_goal_diff_per_match_6}，赔率一致性 {market_odds_consistency}"
  }
}
```

## AI 聊天流程

页面入口：`/dashboard` 增加“我的算法”入口，或新增 `/algorithms` 页面。

核心流程：

1. 用户选择彩种。
2. 用户描述想法。
3. AI 判断信息是否足够。
4. 信息不足时，AI 只追问 1 到 3 个关键问题。
5. 信息足够时，AI 输出 DSL 草稿和人类可读说明。
6. 平台静态校验 DSL。
7. 校验通过后触发回测。
8. 页面展示回测摘要、样本量、命中率、跳过率、收益模拟摘要。
9. 用户可以继续说“更保守一点”“提高主胜门槛”“最近六场权重更高”。
10. AI 基于当前 DSL + 回测结果生成新版本。
11. 用户确认保存为“我的算法”。
12. 创建预测方案时选择该算法。

AI 输出协议：

```json
{
  "reply_type": "draft_algorithm",
  "message": "已按你的思路生成进球率预测法草稿。",
  "questions": [],
  "algorithm": {},
  "change_summary": "新增主队进球率、客队失球率和主胜赔率过滤。",
  "risk_notes": [
    "样本窗口只有 6 场，容易受赛程强弱影响。",
    "建议回测后再决定 min_confidence。"
  ]
}
```

当信息不足：

```json
{
  "reply_type": "need_clarification",
  "message": "还需要确认几个参数。",
  "questions": [
    "你希望最近几场作为窗口？",
    "赔率低于多少视为过热并跳过？",
    "信心不足时是跳过还是仍然给出预测？"
  ],
  "algorithm": null
}
```

## 后端接口设计

算法管理：

- `GET /api/user-algorithms?lottery_type=jingcai_football`
- `POST /api/user-algorithms`
- `GET /api/user-algorithms/<id>`
- `PUT /api/user-algorithms/<id>`
- `DELETE /api/user-algorithms/<id>`
- `POST /api/user-algorithms/<id>/validate`
- `POST /api/user-algorithms/<id>/backtest`
- `POST /api/user-algorithms/<id>/activate-version`

AI 聊天：

- `POST /api/algorithm-chat/sessions`
- `GET /api/algorithm-chat/sessions/<id>`
- `POST /api/algorithm-chat/sessions/<id>/messages`
- `POST /api/algorithm-chat/sessions/<id>/apply-draft`

预测方案兼容：

- `GET /api/predictors` 序列化增加 `algorithm_source`、`user_algorithm_id`、`user_algorithm_name`
- `POST /api/predictors` 接收 `algorithm_key = user:<id>`
- `PUT /api/predictors/<id>` 同上
- `POST /api/predictors/test` 对用户算法执行静态校验 + 小样本 dry run，不测试 AI 连通性

## 执行流程

PC28：

```text
PredictionEngine.generate_prediction()
  -> predictor.algorithm_key
  -> AlgorithmRegistry.resolve()
  -> builtin pc28 function 或 AlgorithmExecutor.execute_pc28()
  -> 标准 prediction payload
  -> db.upsert_prediction()
```

竞彩足球：

```text
JingcaiFootballService.generate_prediction()
  -> enrich_matches_for_prediction()
  -> AlgorithmRegistry.resolve()
  -> builtin football function 或 AlgorithmExecutor.execute_jingcai()
  -> 标准 prediction_items payload
  -> db.create_prediction_run() / db.upsert_prediction_items()
```

算法执行输出必须兼容现有字段：

PC28：

- `prediction_number`
- `prediction_big_small`
- `prediction_odd_even`
- `prediction_combo`
- `confidence`
- `reasoning_summary`

竞彩足球：

- `event_key`
- `match_no`
- `predicted_spf`
- `predicted_rqspf`
- `confidence`
- `reasoning_summary`

## 回测设计

回测不直接复用已落库预测结果，因为用户算法可能是新建的。回测应从历史开奖/赛果重放生成预测，再与实际结果比较。

PC28 回测：

- 使用历史开奖序列按时间滑窗重放。
- 每一期只允许使用该期之前的数据。
- 输出单点、大小、单双、组合命中率。

竞彩足球回测：

- 使用已结算赛事和对应历史赔率快照。
- 每个历史批次只允许使用赛前可获得字段。
- 优先使用结构化赔率、排名、近期战绩、伤停；缺字段时记录缺失率。
- 新浪接口历史详情并不保证全量稳定，回测报告必须展示字段覆盖率。

统一输出：

```json
{
  "sample_size": 180,
  "prediction_count": 126,
  "skip_count": 54,
  "skip_rate": 0.3,
  "hit_rate": {
    "spf": 0.57,
    "rqspf": 0.51
  },
  "confidence_buckets": [],
  "profit_summary": {},
  "field_coverage": {},
  "warnings": []
}
```

## 安全边界

必须强制执行：

- DSL schema 校验。
- 字段白名单。
- 操作符白名单。
- 权重和阈值范围限制。
- 最大规则数量限制。
- 最大特征数量限制。
- 最大回测样本限制。
- 最大执行时间限制。
- 禁止任意代码执行。
- 用户只能读取自己的算法。
- 创建方案时只能引用自己的 `user:<id>` 算法，管理员可另行设计共享策略。

建议默认限制：

- 单个算法最多 30 个 filters。
- 单个算法最多 40 个 score 项。
- 单次预测每场最多执行 200 个 DSL 节点。
- 单次回测最多 1000 条样本。
- 用户算法草稿最大 JSON 长度 32KB。

## 前端设计

新增“我的算法”页面，页面结构：

- 左侧：算法列表、彩种筛选、状态筛选。
- 中间：AI 聊天区。
- 右侧：算法草稿、校验结果、回测摘要。

创建预测方案弹窗调整：

- 执行引擎：`AI 模型` / `机器算法`
- 机器算法来源：`平台内置` / `我的算法`
- 平台内置：展示当前内置算法下拉。
- 我的算法：展示当前用户当前彩种已验证算法。
- 选择我的算法后，隐藏 API Key、模型名称、提示词字段。

## MVP 拆分

第一阶段：基础可用

- 新增用户算法表和版本表。
- 定义 DSL v1 schema。
- 实现 DSL 校验器。
- 实现竞彩足球 DSL 执行器。
- 实现算法列表、创建、编辑、保存。
- 预测方案支持选择 `user:<id>`。
- 先支持手工编辑 JSON，不做完整 AI 聊天。

第二阶段：AI 方案助手

- 新增算法聊天接口。
- AI 根据自然语言生成 DSL 草稿。
- AI 根据回测结果生成调整建议。
- 页面支持聊天、草稿预览、应用草稿。

第三阶段：回测闭环

- PC28 和竞彩足球回测器。
- 回测结果图表。
- 版本对比。
- 按命中率、收益、跳过率辅助调参。

第四阶段：高级能力

- 用户算法模板库。
- 算法复制与派生。
- 管理员审核后公开共享。
- 代码型算法沙箱。该阶段必须单独安全设计，不与 DSL MVP 混在一起。

## 与工程原则的对应

KISS：

- 第一版只做 DSL，不做任意代码沙箱。
- DSL v1 只包含过滤、评分、决策三类能力。

YAGNI：

- 暂不做算法市场、公开售卖、代码执行。
- 暂不做复杂图形化节点编辑器。

DRY：

- 内置算法和用户算法统一通过 `AlgorithmRegistry` 解析。
- PC28 与竞彩足球共享校验器框架，只分开彩种字段白名单和执行适配器。

SOLID：

- 用户算法管理、AI 聊天、校验、执行、回测分开。
- 预测引擎依赖算法解析抽象，不直接依赖具体用户算法实现。

## 验收标准

MVP 完成时应满足：

1. 用户可以创建一个竞彩足球“进球率预测法”算法。
2. 用户可以创建一个竞彩足球“六场预测法”算法。
3. 创建预测方案时可以选择自己的算法。
4. `predict-now` 能执行用户算法并生成标准预测结果。
5. 非算法所有者不能读取、引用或执行该算法。
6. 非法字段、非法操作符、超限规则会被拒绝。
7. 内置算法旧方案不受影响。
8. 用户算法执行不需要 API Key。
9. 回测报告展示样本量、命中率、跳过率和字段覆盖率。
10. 所有用户算法定义都能追踪版本。
