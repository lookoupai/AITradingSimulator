# 外部预测源插件与多用户外部方案 — 架构设计

- 版本：1.0
- 日期：2026-09-18
- 依据：`docs/external-predictions/prd.md`（PRD 1.0）
- 适用项目：`AITradingSimulator`（主体）、`pc28touzhu`（下游，仅离线契约验证，不改运行代码）

## 摘要

新增 `external` 预测引擎：管理员在后台维护“外部来源”（首个插件 `jnd28`，对应 `https://jnd-28.vip/api/ai-predict`），后端单线程共享采集整批模型预测并按期去重留档；普通用户创建“外部预测”方案，绑定来源＋模型＋玩法（大小/单双/组合），复用现有 predictions 表、结算、统计与信号导出。导出路由补充分享等级访问控制与按方案授权 token。下游 `pc28touzhu` 无需改动即可消费新方案信号（自定义请求头透传 token）。

## 1. 真实接口样本确认（2026-09-18 只读抓取）

`GET https://jnd-28.vip/api/ai-predict`（无需鉴权，约 405KB）：

```json
{
  "draw_number": 3483593,                       // 目标期号（int，等于本地下一期）
  "predict_time": "2026-09-18T23:13:35.313074695+08:00",  // 上游发布时间（带时区）
  "models": [                                    // 本样本 100 个模型
    {
      "model_type": "quantum",                   // 稳定标识（slug）
      "model_name": "理论概率偏差",                // 展示名
      "predicted_big": true, "predicted_odd": true, "predicted_group": "大单",
      "predicted_numbers": [0, 1, ...],
      "confidence": 72.49,
      "items": [                                 // 20 项：scope×category
        {"key": "sum_big",  "scope": "sum", "category": "big",   "value": "大"},
        {"key": "sum_odd",  "scope": "sum", "category": "odd",   "value": "单"},
        {"key": "sum_group","scope": "sum", "category": "group", "value": "大单"},
        {"key": "sum_kill_group", ...}, {"key": "sum_number", ...},
        {"key": "ball1_big", "scope": "ball1", ...}, ...]
    }
  ]
}
```

- 本地和 `lottery_draws` 最新期号 `3483592` 与接口 `draw_number=3483593` 严格衔接；`/api/countdown` 显示开奖节奏约 205 秒/期。
- 本期仅解析 `scope=='sum'` 的 `big/odd/group` 三类；`number`、`kill_group`、`ball1/2/3` 不解析（PRD 玩法限定）。
- 值域白名单：`big→大|小`，`odd→单|双`，`group→大单|大双|小单|小双`；白名单外值跳过该目标。
- `/api/ai-prediction-history` 本期不接入（不能替代实时获取时间证明）。

## 2. 概念模型与数据表

沿用“接入插件 → 来源连接 → 模型目录 → 用户方案”四层。新增 5 张表 + `predictions` 增列（迁移全部走现有 `init_db()` 的 `CREATE TABLE IF NOT EXISTS` + try/except `ALTER TABLE ADD COLUMN` 模式，database.py:648-735）。

### 2.1 `external_sources`（来源连接，管理员维护）

| 列 | 说明 |
|---|---|
| id, plugin_key | 插件标识，本期仅 `jnd28` |
| name | 展示名 |
| base_url | 来源基址（默认 `https://jnd-28.vip`，HTTPS） |
| enabled | 停用后采集停止、方案不再产出新预测 |
| interval_seconds | 采集间隔，钳制 ≥`EXTERNAL_SOURCE_MIN_INTERVAL_SECONDS`(30)，默认 60 |
| extra_config | JSON 预留（本期插件不用） |
| last_attempt_at / last_success_at / last_error / last_error_at / last_target_issue / last_model_count | 运行状态（供后台展示与观测） |

### 2.2 `external_models`（模型目录，刷新目录时动态更新）

`id, source_id, model_key(model_type), display_name(model_name), enabled(默认0，管理员开放), first_seen_at, last_seen_at, UNIQUE(source_id, model_key)`

- 刷新只新增/更新目录行；**已开放模型上游消失时不删除行**（UI 标“最近未见”），满足“下架模型不得静默切换”。
- 模型支持目标固定 `['big_small','odd_even','combo']`（sum scope 提供三类时才完整；缺类以实际解析结果为准，存 `supported_targets`）。

### 2.3 `external_prediction_batches`（采集批次，共享原始证据）

| 列 | 说明 |
|---|---|
| id, source_id | |
| target_issue_no | 接口 `draw_number`（文本） |
| upstream_published_at | 接口 `predict_time`（保留原始字符串） |
| fetched_at | 本地首次获取时间（UTC ISO） |
| fingerprint | 决策内容指纹：`sha256(target_issue_no ‖ 逐模型(model_key+big_small+odd_even+combo))`，confidence 不参与（元数据抖动不产生新版本） |
| batch_version | 同 issue 内递增（1=首个版本） |
| model_count / invalid_model_count | 解析统计 |
| raw_payload | 整批原始 JSON，`zlib` 压缩 BLOB（约 405KB→~40KB） |

`UNIQUE(source_id, target_issue_no, fingerprint)`：同决策内容重复取得不新增行（保留首次 fetched_at）；上游改写预测 → 新指纹 → 新版本行，用于审计。

### 2.4 `external_predictions`（已采用快照，按需创建）

| 列 | 说明 |
|---|---|
| id, batch_id, source_id, model_key, lottery_type='pc28', issue_no | |
| upstream_published_at / fetched_at | 承接批次 |
| prediction_big_small / prediction_odd_even / prediction_combo | 白名单内的值或 NULL |
| confidence | 上游评分（仅展示，不与本地命中率混淆） |
| model_payload | 该模型的 JSON 切片（原始证据） |
| fingerprint, created_at | |

`UNIQUE(source_id, model_key, issue_no, fingerprint)`。**只在用户方案实际采用时创建**（见 §4），存储规模 O(订阅模型×期数)，不为无订阅模型逐期复制。

### 2.5 `predictor_export_tokens`（按方案导出授权）

`id, predictor_id, token_hash(sha256, UNIQUE), token_prefix(前8位展示), label, created_at, last_used_at, revoked_at`

- 明文 token `pts_<32hex>` 仅创建时返回一次；校验用 sha256 常量时间比较；不可跨方案；可撤销。
- 下游通过 `Authorization: Bearer <token>` 或 `X-Export-Token: <token>` 请求头传递（pc28touzhu `fetch.headers` 原样透传，已确认）。

### 2.6 `predictions` 增列（外部引擎溯源）

`external_source_id INTEGER`、`external_model_key TEXT`、`external_fetched_at TEXT`（全部可空；AI/机器算法行保持 NULL）。`raw_response` 复用为该模型 JSON 切片文本（分析视图 `raw.raw_response` 呈现，不含来源连接信息）。

## 3. 插件接口（`services/external_sources/`）

```python
# base.py
@dataclass
class ExternalModelSnapshot:
    model_key: str; display_name: str
    targets: dict[str, str]            # {'big_small':'大','odd_even':'单','combo':'大单'}
    confidence: float | None
    raw: dict                          # 模型级原始切片

@dataclass
class ExternalSnapshot:
    target_issue_no: str
    upstream_published_at: str | None
    models: list[ExternalModelSnapshot]
    invalid_model_count: int
    raw: bytes                         # 整批原始响应（用于压缩存档）

class ExternalSourcePlugin(Protocol):
    plugin_key: str; display_name: str
    def fetch_snapshot(self, base_url: str, timeout: float) -> ExternalSnapshot: ...
    def validate_base_url(self, base_url: str) -> bool: ...   # 仅允许 https（或本地测试 http）
```

- `jnd28.py`：解析规则见 §1；`items` 中 `scope=='sum'` 的 `big/odd/group` 三项；模型缺 name/type、值域外 → 该模型计入 invalid，不影响其他模型（FR-002 单模型异常隔离）。
- `registry.py`：`PLUGIN_REGISTRY = {'jnd28': Jnd28Plugin}`；未知 plugin_key 拒绝创建来源。插件清单经 `GET /api/admin/external-sources` 的 `plugins` 字段动态暴露，前端"添加来源"下拉框据此渲染；新增插件的步骤与通用 JSON 字段映射扩展点见 `docs/external_source_plugins.md`。
- 网络统一走 `requests`，超时 `EXTERNAL_SOURCE_REQUEST_TIMEOUT`(15s)，响应体上限 `EXTERNAL_SOURCE_MAX_RESPONSE_BYTES`(2MB)。

## 4. 共享采集与采用流水线

### 4.1 采集调度（新独立线程，与 AI 请求隔离）

`app.py::external_source_loop`（daemon thread + `scheduler_state` 锁 `external_source_collector`，沿用现有线程模式 app.py:327-531）：

1. 每 5s 醒来；对每个 `enabled` 来源，距 `last_attempt_at` ≥ `max(interval_seconds, MIN_INTERVAL)` 时触发一次采集（失败退避：连续失败按 30s×2^n 封顶 `EXTERNAL_SOURCE_FAILURE_BACKOFF_MAX_SECONDS`(300s)）。
2. `fetch_snapshot` → 校验 → 计算 fingerprint → `INSERT OR IGNORE` 批次（冲突则不更新 fetched_at，FR-004 去重）。
3. 更新来源运行状态列；`EXTERNAL_COLLECTOR_ENABLED=false`（测试默认）时线程不启动。
4. 保留期维护：删除 `fetched_at` 早于 `EXTERNAL_SOURCE_RAW_RETENTION_DAYS`(3天) 的批次行及其 adopted 快照引用置空处理（快照行本身随用户 predictions 保留期清理，见 §4.4）。

用户刷新前台页面**不触发**外部 HTTP 请求（FR-002），采集只发生在该线程与手动“测试连接/刷新目录”管理操作。

### 4.2 采用（adoption）规则

在 `PredictionEngine.run_auto_cycle` / `generate_prediction` 的 external 分支中执行（`services/external_prediction_service.py::adopt_prediction(db, predictor, context)`）：

1. `next_issue = next_issue_no(本地最新已开奖期号)`；无本地开奖 → 跳过（等待更新，非异常）。
2. 该方案已有 `predictions(issue_no=next_issue)` 行 → 跳过（**首个有效版本已固定，不被后续版本覆盖**）。
3. 取该 (source, model) 在 `target_issue_no == next_issue` 的**最早版本批次**（min batch_version）；无 → 跳过。
4. 资格判定：本地 `lottery_draws` 无该 issue 行（未开奖）且批次 `fetched_at` 早于当前时间；issue 与本地下一期不一致 → 跳过并记 runtime 说明。
5. 从批次 raw_payload 提取模型切片 → 白名单校验目标值 → 写 `external_predictions`（幂等，UNIQUE 去重）→ 写 `predictions` 行：
   - `status='pending'`、`requested_targets=方案目标∩快照非空目标`、`confidence=上游评分`、`reasoning_summary="外部来源 JND·<模型名> 期号<issue>"`、`raw_response=模型切片 JSON`、provenance 三列、`prediction_big_small/odd_even/combo` 按目标映射。
6. 缺失目标字段：只跳过该目标（导出与结算自然排除），方案其余目标继续（FR-004）。

外部模型未更新、来源停用、模型未开放 → 方案本期无行（不生成 failed 行、不触发 AI 故障保护、不重试上游）。

### 4.3 结算（FR-005）

复用 `PredictionEngine.settle_pending_predictions()`：external 行按现有 `_evaluate_hit` 字符串相等规则结算（本地 `lottery_draws` 的 `big_small/odd_even/combo`）。**外部引擎附加回溯资格校验**：若该 issue 的 `draw_date+draw_time`（北京时间）早于 `external_fetched_at`（UTC），说明采集发生在开奖后 → 置 `status='expired'`、`error_message='采集时该期已开奖，不计入实时统计'`，不计命中样本（FR-004/005）。已结算行为幂等（结算后非 pending，不再进入队列）。

### 4.4 保留与清理

- `predictions`/批次随现有保留任务清理；`external_predictions` 在其 batch 被清理后仍独立保留（adopted 证据不依赖批次行），随方案删除级联清理。
- 批次 raw_payload 保留 3 天（可配），满足“原始证据”排查窗口；已采用快照的 `model_payload` 长期保留（与 predictions 60 天一致）。

## 5. 用户方案（FR-003）

- `utils/predictor_engine.py`：`ALLOWED_ENGINE_TYPES += ('external',)`，标签“外部预测”，`uses_ai_engine('external')=False`。
- `predictors` 增列 `external_source_id`、`external_model_key`。
- `_validate_predictor_payload`（app.py:2040）external 分支：
  - 仅 `lottery_type='pc28'`；目标 ⊆ `{big_small, odd_even, combo}` 且非空（**不强制 number**，区别于 AI/machine）；
  - 主玩法、默认收益玩法 ∈ 目标；
  - 来源必须存在且 enabled；模型必须在目录中且 enabled；模型不支持所选目标时报具体错误（“模型 X 未开放/来源已停用/不支持目标 Y”）；
  - 不要求 api_key/api_url/model_name/system_prompt（表单与校验均跳过 AI 字段）。
- `GET /api/external/options`（@login_required）：返回启用来源＋开放模型（含 supported_targets），供方案表单选择。
- 修改绑定只影响后续预测；历史行保留自己的 provenance 三列与快照。

## 6. 导出与访问控制（FR-006）

### 6.1 路由行为

`GET /api/export/predictors/<id>/signals?view=execution|analysis` 增加访问层（schema 1.0 与既有字段不变）：

| 访问者 | execution | analysis |
|---|---|---|
| 所有者/管理员会话 | 允许 | 允许（含 raw） |
| 有效方案 token | 允许 | 允许（含 raw） |
| 匿名 | 仅 `share_level ∈ {records, analysis}` 且方案 enabled 时允许 | 同左，且**剥离 `raw.prompt_snapshot/raw_response`**（analysis 不得暴露原始响应/提示词） |
| 匿名 + `stats_only` 或私有 | 403 | 403 |

- 403/404 响应不泄露方案内容（统一 `{'error': '...'}`）。
- token 校验：`Authorization: Bearer` 或 `X-Export-Token`；命中后记 `last_used_at`；revoked/不存在 → 视为匿名。
- **旧 AI/机器方案行为变化仅限**：`stats_only` 方案的匿名导出从“可读”收紧为 403（PRD 已授权）；`records/analysis` 公开方案匿名执行导出保持可用。共识导出（`/api/export/consensus/*`）不在本期改动范围。

### 6.2 external execution 有效性门槛（仅 external 引擎）

导出前逐项校验，任一不满足 → `items: []`（不是错误）：方案 enabled、来源 enabled、模型 enabled、`status=='pending'`、issue 未开奖（本地无 draw）、issue == 本地下一期。`published_at` 用 `external_fetched_at`（采集时间，稳定）；`signal_id` 沿用 `pc28-predictor-<id>-<issue>`，轮询稳定不重复（FR-006）。`source_ref` 增加 `engine_type='external'`、`source_name`、`model_key/model_name`、`fetched_at`；不带任何连接配置或整批其他模型内容。

### 6.3 历史与分析

- 已结算历史走现有历史页/`analysis`（授权规则同 §6.1）；external 行的 `prediction` 区块含模型名与上游 confidence，`raw.raw_response` 为模型切片。
- 统计口径：本地样本=已结算且非 expired 行；上游 confidence 单独展示，不混入命中率（FR-005）。

## 7. 管理端 API（@admin_required）

| 方法/路径 | 作用 |
|---|---|
| GET /api/admin/external-sources | 列表＋运行状态＋模型计数 |
| POST /api/admin/external-sources | 创建（plugin_key 白名单；默认 enabled=0） |
| PUT /api/admin/external-sources/<id> | 改名/基址/间隔/启停 |
| DELETE /api/admin/external-sources/<id> | 删除来源（级联清除模型目录/批次/共享快照；绑定方案停止产出新预测，历史行与来源标识保留；响应含 affected_predictors） |
| POST /api/admin/external-sources/<id>/test | 一次性连通测试（不落库），返回模型数/目标期号/耗时/错误 |
| POST /api/admin/external-sources/<id>/refresh-catalog | 采集并刷新目录，返回新增/未见模型 |
| GET /api/admin/external-sources/<id>/models | 模型目录（分页可选，含 enabled） |
| PUT /api/admin/external-sources/<id>/models | 批量设置模型 enabled |
| GET /api/admin/external-sources/<id>/batches?limit= | 最近采集批次（期号/指纹版本/获取时间/模型数） |

普通用户写操作一律 403（`@admin_required`）；聚合状态并入 `/api/admin/dashboard`（`external_sources` 键）供任务状态 panel 显示。

## 8. 下游契约（FR-007）

- 导出结构不变：`{items:[{signal_id, issue_no, published_at, source_ref, signals:[{bet_type, bet_value, confidence, message_text, normalized_payload}]}]}`；`bet_value` 用 大/小/单/双/大单/大双/小单/小双，与 pc28touzhu `settlement_rules._resolve_metric_from_signal` 匹配。
- 空期返回 `items: []` → 下游 `upstream_no_signal` 正常跳过。
- 下游忽略未知字段（`expires_at` 等即便添加也安全）；本期新鲜度由 `published_at`（=采集时间）承载。
- 验证：在 pc28touzhu 仓库新增离线 pytest（临时库＋固定样本＋固定时钟），用真实导出 builder 产物走 SourceFetchService→normalize→去重→过期→结算链路；不改其生产代码。

## 9. 配置新增（config.py）

```
EXTERNAL_COLLECTOR_ENABLED=True          # 测试环境默认 False（tests/support.py 注入）
EXTERNAL_SOURCE_REQUEST_TIMEOUT=15
EXTERNAL_SOURCE_MIN_INTERVAL_SECONDS=30
EXTERNAL_SOURCE_DEFAULT_INTERVAL_SECONDS=60
EXTERNAL_SOURCE_MAX_RESPONSE_BYTES=2000000
EXTERNAL_SOURCE_FAILURE_BACKOFF_MAX_SECONDS=300
EXTERNAL_SOURCE_RAW_RETENTION_DAYS=3
```

## 10. 测试设计（AITradingSimulator，unittest + 既有 harness）

| 文件 | 覆盖 |
|---|---|
| tests/test_external_sources_admin.py | 插件解析（固定 405KB 样本切片）、值域白名单、单模型异常隔离；admin API 权限（普通用户 403）；创建/启停/刷新目录/模型开关；批次指纹去重与版本递增 |
| tests/test_external_predictors.py | external 方案校验（目标限制、来源/模型绑定错误）；采用规则（下一期匹配、首版本固定、已开奖跳过、缺目标跳过）；结算命中与回溯 expired |
| tests/test_external_export.py | execution 有效性门槛（停用/已开奖/过期→items:[]）；signal_id/published_at 稳定；share_level 匿名门槛（records 放行、stats_only 403）；token 授权/撤销/不可跨方案；匿名 analysis 剥离 raw |
| 回归 | 全量 `python -m unittest discover tests`；pc28touzhu 新增契约测试通过 |

## 11. 实施顺序与文件清单

1. `config.py`（§9）→ `utils/predictor_engine.py`（引擎枚举）→ `database.py`（表/列/CRUD）
2. `services/external_sources/{__init__,base,jnd28,registry}.py` → `services/external_prediction_service.py`（采集、采用、结算辅助）
3. `services/prediction_engine.py`（external 分支）→ `app.py`（采集线程、校验分支、admin/user API、导出访问层）
4. 前端：`templates/admin.html`+`static/admin.js`（来源管理 panel）；`templates/dashboard.html`+`static/app.js`（external 表单区块、导出 token 管理）
5. 测试与 pc28touzhu 契约验证 → 文档（`docs/external-predictions/rollout.md` 启用说明）

## 12. 风险对策落实

- 上游 414KB 大响应：压缩存档＋响应体上限＋有界间隔＋退避。
- 上游改写/回填：决策内容指纹版本化＋首版本固定＋采集时间回溯校验。
- 本地开奖延迟：采用时以“本地下一期且未开奖”为准；结算回溯 expired 纠偏。
- 导出收紧影响既有连接：`records/analysis` 保持匿名可用；`stats_only` 收紧由 token/会话兜底，交付说明中提示下游配置 headers。
- 与 AI 互相阻塞：独立采集线程＋external 采用为纯本地操作，不占用 AI 锁内网络时间。
