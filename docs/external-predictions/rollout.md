# 外部预测源 — 交付与启用说明

- 日期：2026-09-18
- 关联：`prd.md` 1.0、`architecture.md` 1.0、`ui-spec.md` 1.0
- 状态：代码与离线验证完成；按 PRD 约定未启动/重启线上服务、未执行真实投注、未推送代码。

## 1. 交付内容

### AITradingSimulator（上游）

| 模块 | 文件 | 说明 |
|---|---|---|
| 引擎枚举 | `utils/predictor_engine.py` | 新增 `external` 引擎（外部预测），执行标签/描述适配 |
| 配置 | `config.py` | `EXTERNAL_COLLECTOR_ENABLED`、请求超时/间隔/退避/响应上限/原始载荷保留期（见 §4） |
| 数据层 | `database.py` | 新表 `external_sources` / `external_models` / `external_prediction_batches` / `external_predictions` / `predictor_export_tokens`；`predictors` 与 `predictions` 增加外部溯源列；CRUD 与幂等 upsert |
| 插件 | `services/external_sources/{base,jnd28,registry}.py` | 接入插件接口 + JND 实现（`/api/ai-predict`，只解析和值大小/单双/组合，值域白名单，单模型异常隔离）；插件注册表经管理 API 动态暴露，"添加来源"下拉框自动列出全部插件 |
| 插件扩展指南 | `docs/external_source_plugins.md` | 新增来源插件的步骤与最小示例；通用 JSON 字段映射接入器（jsonmap）的结构与扩展点设计（P1） |
| 核心服务 | `services/external_prediction_service.py` | 共享采集与批次指纹去重、目录刷新、采用规则（首版本固定）、结算回溯资格校验、导出有效性门槛 |
| 执行引擎 | `services/prediction_engine.py` | external 分支（纯本地采用，不触发网络/AI 故障保护）；结算前回溯校验 |
| 应用层 | `app.py` | 采集守护线程（`external_source_loop`，scheduler_state 锁）；方案校验 external 分支；`/api/admin/external-sources*` 管理 API；`/api/external/options`；导出 Token CRUD；导出路由访问控制（share_level + Bearer/X-Export-Token）与 external 有效性门槛；导出 `source_ref` 增加 engine/source/model/fetched_at 元数据 |
| 管理前端 | `templates/admin.html` + `static/admin.js` | 新增“外部预测源”panel：来源列表/状态/编辑/启停、测试连接、刷新目录、模型目录开放、最近批次（上游修订标记）、采集线程心跳 |
| 用户前端 | `templates/dashboard.html` + `static/app.js` | 新建/编辑方案支持“外部预测”引擎（来源+模型下拉、目标限定大小/单双/组合、隐藏 AI 字段与提示词）；方案列表外部徽标与异常状态文案；导出授权 Token 管理（生成仅显示一次/撤销）；`stats_only` 匿名导出收紧提示 |

### pc28touzhu（下游）

- 新增 `tests/test_external_source_contract.py` + 固定样本 `tests/fixtures/ai_trading_simulator_external_export_sample.json`（由上游真实导出链路生成）。
- **生产代码零改动**：现有 `ai_trading_simulator_export` 来源即可消费新方案导出；`fetch.headers` 原样透传 Token。

## 2. 验证结果

| 验证 | 结果 |
|---|---|
| 上游全量回归（含 44 个新测试） | `python -m unittest discover tests` → 203 tests OK |
| 下游全量回归（含 5 个新契约测试） | 437 tests OK |
| 真实 API 只读端到端 | 采集 100 模型/0 异常 → 目录刷新 → 采用期号 3483612 → 导出 大小+单双（confidence 71.9，`signal_id=pc28-predictor-1-3483612`） |
| 契约要点 | `items:[]` 下游正常跳过；重复抓取复用 raw_item；Token 经 `Authorization: Bearer` 透传；schema 1.0 字段不变 |

## 3. 启用步骤（管理员）

1. 重启服务使新代码生效（本次交付未重启）。
2. 登录管理后台 → “外部预测源” panel → “添加来源”。JND 无需凭据（默认 `https://jnd-28.vip`，间隔 60 秒）；选择“与28开放平台”时必须填写在其平台生成的 API Key（`yu28_` 开头，个人 Key 免费不限流，禁止放 URL，本系统走 `X-Api-Key` 请求头）。
3. 点击 **测试连接** 确认可达（模型数/目标期号/耗时）。
4. 点击 **刷新目录** 拉取模型目录（首次约 100 个，全部默认不开放）。
5. 在 **模型目录** 中开放计划提供的模型（可“全部开放”或挑选）。
6. 确认无误后 **启用采集**（默认创建后为停用状态）。
7. 加错的来源可直接 **删除来源**（红色按钮，二次确认）：模型目录、采集批次与共享快照一并清除；若有用户方案绑定，删除后这些方案停止产出新预测，但历史记录与来源标识保留，删除不可恢复。
7. 采集线程每 5 秒自检、按来源间隔抓取；失败自动退避（30s→300s 封顶），状态在 panel 与任务状态中可见。

## 4. 配置项（环境变量，均可选）

| 变量 | 默认 | 说明 |
|---|---|---|
| `EXTERNAL_COLLECTOR_ENABLED` | True | 采集线程开关 |
| `EXTERNAL_SOURCE_DEFAULT_INTERVAL_SECONDS` | 60 | 新来源默认采集间隔 |
| `EXTERNAL_SOURCE_MIN_INTERVAL_SECONDS` | 30 | 间隔下限（对方限流 60 次/分钟/IP） |
| `EXTERNAL_SOURCE_REQUEST_TIMEOUT` | 15s | 单次请求超时 |
| `EXTERNAL_SOURCE_MAX_RESPONSE_BYTES` | 2MB | 响应体上限 |
| `EXTERNAL_SOURCE_FAILURE_BACKOFF_MAX_SECONDS` | 300 | 失败退避上限 |
| `EXTERNAL_SOURCE_RAW_RETENTION_DAYS` | 3 | 整批原始载荷保留天数（已采用快照随方案保留 60 天） |

## 5. 下游（pc28touzhu）对接

1. 来源类型 `ai_trading_simulator_export`，URL 填方案导出地址：
   `https://<上游域名>/api/export/predictors/<方案ID>/signals?view=execution`
2. 公开分享（records/analysis）方案：匿名即可；`只公开统计` 或私有方案需配置 Token：
   ```json
   { "fetch": { "url": "...", "headers": { "Authorization": "Bearer pts_xxxx" } } }
   ```
   Token 在上游方案编辑弹窗“导出授权 Token”区生成（仅显示一次，可撤销，不跨方案）。
3. `fetch.headers` 会明文存入下游来源配置，注意保管。

## 6. 行为与口径要点

- **共享采集**：所有用户方案共用一次上游请求；用户刷新页面不触发外部 HTTP。
- **实时资格**：预测采用与结算均校验“采集时该期未开奖”；开奖后取得的记录标记 `expired`（不计入本地命中率样本），原始批次仅用于排查。
- **首版本固定**：上游修订同一期预测产生新版本批次用于审计，已采用/已导出的用户预测不被覆盖。
- **本地命中率**：仅统计本地开奖结算结果；上游 `confidence` 只作“上游评分”展示，与本地命中率区分。
- **导出收紧**：`stats_only` 方案的匿名信号导出从“可读”变为 403（PRD 授权的变更）；`records/analysis` 公开方案匿名 execution 导出保持不变；匿名 `analysis` 不再返回原始响应与提示词。**若下游正在匿名消费 `stats_only` 方案，需按 §5 配置 Token。**
- 共识导出（`/api/export/consensus/*`）不在本期改动范围。

## 7. 已知边界与后续（对应 PRD §5）

- 仅解析和值大小/单双/组合；号码集合、杀组合、分球玩法未接入。
- 不含浏览器抓取、Webhook、历史导入、私有用户 API 凭据。
- 模型高度相关，同源 100 模型不可视为独立共识证据（后续共识权重设计）。
- 长期稳定性取决于上游免费政策；插件隔离便于切换域名或更换来源。
