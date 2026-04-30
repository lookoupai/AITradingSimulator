# AITradingSimulator

基于 Flask + SQLite + OpenAI 兼容模型接口的多彩种 AI 预测平台，当前支持加拿大28（PC28）与竞彩足球。

## 核心能力

- 多用户登录与数据隔离
- 每个账号可创建多个预测方案
- 每个方案独立配置 `API Key`、`API URL`、模型名称、提示词、预测目标、温度、数据注入模式
- 支持 `pc28` 与 `jingcai_football` 两类彩种
- 自动轮询 `pc28.help` 官方接口同步开奖数据
- 自动轮询新浪竞彩足球接口同步待赛赛事、已赛赛果与比赛详情
- 支持方案连通性测试、提示词体检、提示词优化与立即预测
- 支持开奖/赛果回写、命中结算、统计看板与公开预测页

## 技术栈

- 后端：Python 3.9 / Flask
- 数据库：SQLite
- 前端：原生 JavaScript / ECharts
- AI 接口：OpenAI 兼容格式（DeepSeek、Gemini OpenAI-compatible、OpenAI 等）
- 数据源：
  - PC28：默认 `https://pc28.help`，当官方最近开奖接口不可用时自动回退 `https://jnd-28.vip/api/recent` 与 `https://feiji28.com/api/keno/latest`
  - 竞彩足球：新浪移动 JSON 接口为主，体彩 `webapi.sporttery.cn` 作为辅助探测/校验源

## 快速开始

### 本地运行

```bash
pip install -r requirements.txt
python app.py
```

启动后访问：

- 首页：`http://localhost:35008`
- 加拿大28公开页：`http://localhost:35008/pc28`
- 竞彩足球公开页：`http://localhost:35008/jingcai-football`
- 登录页：`http://localhost:35008/login`
- 控制台：`http://localhost:35008/dashboard`

### Docker

```bash
docker-compose up -d --build
```

## Codex 项目级 Skill

仓库内置了一个竞彩足球数据维护 skill，路径为 `.agents/skills/jingcai-football-data/`。

- 适用场景：维护新浪/体彩竞彩数据源、字段映射、抓取策略、详情补充与预测数据流。
- 主要内容：`SKILL.md`、`references/` 数据源参考、可选的 `agents/openai.yaml` 元数据。
- 附带脚本：`.agents/skills/jingcai-football-data/scripts/probe_jingcai_source.py`
- 目的：让从 GitHub 拉取本项目的开发者，可以直接在仓库里复用这份领域知识，而不是依赖某台机器上的全局 skill 目录。

## 设计文档

- [用户自定义算法与 AI 方案设计](docs/user_algorithm_design.md)：设计 AI 聊天生成用户自定义预测算法、DSL 校验执行、回测和预测方案接入。
- [用户算法使用说明](docs/user_algorithm_usage.md)：说明如何设计算法、理解回测、识别数据质量问题和避免收益承诺误判。

### 探测脚本示例

探测新浪历史赛事列表：

```bash
.venv/bin/python .agents/skills/jingcai-football-data/scripts/probe_jingcai_source.py --provider sina --date 2026-04-04 --is-prized 1
```

探测新浪列表并附带详情接口：

```bash
.venv/bin/python .agents/skills/jingcai-football-data/scripts/probe_jingcai_source.py --provider sina --date 2026-04-04 --is-prized 1 --include-details --detail-cat footballMatchDetail --detail-cat footballMatchOddsEuro
```

探测体彩接口当前返回形态：

```bash
.venv/bin/python .agents/skills/jingcai-football-data/scripts/probe_jingcai_source.py --provider sporttery
```

## 配置项

主要配置位于 `config.py`，也可通过环境变量覆盖：

```bash
HOST=0.0.0.0
PORT=35008
DATABASE_PATH=pc28_predictor.db
AUTO_PREDICTION=True
PREDICTION_POLL_INTERVAL=20
AI_GATEWAY_CONNECT_TIMEOUT=10
AI_GATEWAY_READ_TIMEOUT=35
AI_GATEWAY_TRANSPORT_ATTEMPTS=2
AI_GATEWAY_PREFERRED_BASE_URL_TTL_SECONDS=21600
PC28_PREDICTION_CUTOFF_BUFFER_SECONDS=20
PC28_PREDICTION_MIN_REQUEST_WINDOW_SECONDS=8
PC28_API_BASE_URL=https://pc28.help
PC28_REQUEST_TIMEOUT=10
PC28_SYNC_HISTORY=120
PC28_JND_RECENT_URL=https://jnd-28.vip/api/recent
PC28_FEIJI_RECENT_URL=https://feiji28.com/api/keno/latest
PC28_RECENT_SOURCE_ORDER=official,jnd,feiji
PC28_PREDICTION_RETENTION_DAYS=60
PC28_DRAW_RETENTION_DAYS=60
PC28_ARCHIVE_MAINTENANCE_INTERVAL=21600
PC28_ARCHIVE_VACUUM_INTERVAL=86400
JINGCAI_REQUEST_TIMEOUT=15
JINGCAI_DETAIL_CACHE_SECONDS=21600
JINGCAI_IDLE_INTERVAL=1800
JINGCAI_PREMATCH_INTERVAL=600
JINGCAI_NEAR_MATCH_INTERVAL=120
JINGCAI_SETTLEMENT_INTERVAL=300
JINGCAI_HISTORY_BACKFILL_ENABLED=True
JINGCAI_HISTORY_BACKFILL_INTERVAL_SECONDS=86400
JINGCAI_HISTORY_BACKFILL_LOOKBACK_DAYS=7
JINGCAI_HISTORY_BACKFILL_INCLUDE_DETAILS=True
```

PC28 历史数据默认采用“近 60 天保留完整明细，超期按方案+日期汇总后清理”的策略：

- `predictions` 只清理 `settled` / `failed` / `expired` 的老记录，`pending` 不会被误删
- `lottery_draws` 只清理未被老 `pending` 记录引用的开奖明细
- 汇总统计会保留长期命中率、总预测数和历史最长连中/连错
- 定时清理后会按配置周期执行 `VACUUM` 回收 SQLite 文件空间

Linux DO OAuth 仍然可用：

```bash
LINUXDO_CLIENT_ID=
LINUXDO_CLIENT_SECRET=
LINUXDO_REDIRECT_URI=
```

## 主要接口

### 认证

- `POST /api/auth/register`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/auth/me`

### 数据概览

- `GET /api/pc28/overview`：当前期号、倒计时、最近开奖、遗漏与今日统计
- `GET /api/lotteries/<lottery_type>/overview`：按彩种获取概览；当前支持 `pc28` 与 `jingcai_football`
- `GET /api/health`：服务健康检查

### 预测方案

- `GET /api/predictors`
- `POST /api/predictors`
- `GET /api/predictors/<id>`
- `PUT /api/predictors/<id>`
- `DELETE /api/predictors/<id>`
- `POST /api/predictors/test`
- `POST /api/predictors/prompt-check`
- `POST /api/predictors/prompt-optimize`
- `POST /api/predictors/prompt-template`
- `GET /api/predictors/<id>/dashboard`
- `GET /api/predictors/<id>/stats`
- `GET /api/predictors/<id>/simulation`
- `POST /api/predictors/<id>/predict-now`

### 用户算法

- `GET /api/user-algorithms`
- `POST /api/user-algorithms`
- `POST /api/user-algorithms/validate`
- `POST /api/user-algorithms/dry-run`
- `POST /api/user-algorithms/backtest`
- `GET /api/user-algorithms/templates`
- `POST /api/user-algorithms/<id>/ai-adjust`
- `POST /api/user-algorithms/<id>/adjust`
- `POST /api/user-algorithms/<id>/backtest`
- `GET /api/user-algorithms/<id>/versions`
- `GET /api/user-algorithms/<id>/execution-logs`
- `GET /api/user-algorithms/<id>/version-comparison`
- `POST /api/user-algorithms/<id>/activate-version`

### 竞彩足球运营

- `POST /api/jingcai-football/history-backfill`：管理员补齐历史赛果与赔率详情
- `GET /api/jingcai-football/data-health`：管理员查看本地竞彩足球历史数据覆盖率

### 公开接口

- `GET /api/public/predictors`
- `GET /api/public/predictors/<id>`
- `GET /api/public/predictors/<id>/simulation`

### 信号导出接口

- `GET /api/export/predictors/<id>/signals?view=execution`
- `GET /api/export/predictors/<id>/signals?view=analysis`
- `GET /api/export/predictors/<id>/performance`

## 预测流程

1. 后台线程按彩种轮询各自数据源
2. 将开奖/赛事/详情同步到本地数据库
3. 为启用中的方案生成下一期 PC28 预测或当前批次竞彩足球预测
4. 在开奖或赛果落地后回写实际结果并完成结算
5. 更新统计、看板与公开展示数据

## 说明

- 当前版本支持 **加拿大28 / PC28** 与 **竞彩足球**
- 平台负责拉取数据、组织提示词、调用模型、保存原始响应并结算结果
- “数学概率”“小六壬”“赔率 + 基本面”等方法论由方案提示词与彩种配置决定
- 竞彩足球当前默认数据源为新浪移动 JSON；体彩接口更适合作为校验或环境探测
- 所有预测仅供娱乐与研究参考，不构成收益承诺
