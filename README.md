# AITradingSimulator

基于 Flask + SQLite + OpenAI 兼容模型接口的多彩种 AI 预测平台，当前支持加拿大28（PC28）与竞彩足球。

## 核心能力

- 多用户登录与数据隔离
- 每个账号可创建多个预测方案
- 每个方案独立配置 `API Key`、`API URL`、模型名称、提示词、预测目标、温度、数据注入模式
- 支持 `pc28` 与 `jingcai_football` 两类彩种
- 自动轮询 `pc28.ai` 官方接口同步开奖数据
- 自动轮询新浪竞彩足球接口同步待赛赛事、已赛赛果与比赛详情
- 支持方案连通性测试、提示词体检、提示词优化与立即预测
- 支持开奖/赛果回写、命中结算、统计看板与公开预测页

## 技术栈

- 后端：Python 3.9 / Flask
- 数据库：SQLite
- 前端：原生 JavaScript / ECharts
- AI 接口：OpenAI 兼容格式（DeepSeek、Gemini OpenAI-compatible、OpenAI 等）
- 数据源：
  - PC28：`https://www.pc28.ai/docs.html`
  - 竞彩足球：新浪移动 JSON 接口为主，体彩 `webapi.sporttery.cn` 作为辅助探测/校验源

## 快速开始

### 本地运行

```bash
pip install -r requirements.txt
python app.py
```

启动后访问：

- 首页：`http://localhost:35008`
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
PC28_API_BASE_URL=https://www.pc28.ai
PC28_REQUEST_TIMEOUT=10
JINGCAI_REQUEST_TIMEOUT=15
JINGCAI_DETAIL_CACHE_SECONDS=21600
JINGCAI_IDLE_INTERVAL=1800
JINGCAI_PREMATCH_INTERVAL=600
JINGCAI_NEAR_MATCH_INTERVAL=120
JINGCAI_SETTLEMENT_INTERVAL=300
```

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

### 公开接口

- `GET /api/public/predictors`
- `GET /api/public/predictors/<id>`
- `GET /api/public/predictors/<id>/simulation`

### 信号导出接口

- `GET /api/export/predictors/<id>/signals?view=execution`
- `GET /api/export/predictors/<id>/signals?view=analysis`

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
