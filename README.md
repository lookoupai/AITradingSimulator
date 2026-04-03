# AI PC28 预测平台

基于 Flask + SQLite + OpenAI 兼容模型接口的加拿大28（PC28）预测平台。

## 核心能力

- 多用户登录与数据隔离
- 每个账号可创建多个预测方案
- 每个方案独立配置：
  - `API Key`
  - `API URL`
  - `模型名称`
  - `预测方法`
  - `自定义提示词`
  - `预测目标`（号码 / 大小 / 单双 / 组合）
- 自动轮询 `pc28.ai` 官方数据接口
- 开奖后自动结算命中率
- 命中统计与最近 20 期表现看板

## 技术栈

- 后端：Python 3.9 / Flask
- 数据库：SQLite
- 前端：原生 JavaScript / ECharts
- AI 接口：OpenAI 兼容格式（DeepSeek、Gemini OpenAI-compatible、OpenAI 等）
- 官方开奖数据：`https://www.pc28.ai/docs.html`

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

### 官方数据

- `GET /api/pc28/overview`：当前期号、倒计时、最近开奖、遗漏与今日统计
- `GET /api/health`：服务健康检查

### 预测方案

- `GET /api/predictors`
- `POST /api/predictors`
- `GET /api/predictors/<id>`
- `PUT /api/predictors/<id>`
- `DELETE /api/predictors/<id>`
- `GET /api/predictors/<id>/dashboard`
- `GET /api/predictors/<id>/stats`
- `POST /api/predictors/<id>/predict-now`

## 预测流程

1. 后台线程轮询 `pc28.ai`
2. 同步最近开奖到本地数据库
3. 为启用中的方案生成“下一期”预测
4. 开奖后自动结算：
   - 号码是否命中
   - 大小是否命中
   - 单双是否命中
   - 组合是否命中

## 说明

- 当前版本只支持 **加拿大28 / PC28**
- 平台只负责拉取历史开奖、组织提示词、调用模型和结算结果
- “数学概率”“小六爻”等方法论由方案提示词决定
- 所有预测仅供娱乐与研究参考，不构成收益承诺
