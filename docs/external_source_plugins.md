# 外部预测源接入插件 — 扩展指南

- 日期：2026-09-18
- 对应需求：PRD FR-001（插件提供统一的目录读取、预测解析和标准化约定；通用 JSON 字段映射的结构和扩展点有文档）
- 代码位置：`services/external_sources/`

## 1. 插件体系与四层概念的对应

上一轮讨论确定的四层概念在代码中的落点：

| 概念 | 代码落点 | 谁维护 |
|---|---|---|
| 接入插件 | `services/external_sources/` 下的插件类 + `registry.py` 注册表 | 开发者（本期内置 `jnd28`） |
| 来源连接 | `external_sources` 表 + 管理后台"外部预测源" panel | 管理员 |
| 外部预测计划 | `external_models` 表（来源刷新目录时动态更新） | 管理员开放 |
| 用户方案 | `predictors` 表 `engine_type='external'` 行（绑定来源+模型+玩法） | 普通用户 |

**关键约定：一个网站只需一个接入插件**。网站换域名 → 管理员改来源基址；模型增减 → 刷新目录；都不需要改代码。新增一个来源网站 = 新增一个插件文件。

## 2. 插件接口规范

插件类放在 `services/external_sources/` 下，需要实现：

```python
class MyNewPlugin:
    plugin_key = 'mynew'            # 稳定标识（写入 external_sources.plugin_key）
    display_name = '新来源'          # 管理后台展示名

    def validate_base_url(self, base_url: str) -> bool:
        """安全约束：生产环境必须 HTTPS；仅测试环境允许 http 回环地址。"""

    def predict_url(self, base_url: str) -> str:
        """实时预测接口的完整 URL。"""

    def fetch_snapshot(self, base_url: str, timeout: float | None = None) -> ExternalSnapshot:
        """请求接口并解析为标准化快照。"""
```

`ExternalSnapshot`（`base.py`）标准化结构：

```python
ExternalSnapshot(
    target_issue_no='3483612',                    # 目标期号（字符串）
    upstream_published_at='...+08:00',            # 上游发布时间（原始字符串，可时区）
    models=[                                      # 全部模型
        ExternalModelSnapshot(
            model_key='quantum',                  # 稳定模型标识
            display_name='理论概率偏差',            # 展示名
            targets={'big_small': '小',            # 仅支持三个目标键
                     'odd_even': '单',
                     'combo': '大单'},
            confidence=71.9,                      # 上游评分，仅展示用
            raw={...}                             # 模型级原始切片（采用时作为证据留档）
        )
    ],
    invalid_model_count=0,                        # 解析失败但被隔离的模型数
    raw=b'...'                                    # 整批原始响应字节（压缩留档）
)
```

**强制规则**（框架不强制、插件必须遵守，代码评审把关）：

1. **值域白名单**：`targets` 的值必须是本地开奖属性口径——`big_small ∈ {大, 小}`、`odd_even ∈ {单, 双}`、`combo ∈ {大单, 大双, 小单, 小双}`（`base.py::EXTERNAL_TARGET_VALUES`）。白名单外的值跳过该目标，不得猜测转换。
2. **只表达真实预测**：不从号码集合、杀组合、分球预测推导另一种玩法；缺失目标留空。
3. **单模型异常隔离**：单个模型解析失败计入 `invalid_model_count`，不得让整批失败。
4. **不伪造时间**：上游没有发布时间就传 `None`，本地首次获取时间由采集层记录。
5. **不做存储/调度**：插件只负责请求与解析；批次留档、去重、采用、结算由 `services/external_prediction_service.py` 统一处理。
6. **有界请求**：必须走 `base.py::fetch_json`（超时 + 响应体上限），不得自行无限重试。

## 3. 新增一个来源插件的步骤

1. 新建 `services/external_sources/mynew.py`，实现上节接口，解析出 `ExternalSnapshot`。
2. 在 `services/external_sources/registry.py` 注册：
   ```python
   from services.external_sources.mynew import MYNEW_PLUGIN
   PLUGIN_REGISTRY = {
       JND28_PLUGIN.plugin_key: JND28_PLUGIN,
       MYNEW_PLUGIN.plugin_key: MYNEW_PLUGIN,
   }
   ```
3. 重启服务。管理后台"外部预测源 → 添加来源"的插件下拉框会自动出现新插件（列表来自 `GET /api/admin/external-sources` 的 `plugins` 字段，动态渲染）。
4. 添加测试：仿照 `tests/test_external_sources.py`（固定样本解析、值域隔离、批次去重）。

用户方案、本地结算、信号导出、pc28touzhu 消费链路**无需任何改动**——它们只依赖标准化后的模型快照与 predictions 表。

## 4. 通用 JSON 字段映射接入器（P1 扩展点设计）

对于"有标准 JSON API、字段结构与 JND 不同"的来源，计划提供**免写代码**的通用接入器：管理员在表单里配置映射，由一个内置插件 `jsonmap` 按配置解析。本期未实现（PRD 列为 P1），结构设计如下，实现时沿用本文件的强制规则。

### 4.1 配置结构（存 `external_sources.extra_config`）

```json
{
  "request_path": "/api/predict",
  "request_params": {"type": "sum"},
  "paths": {
    "target_issue_no": "data.issue_no",
    "upstream_published_at": "data.predict_time",
    "models": "data.models"
  },
  "model_fields": {
    "model_key": "model_type",
    "display_name": "model_name",
    "confidence": "confidence"
  },
  "target_fields": {
    "big_small": "sum_big.value",
    "odd_even": "sum_odd.value",
    "combo": "sum_group.value"
  },
  "value_whitelist_override": null
}
```

- 路径语法：仅支持 `a.b.0.c`（键名与数组下标）的点路径取值，**不支持表达式/脚本**（PRD：不引入任意脚本执行）。
- `target_fields` 指向的值仍必须通过值域白名单校验，白名单默认与 §2 相同，不允许通过配置放宽到白名单之外。
- 数组型 `models` 逐项解析，单项失败计入 `invalid_model_count`。

### 4.2 明确不做

- 不做"管理员上传脚本/正则改写"类能力——任意代码执行边界。
- 不做 HTML/浏览器来源——那些仍是专用插件（或后续独立采集任务），见 PRD §5 排期表。
- 不做多来源鉴权凭据的通用存储——私人 API 凭据属后续"用户私有来源"设计。

### 4.3 什么时候该写专用插件而不是用 jsonmap

| 情况 | 选择 |
|---|---|
| 标准 JSON，字段能用点路径定位、值在白名单内 | 未来用 `jsonmap` 配置接入 |
| 需要签名/特殊请求头逻辑、嵌套变换、值域转换 | 专用插件（少量适配代码） |
| 预测在 HTML 页面或需浏览器渲染 | 专用插件或独立采集任务（本期排除） |
| 上游主动推送 | Webhook 接收器（后续设计，需校验来源与去重） |
