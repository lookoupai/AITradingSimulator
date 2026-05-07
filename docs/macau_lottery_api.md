# 新澳门六合彩 API 接口文档

> 数据来源：macaujc.com  
> 接口地址：https://macaujc.com/api/

---

## API 端点

### 1. 最新一期开奖（备用源 - 推荐）

```
GET https://api3.marksix6.net/lottery_api.php?type=newMacau
```

**参数：** 无

**响应示例：**
```json
{
  "expect": "2026127",
  "openCode": "14,08,27,03,07,29,45",
  "openTime": "2026-05-07 21:32:32",
  "wave": "blue,red,green,blue,red,red,red",
  "zodiac": "蛇,豬,龍,龍,鼠,虎,狗",
  "numbers": ["14", "08", "27", "03", "07", "29", "45"],
  "type": "8"
}
```

**优势：** 直接返回JSON，包含 `numbers` 数组便于解析

---

### 2. 最新一期开奖（官方源）

```
GET https://macaumarksix.com/api/macaujc2
```

**参数：** 无

**响应示例：**
```json
[
  {
    "expect": "2025123",
    "openCode": "37,30,49,16,09,12,45",
    "openTime": "2025-05-03 21:32:32",
    "wave": "blue,red,green,green,blue,red,red",
    "zodiac": "蛇,鼠,蛇,虎,雞,馬,雞",
    "type": "8",
    "firstsecend": 0,
    "verify": false
  }
]
```

### 2. 一颗一颗开奖（资料网专用）

```
GET https://macaumarksix.com/api/live2
```

**参数：** 无  
**响应格式：** 同上

### 3. 历史开奖数据（按年份）

```
GET https://history.macaumarksix.com/history/macaujc2/y/${year}
```

**参数：**
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| year | number | 是 | 年份 |

**响应示例：**
```json
{
  "code": 200,
  "result": true,
  "message": "操作成功！",
  "timestamp": 1717468655066,
  "data": [
    {
      "expect": "2025123",
      "openCode": "37,30,49,16,09,12,45",
      "openTime": "2025-05-03 21:32:32",
      "wave": "blue,red,green,green,blue,red,red",
      "zodiac": "蛇,鼠,蛇,虎,雞,馬,雞"
    }
  ]
}
```

### 4. 按期号查询

```
GET https://history.macaumarksix.com/history/macaujc2/expect/${number}
```

**参数：**
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| number | number | 是 | 期号 |

**响应格式：** 同历史开奖接口

---

## 数据字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `expect` | string | 期号 |
| `openCode` | string | 开奖号码，7个数字，逗号分隔 |
| `openTime` | string | 开奖时间 |
| `wave` | string | 波色 (blue=蓝, red=红, green=绿) |
| `zodiac` | string | 生肖 |
| `type` | string | 类型标识 |
| `firstsecend` | number | 秒数 |
| `verify` | boolean | 验证状态 |

---

## 波色对照

| 颜色 | 中文 | 对应号码范围 |
|------|------|-------------|
| red | 红波 | 1, 2, 7, 8, 12, 13, 18, 19, 23, 24, 29, 30, 34, 35, 40, 45, 46 |
| blue | 蓝波 | 3, 4, 9, 10, 14, 15, 20, 25, 26, 31, 36, 37, 41, 42, 47, 48 |
| green | 绿波 | 5, 6, 11, 16, 17, 21, 22, 27, 28, 32, 33, 38, 39, 43, 44, 49 |

---

## 直播流地址

| 格式 | 地址 |
|------|------|
| FLV | `https://live-macaujc.com/live/livestream/new.flv` |
| M3U8 | `https://live-macaujc.com/live/livestream/new.m3u8` |

---

## 联系方式

- 官网：macaujc.com
- 技术支持：support@macaujc.com

---

## 备注

- 开奖号码共7个：前6个为正码，第7个为特码
- 波色和生肖按顺序对应每个开奖号码
- 历史数据接口返回数组，包含该年份所有开奖记录
