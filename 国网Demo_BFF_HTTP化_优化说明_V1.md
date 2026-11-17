# 国网 Demo BFF · HTTP 化优化说明 V1

> 日期：2026-11-17 · 改动文件：`sgcc_bff_http.py`（新增），其余逻辑零改动

## 一、改了什么

`sgcc_bff_http.py` 是 `sgcc_bff.py` 的**纯 HTTP 后端版**：对外的 8 个 `/api/*` 路由、
缓存 / 快照 / 限流 / 审计 / 口令门全部原样保留，只把底层从「子进程调 apemind.exe CLI」
换成了「直连 apemind.ai 的 HTTP API」（OpenAPI 定义见 https://apemind.ai/api/openapi.json ，
版本 v2.12.5）。

## 二、为什么值得改

| | CLI 版（原） | HTTP 版（新） |
|---|---|---|
| 判定（46 条）端到端 | 2.4~3.6s | **约 1.0s**（冒烟实测） |
| 抽取（field-extract） | 中位 23s | **6.0s**（冒烟实测，含轮询） |
| 依赖 | Windows + apemind.exe + 登录态 | 只要一个 api-key，任何机器可跑 |
| 故障面 | 进程冷启动 / 本机代理 / 换机器 | 更小；显式绕开本机代理 |
| 演示现场换电脑 | 基本没戏 | 拷过去就能跑 |

HTTP 化的依据（2026-11-16 实测）：原「官方未公开 /api/v2 接口清单」的结论已过时，
以下接口实测全部可用：
- `POST /api/v2/orgs/{ws}/functions/{ref}/invoke` —— 两个 sgcc 函数都能真调
- `GET  /api/v2/collections/{id}/documents` / `.../documents/{id}/content`
- `POST /api/v2/collections/{id}/searches`（全文 + 向量）
- `POST /api/v2/apikeys`（登录会话可创建长期密钥，30 天以上）

## 三、grep 的口径（重点，没有打折）

CLI grep 是服务端全库逐行扫描，HTTP 侧没有等价接口。新实现分两步：

1. **快路**：全文检索找出候选文档 → 拉回原文（带 10 分钟缓存）→ 本地逐行正则扫描，
   行号 / max-count / 截断标记与 CLI 同口径。
2. **补全**：启动期后台把招标主库 155 篇原文全部预热进缓存（实测约 100s），
   之后同库 grep 扫描集合即全库，`coverage=complete`。
   **只有真的扫过全库才报 complete**——「能负责任地说没有」的承诺不变，
   没扫完时依旧如实报 partial，前端照旧说「未扫完」。

## 四、运行方法

```bash
export APEMIND_API_KEY=sk-xxxx        # 必填
export APEMIND_WORKSPACE_ID=orge0066e5cc05abc86   # 默认已内置
export APEMIND_BASE_URL=https://apemind.ai        # 默认已内置
python3 sgcc_bff_http.py              # 其余环境变量与 sgcc_bff.py 完全一致
```

公网模式照旧：`BFF_PUBLIC=1` 且必须设 `BFF_TOKEN`。
健康检查多吐两个字段：`content_cache`（原文缓存条数）、`full_scan`（各库预热状态）。

## 五、冒烟证据

先起 HTTP 版 BFF，再跑 `python3 bff_smoke.py`（脚本零改动，直接兼容）：
9/9 PASS，证据落盘 `国网Demo_C方案_BFF真跑证据.json`。
关键读数：判定矩阵快照命中 0.01s；检索 1.34s；抽取全链路 6.04s。

## 六、已知差异（如实说明）

- CLI `read` 的行号来自服务端切行；HTTP 版用 `/content` 原文按 `\n` 切行，
  个别文档两者可能有 ±1 行级出入（结论都以"原文+行号"同屏呈现，可现场验证）。
- 判定响应里的 run 号是 invoke 后补查最近 runs 得到的，函数内耗时字段若查不到
  会退化为本地计时。
- `subprocess` / CLI 路径已整体移除，`_judge_in_*.json` 输入快照与快照物化机制不变。

## 七、建议的下一步（周四前）

1. 演示机用本文件替换后完整跑一遍 `bff_smoke.py` + 页面动线。
2. 仍建议保留原 CLI 版文件不删，作为断网/兜底的备胎（离线包 demo_pack_v2 也不动）。
3. api-key 建议演示前新建一个 30 天期的专用 key，演示后作废。
