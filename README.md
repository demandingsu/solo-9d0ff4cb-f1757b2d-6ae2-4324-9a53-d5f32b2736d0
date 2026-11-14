# 地磅单采集 → 结算

码头数据员把地磅单持续接入结算系统：采集端（FastAPI + SQLite）订阅门户的 SSE 推送，
并按固定快照分页补采历史；同单仅保留最高修订号，订阅正常且历史取齐后才开放有效单 CSV 下载。

## 快速启动

```bash
docker compose up --build
```

| 服务 | 地址 | 说明 |
|---|---|---|
| 采集端 collector | http://localhost:8001 | 发起采集、查看同步状态、下载 CSV |
| 门户模拟器 portal | http://localhost:8000 | 控制面板：补称 / 作废 / 迟到旧版 / 断流 / 历史延迟 |

浏览器打开 **http://localhost:8001**，输入门户地址（默认 `http://portal:8000`）和日期，点击「开始采集」。

## 采集流程与状态机

```
connecting ──订阅接通──▶ backfilling ──历史取齐──▶ synced（开放下载）
     ▲                       │                        │
     │                       │ 历史失败                │ 断流
     │                       ▼                        ▼
     └──── 自动重连 ── reconnecting                failed（显示原因，保持未同步，自动重试）
```

1. **先接通订阅再补采历史**：SSE 连接建立后才发起历史分页请求，补采期间推送持续落库。
2. **固定快照分页**：历史第 1 页在门户侧冻结快照并返回 `snapshot_id`，后续页凭快照翻页；
   补采期间发生的补称/作废通过 SSE 送达，不会打乱分页。
3. **同单仅保留最高修订**：落库使用
   `INSERT ... ON CONFLICT DO UPDATE ... WHERE excluded.revision > 现有revision`，
   因此迟到旧版既不能覆盖新数据，也不能恢复已作废单。
4. **只有 `synced` 可下载**：断流立即转入 `reconnecting` 并关闭下载（CSV 接口返回 409）。
5. **代次（epoch）防护**：每次（重）连接 epoch +1；补采完成只有在 epoch 未变且仍处于
   本连接生命期内才推进到 `synced` —— 旧连接的补采完成不能恢复下载。
6. **历史失败可见**：超时、快照过期（410）、门户 5xx 等原因展示在任务列表中，
   状态保持 `failed`（未同步），后台按 `BACKFILL_RETRY_DELAY` 自动重试。

## 演示流程

在门户控制面板（http://localhost:8000）操作，观察采集端（http://localhost:8001）变化：

| 操作 | 预期 |
|---|---|
| 「补称」/「作废」 | 采集端秒级更新修订号/作废标记；作废单从 CSV 消失 |
| 「推送迟到旧版」 | 采集端忽略：修订号不变，已作废单不复活 |
| 「断开所有订阅」 | 任务立即变「订阅中断 · 重连中」，CSV 409；约 1s 后自动重连、重新补采并恢复可下载 |
| 「历史超时(8s)」后新建任务 | 任务变「历史补采失败」，原因显示超时；「恢复正常」后自动追齐 |
| 「快照过期(1.5s×TTL2s)」后新建任务 | 补采中途快照失效（410），显示原因并保持未同步 |

## API 摘要

采集端（8001）：

- `POST /api/jobs` `{portal_url, biz_date}` 创建采集任务
- `GET /api/jobs` / `GET /api/jobs/{id}` / `GET /api/jobs/{id}/tickets` 状态查询
- `GET /api/jobs/{id}/csv` 有效单 CSV（仅 `synced`，否则 409）

门户（8000）：

- `GET /api/sse?date=` SSE 推送（15s 心跳）
- `GET /api/history?date=&page=&snapshot_id=` 固定快照分页（每页 5 条）
- `POST /api/control/{reweigh,void,replay_old,new_ticket,disconnect,config}` 控制面

## 配置（collector 环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `/data` | SQLite 数据目录（compose 已挂卷，重启任务自动恢复） |
| `HISTORY_TIMEOUT` | `5` | 历史接口单次请求超时（秒） |
| `RECONNECT_DELAY` | `1` | 断流重连间隔（秒） |
| `BACKFILL_RETRY_DELAY` | `5` | 历史补采失败重试间隔（秒） |

## 本地开发（不用 Docker）

```bash
pip install -r portal/requirements.txt -r collector/requirements.txt
(cd portal && uvicorn app.main:app --port 8000) &
(cd collector && DATA_DIR=./data uvicorn app.main:app --port 8001) &
```
