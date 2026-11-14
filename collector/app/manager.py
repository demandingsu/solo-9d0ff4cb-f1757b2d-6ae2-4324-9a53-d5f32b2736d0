"""采集任务调度。

状态机（每个采集任务一个后台协程）：
    connecting    正在建立 SSE 订阅
    backfilling   订阅已接通，正在补采历史（推送持续接收）
    synced        订阅正常且历史取齐 —— 唯一允许下载 CSV 的状态
    reconnecting  订阅中断，等待自动重连（下载立即关闭）
    failed        历史补采失败，显示原因并保持未同步（自动重试）
    revoked       已撤销（终态）：订阅与补采停止、磅单清除、下载永久关闭

关键不变式：
1. 先接通订阅，再补采历史；补采期间推送照常落库。
2. 同单只保留最高修订号（UPSERT ... WHERE excluded.revision > 现有），
   因此迟到旧版无法覆盖、也无法恢复已作废单。
3. 每次（重）连接 epoch +1；补采完成只有在 epoch 未变且仍处于本连接
   生命期内才把状态推进到 synced —— 旧连接的补采完成不能恢复下载。
4. revoked 是终态：所有状态迁移与票据 UPSERT 都带 state<>'revoked' 条件，
   正在返回的历史页、已排队的推送、残留的补采回调在撤销后全部成为
   空操作；重复撤销与进程重启都不会让任务复活或留下单据。
"""
import asyncio
import json
import os

import httpx

from . import db

HISTORY_TIMEOUT = float(os.environ.get("HISTORY_TIMEOUT", "5"))
RECONNECT_DELAY = float(os.environ.get("RECONNECT_DELAY", "1"))
BACKFILL_RETRY_DELAY = float(os.environ.get("BACKFILL_RETRY_DELAY", "5"))

# 同单仅保留最高修订：低版本到达时 WHERE 不成立，整行不变。
# 末尾的 EXISTS 防护：任务已撤销时 SELECT 返回 0 行，插入/更新整体成为
# 空操作 —— 正在返回的历史页与已排队的推送不会在撤销后写回。
UPSERT_SQL = """
INSERT INTO tickets (job_id, ticket_no, revision, plate, net_weight, voided)
SELECT ?, ?, ?, ?, ?, ?
WHERE EXISTS (SELECT 1 FROM jobs WHERE jobs.id = ? AND jobs.state <> 'revoked')
ON CONFLICT(job_id, ticket_no) DO UPDATE SET
    revision   = excluded.revision,
    plate      = excluded.plate,
    net_weight = excluded.net_weight,
    voided     = excluded.voided
WHERE excluded.revision > tickets.revision
"""

_runners: dict[int, asyncio.Task] = {}


def ensure_runner(job_id: int, portal_url: str, biz_date: str) -> None:
    task = _runners.get(job_id)
    if task is None or task.done():
        _runners[job_id] = asyncio.create_task(_run(job_id, portal_url.rstrip("/"), biz_date))


async def _set_state(job_id: int, epoch: int, state: str,
                     error: str | None = None, guard: tuple = ()) -> None:
    """按 (job, epoch) 条件更新状态；guard 限定允许从哪些状态迁移。

    state<>'revoked' 是硬条件：已撤销是终态，任何残留回调（断流收尾、
    补采完成、失败重试）都无法让任务复活。
    """
    d = await db.conn()
    sql = ("UPDATE jobs SET state=?, error=?, updated_at=datetime('now') "
           "WHERE id=? AND epoch=? AND state<>'revoked'")
    params: list = [state, error, job_id, epoch]
    if guard:
        sql += " AND state IN (%s)" % ",".join("?" * len(guard))
        params += list(guard)
    await d.execute(sql, params)
    await d.commit()


async def revoke(job_id: int) -> None:
    """撤销任务：停止订阅与补采、清除已采磅单、永久关闭下载。

    顺序保证无写回窗口：
      1) 单条 UPDATE 置 revoked（终态）——此后所有 UPSERT 因 EXISTS
         防护失效，所有状态迁移因 revoked 硬条件失效；
      2) 删除该任务全部磅单；
      3) 取消后台协程并等其收尾（收尾里的状态迁移同样被终态挡住）。
    幂等：重复撤销时 UPDATE 命中 0 行，结果不变。
    """
    d = await db.conn()
    await d.execute(
        "UPDATE jobs SET state='revoked', "
        "error='任务已撤销：订阅与补采已停止，磅单已清除，不进入结算', "
        "revoked_at=COALESCE(revoked_at, datetime('now')), "
        "updated_at=datetime('now') "
        "WHERE id=? AND state<>'revoked'", (job_id,))
    await d.execute("DELETE FROM tickets WHERE job_id=?", (job_id,))
    await d.commit()
    task = _runners.pop(job_id, None)
    if task is not None:
        task.cancel()
        try:
            await task  # 等收尾跑完，确保不留未完成的回调
        except asyncio.CancelledError:
            pass


async def _run(job_id: int, portal: str, date: str) -> None:
    while True:
        d = await db.conn()
        # 新一轮连接：代次 +1，使上一代的补采回调全部失效。
        # 已撤销任务到此为止（revoke() 会取消本协程；此处是兜底防线）。
        await d.execute(
            "UPDATE jobs SET epoch=epoch+1, state='connecting', error=NULL, "
            "pages_fetched=0, updated_at=datetime('now') "
            "WHERE id=? AND state<>'revoked'", (job_id,))
        await d.commit()
        row = await (await d.execute(
            "SELECT epoch, state FROM jobs WHERE id=?", (job_id,))).fetchone()
        if row is None or row["state"] == "revoked":
            return
        epoch = row["epoch"]

        backfill_task: asyncio.Task | None = None
        disconnect_reason = "门户关闭连接"
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream(
                        "GET", f"{portal}/api/sse",
                        params={"date": date}) as resp:
                    resp.raise_for_status()
                    # 1) 订阅接通后才允许补采
                    await _set_state(job_id, epoch, "backfilling")
                    # 2) 补采与推送接收并行
                    backfill_task = asyncio.create_task(
                        _backfill_loop(job_id, portal, date, epoch))
                    async for line in resp.aiter_lines():
                        if not line or line.startswith(":"):
                            continue  # 空行 / 心跳注释
                        if line.startswith("data:"):
                            try:
                                evt = json.loads(line[len("data:"):].strip())
                            except json.JSONDecodeError:
                                continue
                            if isinstance(evt, dict) and evt.get("ticket_no"):
                                await _apply_event(job_id, evt)
        except asyncio.CancelledError:
            if backfill_task is not None:
                backfill_task.cancel()
            raise
        except Exception as exc:  # 连接失败 / 流中断 / 门户异常
            disconnect_reason = f"{type(exc).__name__}: {exc}"
        finally:
            if backfill_task is not None:
                backfill_task.cancel()
            # 断流立即关闭下载（只有 synced 才允许下载）
            await _set_state(
                job_id, epoch, "reconnecting",
                error=f"订阅中断（{disconnect_reason}），"
                      f"{RECONNECT_DELAY:g}s 后自动重连并重新补采")
        await asyncio.sleep(RECONNECT_DELAY)


async def _backfill_loop(job_id: int, portal: str, date: str, epoch: int) -> None:
    """补采历史；失败保持未同步并定时重试，直到成功或被取消。"""
    while True:
        try:
            await _backfill_once(job_id, portal, date)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _set_state(
                job_id, epoch, "failed",
                error=f"历史补采失败：{_reason(exc)}"
                      f"（{BACKFILL_RETRY_DELAY:g}s 后自动重试）",
                guard=("backfilling", "failed"))
            await asyncio.sleep(BACKFILL_RETRY_DELAY)
            continue
        # 历史取齐：仅当代次未变且仍在本连接生命期内才开放下载。
        # 若期间断流，状态已是 reconnecting / epoch 已推进，此处为无操作。
        await _set_state(job_id, epoch, "synced", error=None,
                         guard=("backfilling", "failed"))
        return


async def _backfill_once(job_id: int, portal: str, date: str) -> None:
    async with httpx.AsyncClient(timeout=HISTORY_TIMEOUT) as client:
        resp = await client.get(f"{portal}/api/history",
                                params={"date": date, "page": 1})
        data = _checked(resp)
        await _apply_page(job_id, data["items"])
        sid, pages = data["snapshot_id"], data["pages"]
        for page in range(2, pages + 1):
            resp = await client.get(
                f"{portal}/api/history",
                params={"date": date, "page": page, "snapshot_id": sid})
            data = _checked(resp)
            await _apply_page(job_id, data["items"])


class HistoryError(Exception):
    """门户历史接口返回非 200。"""


def _checked(resp: httpx.Response) -> dict:
    if resp.status_code != 200:
        raise HistoryError(f"门户返回 {resp.status_code}: {resp.text[:150]}")
    return resp.json()


def _reason(exc: Exception) -> str:
    if isinstance(exc, HistoryError):
        return str(exc)
    if isinstance(exc, httpx.TimeoutException):
        return f"请求门户超时（>{HISTORY_TIMEOUT:g}s）"
    if isinstance(exc, httpx.ConnectError):
        return "无法连接门户历史接口"
    return f"{type(exc).__name__}: {exc}"


async def _apply_event(job_id: int, evt: dict) -> None:
    d = await db.conn()
    await d.execute(UPSERT_SQL, (
        job_id, str(evt["ticket_no"]), int(evt["revision"]),
        str(evt["plate"]), float(evt["net_weight"]),
        1 if evt.get("voided") else 0, job_id))
    await d.execute(
        "UPDATE jobs SET events_received=events_received+1, "
        "updated_at=datetime('now') WHERE id=? AND state<>'revoked'", (job_id,))
    await d.commit()


async def _apply_page(job_id: int, items: list[dict]) -> None:
    d = await db.conn()
    await d.executemany(UPSERT_SQL, [
        (job_id, str(it["ticket_no"]), int(it["revision"]), str(it["plate"]),
         float(it["net_weight"]), 1 if it.get("voided") else 0, job_id)
        for it in items
    ])
    await d.execute(
        "UPDATE jobs SET pages_fetched=pages_fetched+1, "
        "updated_at=datetime('now') WHERE id=? AND state<>'revoked'", (job_id,))
    await d.commit()
