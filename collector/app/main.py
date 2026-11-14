"""采集端 HTTP API 与页面。"""
import csv
import io
import re
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from . import db, manager

STATIC_DIR = Path(__file__).parent / "static"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

app = FastAPI(title="地磅单采集器")


class JobIn(BaseModel):
    portal_url: str
    biz_date: str


@app.on_event("startup")
async def startup() -> None:
    # 重启后恢复所有未完成的采集任务（数据已持久化，状态机重新走连接流程）
    d = await db.conn()
    rows = await (await d.execute(
        "SELECT id, portal_url, biz_date FROM jobs")).fetchall()
    for r in rows:
        manager.ensure_runner(r["id"], r["portal_url"], r["biz_date"])


@app.post("/api/jobs", status_code=201)
async def create_job(body: JobIn):
    portal = body.portal_url.strip().rstrip("/")
    if not portal.startswith(("http://", "https://")):
        raise HTTPException(422, "门户地址需以 http:// 或 https:// 开头")
    if not DATE_RE.match(body.biz_date):
        raise HTTPException(422, "日期格式应为 YYYY-MM-DD")
    d = await db.conn()
    cur = await d.execute(
        "INSERT INTO jobs (portal_url, biz_date) VALUES (?, ?)",
        (portal, body.biz_date))
    await d.commit()
    job_id = cur.lastrowid
    manager.ensure_runner(job_id, portal, body.biz_date)
    return await _job_or_404(job_id)


@app.get("/api/jobs")
async def list_jobs():
    d = await db.conn()
    rows = await (await d.execute("""
        SELECT j.*,
          (SELECT COUNT(*) FROM tickets t WHERE t.job_id = j.id) AS ticket_total,
          (SELECT COUNT(*) FROM tickets t WHERE t.job_id = j.id AND t.voided = 0)
              AS ticket_valid
        FROM jobs j ORDER BY j.id DESC""")).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/jobs/{job_id}")
async def job_detail(job_id: int):
    return await _job_or_404(job_id)


@app.get("/api/jobs/{job_id}/tickets")
async def job_tickets(job_id: int):
    await _job_or_404(job_id)
    d = await db.conn()
    rows = await (await d.execute(
        "SELECT ticket_no, revision, plate, net_weight, voided "
        "FROM tickets WHERE job_id=? ORDER BY ticket_no", (job_id,))).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/jobs/{job_id}/csv")
async def job_csv(job_id: int):
    job = await _job_or_404(job_id)
    if job["state"] != "synced":
        raise HTTPException(
            409, f"未同步完成，禁止下载（当前状态：{job['state']}）")
    d = await db.conn()
    rows = await (await d.execute(
        "SELECT ticket_no, revision, plate, net_weight FROM tickets "
        "WHERE job_id=? AND voided=0 ORDER BY ticket_no", (job_id,))).fetchall()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["单号", "修订号", "车牌", "净重(kg)"])
    for r in rows:
        w.writerow([r["ticket_no"], r["revision"], r["plate"], r["net_weight"]])
    return Response(
        content=buf.getvalue().encode("utf-8-sig"),  # BOM 便于 Excel 打开
        media_type="text/csv",
        headers={"Content-Disposition":
                 f"attachment; filename=tickets_{job['biz_date']}_job{job_id}.csv"})


async def _job_or_404(job_id: int) -> dict:
    d = await db.conn()
    row = await (await d.execute(
        "SELECT * FROM jobs WHERE id=?", (job_id,))).fetchone()
    if row is None:
        raise HTTPException(404, "采集任务不存在")
    return dict(row)


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(STATIC_DIR / "index.html")
