"""地磅门户模拟器。

提供：
- GET  /api/sse?date=YYYY-MM-DD      SSE 实时推送（补称/作废/新单/迟到旧版）
- GET  /api/history                  固定快照的历史分页（page=1 建快照，后续页带 snapshot_id）
- POST /api/control/*                控制面：补称、作废、迟到旧版、新单、断流、延迟、快照 TTL
- GET  /                             控制面板页面
"""
import asyncio
import json
import random
import time
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse

PAGE_SIZE = 5

app = FastAPI(title="地磅门户模拟器")

PLATES = ["沪A12345", "苏E67890", "浙B24680", "皖C13579", "鲁D97531",
          "闽F86420", "豫G55231", "湘H71026", "赣J30817", "辽K66409"]

# date -> ticket_no -> 当前版本
_tickets: dict[str, dict[str, dict]] = {}
# date -> ticket_no -> [所有历史版本]（用于回放迟到旧版）
_versions: dict[str, dict[str, list[dict]]] = {}
_seq: dict[str, int] = {}
_subscribers: dict[str, set[asyncio.Queue]] = {}
_snapshots: dict[str, dict] = {}
_config = {"history_delay_ms": 0, "snapshot_ttl_s": 120}


def ensure_date(date: str) -> None:
    """为某天生成 23 张初始磅单（确定性数据）。"""
    if date in _tickets:
        return
    _tickets[date] = {}
    _versions[date] = {}
    compact = date.replace("-", "")
    for i in range(1, 24):
        no = f"WB{compact}-{i:03d}"
        v = {"ticket_no": no, "revision": 1, "plate": PLATES[i % len(PLATES)],
             "net_weight": 12000 + (i * 977) % 38000, "voided": False}
        _tickets[date][no] = dict(v)
        _versions[date][no] = [dict(v)]
    _seq[date] = 23


def _broadcast(date: str, event: dict) -> None:
    for q in list(_subscribers.get(date, ())):
        q.put_nowait(event)


def _bump(date: str, no: str, **changes) -> dict:
    """递增修订号生成新版本。"""
    cur = _tickets[date][no]
    new = dict(cur)
    new["revision"] = cur["revision"] + 1
    new.update(changes)
    _tickets[date][no] = dict(new)
    _versions[date][no].append(dict(new))
    return new


# ---------------------------------------------------------------- SSE 推送

@app.get("/api/sse")
async def sse(date: str):
    ensure_date(date)
    queue: asyncio.Queue = asyncio.Queue()
    subs = _subscribers.setdefault(date, set())
    subs.add(queue)

    async def stream():
        try:
            yield ": connected\n\n"
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": hb\n\n"  # 心跳，保持连接
                    continue
                if item is None:  # 控制面要求断流：直接结束响应体
                    return
                yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
        finally:
            subs.discard(queue)

    return StreamingResponse(
        stream(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ------------------------------------------------------- 固定快照历史分页

@app.get("/api/history")
async def history(date: str, page: int = 1, snapshot_id: str | None = None):
    ensure_date(date)
    delay = _config["history_delay_ms"]
    if delay:
        await asyncio.sleep(delay / 1000)

    if snapshot_id is None:
        if page != 1:
            raise HTTPException(400, "page>1 必须携带 snapshot_id")
        # 顺带清理过期快照
        now = time.time()
        for k in [k for k, s in _snapshots.items() if s["expires"] < now]:
            del _snapshots[k]
        # 建快照：冻结当前时刻的全量数据，之后补称/作废不影响本次分页
        items = [dict(t) for _, t in sorted(_tickets[date].items())]
        sid = uuid.uuid4().hex[:12]
        _snapshots[sid] = {"items": items,
                           "expires": now + _config["snapshot_ttl_s"]}
    else:
        snap = _snapshots.get(snapshot_id)
        if snap is None or snap["expires"] < time.time():
            raise HTTPException(410, "快照不存在或已过期，请重新补采")
        sid = snapshot_id
        items = snap["items"]

    total = len(items)
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    if page < 1 or page > pages:
        raise HTTPException(416, f"页码超出范围（共 {pages} 页）")
    return {"snapshot_id": sid, "date": date, "page": page, "pages": pages,
            "total": total, "items": items[(page - 1) * PAGE_SIZE: page * PAGE_SIZE]}


# ---------------------------------------------------------------- 控制面

@app.post("/api/control/reweigh")
async def reweigh(date: str, ticket_no: str | None = None, net_weight: float | None = None):
    """补称：随机（或指定）一张未作废单，修订号 +1 并推送。"""
    ensure_date(date)
    if ticket_no:
        if ticket_no not in _tickets[date]:
            raise HTTPException(404, "单号不存在")
        no = ticket_no
    else:
        candidates = [n for n, t in _tickets[date].items() if not t["voided"]]
        if not candidates:
            raise HTTPException(409, "没有可补称的单据")
        no = random.choice(candidates)
    weight = net_weight if net_weight is not None else random.randint(10000, 50000)
    new = _bump(date, no, net_weight=weight)
    _broadcast(date, new)
    return new


@app.post("/api/control/void")
async def void_ticket(date: str, ticket_no: str | None = None):
    """作废：随机（或指定）一张未作废单，修订号 +1、作废标记置位并推送。"""
    ensure_date(date)
    if ticket_no:
        if ticket_no not in _tickets[date]:
            raise HTTPException(404, "单号不存在")
        no = ticket_no
    else:
        candidates = [n for n, t in _tickets[date].items() if not t["voided"]]
        if not candidates:
            raise HTTPException(409, "没有可作废的单据")
        no = random.choice(candidates)
    new = _bump(date, no, voided=True)
    _broadcast(date, new)
    return new


@app.post("/api/control/replay_old")
async def replay_old(date: str, ticket_no: str | None = None):
    """把某张单的旧版本重新推送，模拟迟到旧版（采集端应忽略）。"""
    ensure_date(date)
    hist = _versions[date]
    if ticket_no:
        if len(hist.get(ticket_no, [])) < 2:
            raise HTTPException(404, "该单没有旧版本")
        no = ticket_no
    else:
        candidates = [n for n, vs in hist.items() if len(vs) >= 2]
        if not candidates:
            raise HTTPException(409, "还没有旧版本，请先补称或作废")
        voided = [n for n in candidates if _tickets[date][n]["voided"]]
        no = random.choice(voided or candidates)  # 优先选已作废单，演示"旧版不能恢复作废"
    old = dict(random.choice(hist[no][:-1]))
    _broadcast(date, old)
    return {"replayed": old, "current": _tickets[date][no]}


@app.post("/api/control/new_ticket")
async def new_ticket(date: str, plate: str | None = None, net_weight: float | None = None):
    """新增一张磅单并推送。"""
    ensure_date(date)
    _seq[date] += 1
    no = f"WB{date.replace('-', '')}-{_seq[date]:03d}"
    v = {"ticket_no": no, "revision": 1, "plate": plate or random.choice(PLATES),
         "net_weight": net_weight if net_weight is not None else random.randint(10000, 50000),
         "voided": False}
    _tickets[date][no] = dict(v)
    _versions[date][no] = [dict(v)]
    _broadcast(date, v)
    return v


@app.post("/api/control/disconnect")
async def disconnect():
    """断流：关闭当前所有 SSE 连接。"""
    n = 0
    for subs in _subscribers.values():
        for q in subs:
            q.put_nowait(None)
            n += 1
    return {"disconnected": n}


@app.get("/api/control/config")
async def get_config():
    return {**_config,
            "subscribers": sum(len(s) for s in _subscribers.values()),
            "dates": {d: len(t) for d, t in _tickets.items()}}


@app.post("/api/control/config")
async def set_config(history_delay_ms: int | None = None, snapshot_ttl_s: int | None = None):
    """调整历史接口延迟（毫秒）与快照有效期（秒）。"""
    if history_delay_ms is not None:
        _config["history_delay_ms"] = max(0, history_delay_ms)
    if snapshot_ttl_s is not None:
        _config["snapshot_ttl_s"] = max(1, snapshot_ttl_s)
    return _config


# ---------------------------------------------------------------- 控制面板

@app.get("/", response_class=HTMLResponse)
async def index():
    return CONTROL_PAGE


CONTROL_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>地磅门户模拟器</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 880px; margin: 24px auto; padding: 0 16px; background:#f6f7f9; color:#222; }
  h1 { font-size: 20px; } h2 { font-size: 15px; margin-bottom: 8px; }
  .card { background:#fff; border:1px solid #e3e5e8; border-radius:10px; padding:16px; margin-bottom:16px; }
  button { padding:7px 14px; border:1px solid #c9ccd1; border-radius:7px; background:#fff; cursor:pointer; margin:3px 4px 3px 0; font-size:13px; }
  button:hover { background:#eef1f4; }
  button.danger { border-color:#d9707a; color:#b02a37; }
  input { padding:6px 9px; border:1px solid #c9ccd1; border-radius:7px; font-size:13px; }
  pre { background:#0f1720; color:#b7e0c0; padding:12px; border-radius:8px; font-size:12px; overflow:auto; max-height:300px; }
  .row { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
  label { font-size:13px; color:#555; }
</style>
</head>
<body>
<h1>地磅门户模拟器 · 控制面板</h1>
<div class="card">
  <h2>业务操作（SSE 推送）</h2>
  <div class="row">
    <label>日期 <input id="date" type="date"></label>
    <label>单号(可空=随机) <input id="no" placeholder="WB...-001" size="14"></label>
  </div>
  <div class="row" style="margin-top:8px">
    <button onclick="act('reweigh')">补称（修订+1）</button>
    <button onclick="act('void')">作废（修订+1）</button>
    <button onclick="act('replay_old')">推送迟到旧版</button>
    <button onclick="act('new_ticket')">新增磅单</button>
    <button class="danger" onclick="post('/api/control/disconnect')">断开所有订阅（断流）</button>
  </div>
</div>
<div class="card">
  <h2>故障注入</h2>
  <div class="row">
    <label>历史延迟 ms <input id="delay" type="number" value="0" style="width:90px"></label>
    <label>快照有效期 s <input id="ttl" type="number" value="120" style="width:70px"></label>
    <button onclick="setCfg()">应用</button>
    <button onclick="quickCfg(0,120)">恢复正常</button>
    <button onclick="quickCfg(8000,120)">历史超时(8s)</button>
    <button onclick="quickCfg(1500,2)">快照过期(1.5s×TTL2s)</button>
  </div>
</div>
<div class="card">
  <h2>状态</h2>
  <pre id="out">…</pre>
</div>
<script>
const today = new Date().toISOString().slice(0,10);
document.getElementById('date').value = today;
async function post(url){ const r = await fetch(url,{method:'POST'}); return r.json(); }
async function act(name){
  const d = document.getElementById('date').value;
  const no = document.getElementById('no').value.trim();
  let url = `/api/control/${name}?date=${d}`;
  if(no) url += `&ticket_no=${encodeURIComponent(no)}`;
  const j = await post(url); flash(j);
}
async function setCfg(){
  const delay = document.getElementById('delay').value;
  const ttl = document.getElementById('ttl').value;
  flash(await post(`/api/control/config?history_delay_ms=${delay}&snapshot_ttl_s=${ttl}`));
}
async function quickCfg(delay, ttl){
  document.getElementById('delay').value = delay;
  document.getElementById('ttl').value = ttl;
  await setCfg();
}
function flash(j){ document.getElementById('out').textContent = JSON.stringify(j,null,2); }
async function refresh(){
  try { flash(await (await fetch('/api/control/config')).json()); } catch(e){}
}
setInterval(refresh, 3000); refresh();
</script>
</body>
</html>"""
