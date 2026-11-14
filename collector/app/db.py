"""SQLite 存储层（aiosqlite 单连接，内部串行化，多协程共享安全）。"""
import os

import aiosqlite

DATA_DIR = os.environ.get("DATA_DIR", ".")
DB_PATH = os.path.join(DATA_DIR, "collector.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    portal_url      TEXT NOT NULL,
    biz_date        TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'connecting',
    error           TEXT,
    epoch           INTEGER NOT NULL DEFAULT 0,
    events_received INTEGER NOT NULL DEFAULT 0,
    pages_fetched   INTEGER NOT NULL DEFAULT 0,
    revoked_at      TEXT,  -- 撤销时间；非 NULL 即终态，任务永不复活
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS tickets (
    job_id      INTEGER NOT NULL,
    ticket_no   TEXT    NOT NULL,
    revision    INTEGER NOT NULL,
    plate       TEXT    NOT NULL,
    net_weight  REAL    NOT NULL,
    voided      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job_id, ticket_no)
);
"""

_conn: aiosqlite.Connection | None = None


async def conn() -> aiosqlite.Connection:
    global _conn
    if _conn is None:
        os.makedirs(DATA_DIR, exist_ok=True)
        _conn = await aiosqlite.connect(DB_PATH)
        _conn.row_factory = aiosqlite.Row
        await _conn.executescript(SCHEMA)
        await _migrate(_conn)
        await _conn.commit()
    return _conn


async def _migrate(d: aiosqlite.Connection) -> None:
    """老库就地升级：jobs 缺 revoked_at 列时补上。"""
    rows = await (await d.execute("PRAGMA table_info(jobs)")).fetchall()
    cols = {r["name"] for r in rows}
    if "revoked_at" not in cols:
        await d.execute("ALTER TABLE jobs ADD COLUMN revoked_at TEXT")
