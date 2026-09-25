"""SQLite 持久化：授权、过滤规则、游标、发件箱（批次/租约）、计费与审计。

全部运行状态落库，进程重启后游标与未确认的发件箱批次可恢复。
多步写入通过 ``transaction()``（BEGIN IMMEDIATE）保证原子性；
单条语句在自动提交模式下执行。连接由可重入锁保护，可安全用于多线程。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS partners (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS partner_keys (
    id TEXT PRIMARY KEY,
    partner_id TEXT NOT NULL REFERENCES partners(id),
    key_hash TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    created_at REAL NOT NULL,
    not_after REAL
);
CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY,
    partner_id TEXT NOT NULL REFERENCES partners(id),
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    regions TEXT NOT NULL,                -- JSON 数组：区域过滤
    fields TEXT NOT NULL,                 -- JSON 数组：字段白名单，["*"] 表示全部
    min_interval_seconds REAL NOT NULL,   -- 刷新频率：两次成功拉取的最小间隔
    max_batch_size INTEGER NOT NULL,
    lease_seconds REAL NOT NULL,          -- 默认交付租约时长
    cursor_version INTEGER NOT NULL DEFAULT 0,  -- 游标：已确认的全局版本前沿
    last_pull_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS increments (
    version INTEGER PRIMARY KEY AUTOINCREMENT,  -- 全局单调版本
    event_id TEXT NOT NULL UNIQUE,              -- 幂等键：同一事件只入日志一次
    station_id TEXT NOT NULL,
    region TEXT NOT NULL,
    priority INTEGER NOT NULL,
    payload TEXT NOT NULL,                      -- JSON 全量载荷，仅服务端可见
    occurred_at REAL NOT NULL,
    ingested_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    id TEXT PRIMARY KEY,
    subscription_id TEXT NOT NULL REFERENCES subscriptions(id),
    seq INTEGER NOT NULL,                   -- 订阅内单调序号，保证顺序重投
    version_from INTEGER NOT NULL,          -- 增量区间 (version_from, version_to]
    version_to INTEGER NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    leased_at REAL,
    lease_expires_at REAL,
    acked_at REAL,
    created_at REAL NOT NULL,
    UNIQUE (subscription_id, seq)
);
CREATE TABLE IF NOT EXISTS batch_items (
    batch_id TEXT NOT NULL REFERENCES batches(id),
    event_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (batch_id, event_id)        -- 批次内容快照：只存事件引用，不存载荷
);
CREATE TABLE IF NOT EXISTS billing (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    units INTEGER NOT NULL,
    billed_at REAL NOT NULL,
    UNIQUE (subscription_id, event_id)      -- 同一事件对同一订阅只计费一次
);
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at REAL NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '{}'
);
"""

_SUB_JSON_COLUMNS = ("regions", "fields")


class Storage:
    """SQLite 存储门户。"""

    def __init__(self, path: str) -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """多步写入的原子事务（BEGIN IMMEDIATE，写锁全程持有）。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    # ---- 基础执行 ----

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._conn.execute(sql, params)

    def _one(self, sql: str, params: tuple = ()) -> dict[str, Any] | None:
        row = self._execute(sql, params).fetchone()
        return dict(row) if row is not None else None

    def _all(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        return [dict(r) for r in self._execute(sql, params).fetchall()]

    # ---- 合作方与密钥 ----

    def insert_partner(self, row: dict[str, Any]) -> None:
        self._execute(
            "INSERT INTO partners (id, name, status, created_at) VALUES (?, ?, ?, ?)",
            (row["id"], row["name"], row["status"], row["created_at"]),
        )

    def get_partner(self, partner_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM partners WHERE id = ?", (partner_id,))

    def update_partner_status(self, partner_id: str, status: str) -> None:
        self._execute("UPDATE partners SET status = ? WHERE id = ?", (status, partner_id))

    def insert_key(self, row: dict[str, Any]) -> None:
        self._execute(
            "INSERT INTO partner_keys (id, partner_id, key_hash, label, status, created_at, not_after)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row["id"], row["partner_id"], row["key_hash"], row["label"],
             row["status"], row["created_at"], row["not_after"]),
        )

    def get_key_by_hash(self, key_hash: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM partner_keys WHERE key_hash = ?", (key_hash,))

    def get_key(self, key_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM partner_keys WHERE id = ?", (key_id,))

    def list_keys(self, partner_id: str, statuses: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
        if statuses:
            marks = ",".join("?" for _ in statuses)
            return self._all(
                f"SELECT * FROM partner_keys WHERE partner_id = ? AND status IN ({marks})",
                (partner_id, *statuses),
            )
        return self._all("SELECT * FROM partner_keys WHERE partner_id = ?", (partner_id,))

    def update_key(self, key_id: str, **fields: Any) -> None:
        sets = ", ".join(f"{k} = ?" for k in fields)
        self._execute(f"UPDATE partner_keys SET {sets} WHERE id = ?", (*fields.values(), key_id))

    # ---- 订阅（授权与过滤规则、游标） ----

    def insert_subscription(self, row: dict[str, Any]) -> None:
        self._execute(
            "INSERT INTO subscriptions (id, partner_id, name, status, regions, fields,"
            " min_interval_seconds, max_batch_size, lease_seconds, cursor_version,"
            " last_pull_at, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (row["id"], row["partner_id"], row["name"], row["status"],
             json.dumps(row["regions"]), json.dumps(row["fields"]),
             row["min_interval_seconds"], row["max_batch_size"], row["lease_seconds"],
             row["cursor_version"], row["last_pull_at"], row["created_at"], row["updated_at"]),
        )

    def get_subscription(self, subscription_id: str) -> dict[str, Any] | None:
        row = self._one("SELECT * FROM subscriptions WHERE id = ?", (subscription_id,))
        if row is None:
            return None
        for col in _SUB_JSON_COLUMNS:
            row[col] = json.loads(row[col])
        return row

    def update_subscription(self, subscription_id: str, **fields: Any) -> None:
        encoded = {
            k: (json.dumps(v) if k in _SUB_JSON_COLUMNS else v)
            for k, v in fields.items()
        }
        sets = ", ".join(f"{k} = ?" for k in encoded)
        self._execute(
            f"UPDATE subscriptions SET {sets} WHERE id = ?",
            (*encoded.values(), subscription_id),
        )

    def touch_pull(self, subscription_id: str, at: float) -> None:
        self._execute(
            "UPDATE subscriptions SET last_pull_at = ? WHERE id = ?", (at, subscription_id)
        )

    # ---- 版本化增量日志 ----

    def insert_increment(self, row: dict[str, Any]) -> int:
        cur = self._execute(
            "INSERT INTO increments (event_id, station_id, region, priority, payload,"
            " occurred_at, ingested_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row["event_id"], row["station_id"], row["region"], row["priority"],
             json.dumps(row["payload"]), row["occurred_at"], row["ingested_at"]),
        )
        return int(cur.lastrowid)

    def get_increment_by_event(self, event_id: str) -> dict[str, Any] | None:
        row = self._one("SELECT * FROM increments WHERE event_id = ?", (event_id,))
        if row is not None:
            row["payload"] = json.loads(row["payload"])
        return row

    def max_version(self) -> int:
        row = self._one("SELECT COALESCE(MAX(version), 0) AS v FROM increments")
        return int(row["v"])

    def pending_increments(
        self, regions: list[str], after_version: int, limit: int
    ) -> list[dict[str, Any]]:
        if not regions:
            return []
        marks = ",".join("?" for _ in regions)
        rows = self._all(
            f"SELECT * FROM increments WHERE version > ? AND region IN ({marks})"
            " ORDER BY version LIMIT ?",
            (after_version, *regions, limit),
        )
        for row in rows:
            row["payload"] = json.loads(row["payload"])
        return rows

    def has_urgent_pending(self, regions: list[str], after_version: int, min_priority: int) -> bool:
        if not regions:
            return False
        marks = ",".join("?" for _ in regions)
        row = self._one(
            f"SELECT 1 AS x FROM increments WHERE version > ? AND region IN ({marks})"
            " AND priority >= ? LIMIT 1",
            (after_version, *regions, min_priority),
        )
        return row is not None

    # ---- 发件箱：批次与租约 ----

    def insert_batch(self, row: dict[str, Any]) -> None:
        self._execute(
            "INSERT INTO batches (id, subscription_id, seq, version_from, version_to,"
            " status, attempt, leased_at, lease_expires_at, acked_at, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (row["id"], row["subscription_id"], row["seq"], row["version_from"],
             row["version_to"], row["status"], row["attempt"], row["leased_at"],
             row["lease_expires_at"], row["acked_at"], row["created_at"]),
        )

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM batches WHERE id = ?", (batch_id,))

    def open_batch(self, subscription_id: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM batches WHERE subscription_id = ? AND status = 'leased'"
            " ORDER BY seq DESC LIMIT 1",
            (subscription_id,),
        )

    def next_seq(self, subscription_id: str) -> int:
        row = self._one(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM batches WHERE subscription_id = ?",
            (subscription_id,),
        )
        return int(row["s"])

    def update_batch(self, batch_id: str, **fields: Any) -> None:
        sets = ", ".join(f"{k} = ?" for k in fields)
        self._execute(f"UPDATE batches SET {sets} WHERE id = ?", (*fields.values(), batch_id))

    def supersede_open_batches(self, subscription_id: str) -> int:
        cur = self._execute(
            "UPDATE batches SET status = 'superseded', lease_expires_at = NULL"
            " WHERE subscription_id = ? AND status = 'leased'",
            (subscription_id,),
        )
        return cur.rowcount

    def list_batches(self, subscription_id: str) -> list[dict[str, Any]]:
        return self._all(
            "SELECT * FROM batches WHERE subscription_id = ? ORDER BY seq", (subscription_id,)
        )

    def insert_batch_items(self, batch_id: str, items: list[tuple[str, int]]) -> None:
        with self._lock:
            self._conn.executemany(
                "INSERT INTO batch_items (batch_id, event_id, version) VALUES (?, ?, ?)",
                [(batch_id, event_id, version) for event_id, version in items],
            )

    def batch_items(self, batch_id: str) -> list[dict[str, Any]]:
        rows = self._all(
            "SELECT i.event_id, i.version, inc.station_id, inc.region, inc.priority,"
            " inc.payload, inc.occurred_at"
            " FROM batch_items i JOIN increments inc ON inc.event_id = i.event_id"
            " WHERE i.batch_id = ? ORDER BY i.version",
            (batch_id,),
        )
        for row in rows:
            row["payload"] = json.loads(row["payload"])
        return rows

    # ---- 计费 ----

    def insert_billing_ignore(self, row: dict[str, Any]) -> bool:
        """幂等计费：同一 (订阅, 事件) 只入账一次，返回是否真正写入。"""
        cur = self._execute(
            "INSERT OR IGNORE INTO billing (subscription_id, event_id, batch_id, units, billed_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (row["subscription_id"], row["event_id"], row["batch_id"],
             row["units"], row["billed_at"]),
        )
        return cur.rowcount > 0

    def list_billing(self, subscription_id: str | None = None) -> list[dict[str, Any]]:
        if subscription_id:
            return self._all(
                "SELECT * FROM billing WHERE subscription_id = ? ORDER BY id", (subscription_id,)
            )
        return self._all("SELECT * FROM billing ORDER BY id")

    # ---- 审计 ----

    def insert_audit(self, row: dict[str, Any]) -> None:
        self._execute(
            "INSERT INTO audit_log (at, actor, action, target_type, target_id, detail)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (row["at"], row["actor"], row["action"], row["target_type"],
             row["target_id"], json.dumps(row.get("detail") or {}, ensure_ascii=False)),
        )

    def list_audit(self, target_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        if target_id:
            rows = self._all(
                "SELECT * FROM audit_log WHERE target_id = ? ORDER BY id LIMIT ?",
                (target_id, limit),
            )
        else:
            rows = self._all("SELECT * FROM audit_log ORDER BY id LIMIT ?", (limit,))
        for row in rows:
            row["detail"] = json.loads(row["detail"])
        return rows
