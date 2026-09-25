"""SQLite 持久化：事件、授权、规则历史、游标、发件箱、计费、审计全部落盘。"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterable, Mapping

from ..domain import models as m

SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    station_id TEXT NOT NULL,
    area TEXT NOT NULL,
    status TEXT NOT NULL,
    congestion TEXT NOT NULL,
    occurred_at REAL NOT NULL,
    ingested_at REAL NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS partners (
    partner_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at REAL NOT NULL,
    revoked_at REAL,
    revoked_reason TEXT
);

CREATE TABLE IF NOT EXISTS api_keys (
    key_id TEXT PRIMARY KEY,
    partner_id TEXT NOT NULL REFERENCES partners(partner_id),
    secret_hash TEXT NOT NULL,
    created_at REAL NOT NULL,
    expires_at REAL,
    revoked_at REAL
);

CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id TEXT PRIMARY KEY,
    partner_id TEXT NOT NULL REFERENCES partners(partner_id),
    refresh_interval REAL NOT NULL,
    max_batch_size INTEGER NOT NULL,
    created_at REAL NOT NULL,
    last_pull_at REAL
);

CREATE TABLE IF NOT EXISTS filter_rules (
    rule_rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id TEXT NOT NULL REFERENCES subscriptions(subscription_id),
    version INTEGER NOT NULL,
    areas_json TEXT,
    fields_json TEXT,
    min_congestion TEXT NOT NULL,
    station_ids_json TEXT,
    changed_at REAL NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    UNIQUE(subscription_id, version)
);

CREATE TABLE IF NOT EXISTS cursors (
    subscription_id TEXT PRIMARY KEY REFERENCES subscriptions(subscription_id),
    last_acked_seq INTEGER NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    batch_id TEXT PRIMARY KEY,
    subscription_id TEXT NOT NULL,
    delivery_attempt INTEGER NOT NULL,
    from_seq INTEGER NOT NULL,
    to_seq INTEGER NOT NULL,
    rule_version INTEGER NOT NULL,
    state TEXT NOT NULL,
    lease_owner TEXT,
    leased_at REAL,
    lease_expires_at REAL,
    created_at REAL NOT NULL,
    priority INTEGER NOT NULL,
    redacted INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_outbox_sub_state ON outbox(subscription_id, state);
CREATE INDEX IF NOT EXISTS idx_outbox_dispatch ON outbox(state, priority, created_at);
-- 每个订阅至多存在一个存活批次（待投递/投递中/已过期），并发下兜底防重
CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_one_live ON outbox(subscription_id)
    WHERE state IN ('pending', 'delivered', 'expired');

CREATE TABLE IF NOT EXISTS batch_items (
    batch_id TEXT NOT NULL REFERENCES outbox(batch_id),
    position INTEGER NOT NULL,
    event_seq INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    projection_json TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    PRIMARY KEY (batch_id, position)
);

-- 计费去重：同一 (订阅, 事件) 只能记账一次
CREATE TABLE IF NOT EXISTS billing (
    fingerprint TEXT PRIMARY KEY,
    subscription_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    charged_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    at REAL NOT NULL,
    action TEXT NOT NULL,
    partner_id TEXT,
    subscription_id TEXT,
    actor TEXT NOT NULL,
    detail_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_sub ON audit_log(subscription_id, audit_id);
CREATE INDEX IF NOT EXISTS idx_audit_partner ON audit_log(partner_id, audit_id);
"""


def _dumps(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, frozenset | set):
        return json.dumps(sorted(value), ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)


def _loads_list(text: str | None) -> list[str] | None:
    if text is None:
        return None
    return json.loads(text)


class SQLiteStore:
    """线程安全的 SQLite 仓储。单连接 + 排他锁，事务内完成读改写。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._path = str(path)
        # check_same_thread=False：并发确认测试需要跨线程共享
        self._conn = sqlite3.connect(
            self._path,
            check_same_thread=False,
            isolation_level=None,  # 显式事务
            timeout=30.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.RLock()
        self._initialize()

    # -- 基础 ---------------------------------------------------------------

    def _initialize(self) -> None:
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.execute(
                "INSERT OR IGNORE INTO schema_meta(key, value) VALUES('version', ?)",
                (str(SCHEMA_VERSION),),
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @property
    def path(self) -> str:
        return self._path

    # -- 事件 ---------------------------------------------------------------

    def insert_event(
        self,
        *,
        event_id: str,
        station_id: str,
        area: str,
        status: m.StationStatus,
        payload: Mapping[str, Any],
        congestion: m.CongestionLevel,
        occurred_at: float,
        ingested_at: float,
    ) -> m.StationEvent | None:
        """幂等摄入：重复 event_id 返回 None。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO events(event_id, station_id, area, status,"
                " congestion, occurred_at, ingested_at, payload_json)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (
                    event_id,
                    station_id,
                    area,
                    status.value,
                    congestion.value,
                    occurred_at,
                    ingested_at,
                    json.dumps(dict(payload), ensure_ascii=False),
                ),
            )
            if cur.rowcount == 0:
                return None
            seq = cur.lastrowid
        return m.StationEvent(
            seq=seq,
            event_id=event_id,
            station_id=station_id,
            area=area,
            status=status,
            payload=dict(payload),
            congestion=congestion,
            occurred_at=occurred_at,
            ingested_at=ingested_at,
        )

    def _load_event(self, row: sqlite3.Row) -> m.StationEvent:
        return m.StationEvent(
            seq=row["seq"],
            event_id=row["event_id"],
            station_id=row["station_id"],
            area=row["area"],
            status=m.StationStatus(row["status"]),
            payload=json.loads(row["payload_json"]),
            congestion=m.CongestionLevel(row["congestion"]),
            occurred_at=row["occurred_at"],
            ingested_at=row["ingested_at"],
        )

    def get_event(self, seq: int) -> m.StationEvent | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM events WHERE seq=?", (seq,)
            ).fetchone()
        return self._load_event(row) if row else None

    def events_after(self, last_seq: int) -> list[m.StationEvent]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE seq>? ORDER BY seq ASC", (last_seq,)
            ).fetchall()
        return [self._load_event(r) for r in rows]

    def max_event_seq(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COALESCE(MAX(seq),0) AS m FROM events").fetchone()
        return int(row["m"])

    # -- 合作方与密钥 --------------------------------------------------------

    def insert_partner(self, partner: m.Partner) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO partners(partner_id, name, state, created_at,"
                " revoked_at, revoked_reason) VALUES(?,?,?,?,?,?)",
                (
                    partner.partner_id,
                    partner.name,
                    partner.state.value,
                    partner.created_at,
                    partner.revoked_at,
                    partner.revoked_reason,
                ),
            )
            for key in partner.keys.values():
                self._conn.execute(
                    "INSERT INTO api_keys(key_id, partner_id, secret_hash, created_at,"
                    " expires_at, revoked_at) VALUES(?,?,?,?,?,?)",
                    (
                        key.key_id,
                        partner.partner_id,
                        key.secret_hash,
                        key.created_at,
                        key.expires_at,
                        key.revoked_at,
                    ),
                )

    def _load_partner(self, row: sqlite3.Row) -> m.Partner:
        key_rows = self._conn.execute(
            "SELECT * FROM api_keys WHERE partner_id=?", (row["partner_id"],)
        ).fetchall()
        keys = {
            kr["key_id"]: m.ApiKey(
                key_id=kr["key_id"],
                secret_hash=kr["secret_hash"],
                created_at=kr["created_at"],
                expires_at=kr["expires_at"],
                revoked_at=kr["revoked_at"],
            )
            for kr in key_rows
        }
        return m.Partner(
            partner_id=row["partner_id"],
            name=row["name"],
            state=m.AuthState(row["state"]),
            keys=keys,
            created_at=row["created_at"],
            revoked_at=row["revoked_at"],
            revoked_reason=row["revoked_reason"],
        )

    def get_partner(self, partner_id: str) -> m.Partner | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM partners WHERE partner_id=?", (partner_id,)
            ).fetchone()
            return self._load_partner(row) if row else None

    def list_partners(self) -> list[m.Partner]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM partners ORDER BY created_at, partner_id"
            ).fetchall()
            return [self._load_partner(r) for r in rows]

    def update_partner_state(
        self,
        partner_id: str,
        state: m.AuthState,
        *,
        revoked_at: float | None = None,
        revoked_reason: str | None = None,
    ) -> None:
        with self._lock, self._conn:
            if state is m.AuthState.REVOKED:
                self._conn.execute(
                    "UPDATE partners SET state=?, revoked_at=?, revoked_reason=?"
                    " WHERE partner_id=?",
                    (state.value, revoked_at, revoked_reason, partner_id),
                )
            else:
                self._conn.execute(
                    "UPDATE partners SET state=? WHERE partner_id=?",
                    (state.value, partner_id),
                )

    def insert_api_key(self, partner_id: str, key: m.ApiKey) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO api_keys(key_id, partner_id, secret_hash, created_at,"
                " expires_at, revoked_at) VALUES(?,?,?,?,?,?)",
                (
                    key.key_id,
                    partner_id,
                    key.secret_hash,
                    key.created_at,
                    key.expires_at,
                    key.revoked_at,
                ),
            )

    def revoke_api_key(self, key_id: str, now: float) -> bool:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE api_keys SET revoked_at=? WHERE key_id=? AND revoked_at IS NULL",
                (now, key_id),
            )
            return cur.rowcount > 0

    # -- 订阅与规则 ----------------------------------------------------------

    def insert_subscription(self, sub: m.Subscription, now: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO subscriptions(subscription_id, partner_id, refresh_interval,"
                " max_batch_size, created_at, last_pull_at) VALUES(?,?,?,?,?,?)",
                (
                    sub.subscription_id,
                    sub.partner_id,
                    sub.refresh_interval,
                    sub.max_batch_size,
                    now,
                    None,
                ),
            )
            for rule in sub.rules:
                self._insert_rule_row(sub.subscription_id, rule)
            self._conn.execute(
                "INSERT INTO cursors(subscription_id, last_acked_seq, updated_at)"
                " VALUES(?,?,?)",
                (sub.subscription_id, 0, now),
            )

    def _insert_rule_row(self, subscription_id: str, rule: m.FilterRule) -> None:
        self._conn.execute(
            "INSERT INTO filter_rules(subscription_id, version, areas_json, fields_json,"
            " min_congestion, station_ids_json, changed_at, reason)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (
                subscription_id,
                rule.version,
                _dumps(rule.areas),
                _dumps(rule.fields),
                rule.min_congestion.value,
                _dumps(rule.station_ids),
                rule.changed_at,
                rule.reason,
            ),
        )

    def _load_rule(self, row: sqlite3.Row) -> m.FilterRule:
        areas = _loads_list(row["areas_json"])
        fields = _loads_list(row["fields_json"])
        station_ids = _loads_list(row["station_ids_json"])
        return m.FilterRule(
            version=row["version"],
            areas=frozenset(areas) if areas is not None else None,
            fields=frozenset(fields) if fields is not None else None,
            min_congestion=m.CongestionLevel(row["min_congestion"]),
            station_ids=frozenset(station_ids) if station_ids is not None else None,
            changed_at=row["changed_at"],
            reason=row["reason"],
        )

    def get_subscription(self, subscription_id: str) -> m.Subscription | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM subscriptions WHERE subscription_id=?",
                (subscription_id,),
            ).fetchone()
            if row is None:
                return None
            rule_rows = self._conn.execute(
                "SELECT * FROM filter_rules WHERE subscription_id=? ORDER BY version",
                (subscription_id,),
            ).fetchall()
        return m.Subscription(
            subscription_id=row["subscription_id"],
            partner_id=row["partner_id"],
            refresh_interval=row["refresh_interval"],
            max_batch_size=row["max_batch_size"],
            rules=[self._load_rule(r) for r in rule_rows],
        )

    def list_subscriptions(self, partner_id: str | None = None) -> list[m.Subscription]:
        with self._lock:
            if partner_id is None:
                rows = self._conn.execute(
                    "SELECT subscription_id FROM subscriptions ORDER BY created_at"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT subscription_id FROM subscriptions WHERE partner_id=?"
                    " ORDER BY created_at",
                    (partner_id,),
                ).fetchall()
        return [s for s in (self.get_subscription(r["subscription_id"]) for r in rows) if s]

    def insert_filter_rule(self, subscription_id: str, rule: m.FilterRule) -> None:
        with self._lock, self._conn:
            self._insert_rule_row(subscription_id, rule)

    # -- 游标 ---------------------------------------------------------------

    def get_cursor(self, subscription_id: str) -> m.Cursor:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM cursors WHERE subscription_id=?", (subscription_id,)
            ).fetchone()
        if row is None:
            return m.Cursor(subscription_id=subscription_id)
        return m.Cursor(
            subscription_id=subscription_id,
            last_acked_seq=row["last_acked_seq"],
            updated_at=row["updated_at"],
        )

    def advance_cursor(self, subscription_id: str, last_acked_seq: int, now: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE cursors SET last_acked_seq=?, updated_at=? WHERE subscription_id=?",
                (last_acked_seq, now, subscription_id),
            )

    def reset_cursor(self, subscription_id: str, last_acked_seq: int, now: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE cursors SET last_acked_seq=?, updated_at=? WHERE subscription_id=?",
                (last_acked_seq, now, subscription_id),
            )

    def get_last_pull(self, subscription_id: str) -> float | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT last_pull_at FROM subscriptions WHERE subscription_id=?",
                (subscription_id,),
            ).fetchone()
        return row["last_pull_at"] if row else None

    def set_last_pull(self, subscription_id: str, now: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE subscriptions SET last_pull_at=? WHERE subscription_id=?",
                (now, subscription_id),
            )

    # -- 发件箱 -------------------------------------------------------------

    def insert_batch(self, batch: m.DeliveryBatch) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO outbox(batch_id, subscription_id, delivery_attempt, from_seq,"
                " to_seq, rule_version, state, lease_owner, leased_at, lease_expires_at,"
                " created_at, priority, redacted) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    batch.batch_id,
                    batch.subscription_id,
                    batch.delivery_attempt,
                    batch.from_seq,
                    batch.to_seq,
                    batch.rule_version,
                    batch.state.value,
                    batch.lease_owner,
                    batch.leased_at,
                    batch.lease_expires_at,
                    batch.created_at,
                    batch.priority,
                    1 if batch.redacted else 0,
                ),
            )
            for pos, item in enumerate(batch.items):
                self._conn.execute(
                    "INSERT INTO batch_items(batch_id, position, event_seq, event_id,"
                    " projection_json, fingerprint) VALUES(?,?,?,?,?,?)",
                    (
                        batch.batch_id,
                        pos,
                        item.event.seq,
                        item.event.event_id,
                        json.dumps(dict(item.projection), ensure_ascii=False),
                        item.billing_fingerprint,
                    ),
                )

    def _load_batch(self, brow: sqlite3.Row) -> m.DeliveryBatch:
        item_rows = self._conn.execute(
            "SELECT bi.*, e.event_id AS e_event_id, e.station_id, e.area, e.status,"
            " e.congestion, e.occurred_at, e.ingested_at, e.payload_json"
            " FROM batch_items bi JOIN events e ON e.seq = bi.event_seq"
            " WHERE bi.batch_id=? ORDER BY bi.position",
            (brow["batch_id"],),
        ).fetchall()
        items: list[m.BatchItem] = []
        for ir in item_rows:
            event = m.StationEvent(
                seq=ir["event_seq"],
                event_id=ir["e_event_id"],
                station_id=ir["station_id"],
                area=ir["area"],
                status=m.StationStatus(ir["status"]),
                payload=json.loads(ir["payload_json"]),
                congestion=m.CongestionLevel(ir["congestion"]),
                occurred_at=ir["occurred_at"],
                ingested_at=ir["ingested_at"],
            )
            items.append(
                m.BatchItem(
                    event=event,
                    projection=json.loads(ir["projection_json"]),
                    billing_fingerprint=ir["fingerprint"],
                )
            )
        return m.DeliveryBatch(
            batch_id=brow["batch_id"],
            subscription_id=brow["subscription_id"],
            delivery_attempt=brow["delivery_attempt"],
            from_seq=brow["from_seq"],
            to_seq=brow["to_seq"],
            items=tuple(items),
            rule_version=brow["rule_version"],
            lease_owner=brow["lease_owner"],
            leased_at=brow["leased_at"],
            lease_expires_at=brow["lease_expires_at"],
            state=m.LeaseState(brow["state"]),
            created_at=brow["created_at"],
            priority=brow["priority"],
            redacted=bool(brow["redacted"]),
        )

    def get_batch(self, batch_id: str) -> m.DeliveryBatch | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM outbox WHERE batch_id=?", (batch_id,)
            ).fetchone()
            return self._load_batch(row) if row else None

    def outstanding_batch(self, subscription_id: str) -> m.DeliveryBatch | None:
        """取该订阅唯一的未确认存活批次（PENDING/DELIVERED/EXPIRED）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM outbox WHERE subscription_id=? AND state IN"
                " ('pending','delivered','expired') ORDER BY created_at DESC LIMIT 1",
                (subscription_id,),
            ).fetchone()
            return self._load_batch(row) if row else None

    def update_batch_lease(
        self,
        batch_id: str,
        *,
        state: m.LeaseState,
        attempt: int | None = None,
        owner: str | None,
        leased_at: float | None,
        expires_at: float | None,
    ) -> None:
        with self._lock, self._conn:
            if attempt is None:
                self._conn.execute(
                    "UPDATE outbox SET state=?, lease_owner=?, leased_at=?,"
                    " lease_expires_at=? WHERE batch_id=?",
                    (state.value, owner, leased_at, expires_at, batch_id),
                )
            else:
                self._conn.execute(
                    "UPDATE outbox SET state=?, delivery_attempt=?, lease_owner=?,"
                    " leased_at=?, lease_expires_at=? WHERE batch_id=?",
                    (state.value, attempt, owner, leased_at, expires_at, batch_id),
                )

    def mark_batch_state(self, batch_id: str, state: m.LeaseState) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE outbox SET state=? WHERE batch_id=?", (state.value, batch_id)
            )

    def ack_batch_atomic(
        self, batch_id: str, lease_owner: str, now: float
    ) -> int | None:
        """原子确认：仅 delivered 且租约所有者匹配时成功。

        同事务内将游标单调推进到批次 to_seq。
        成功返回推进后的 last_acked_seq；并发竞争失败者返回 None。
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT subscription_id, to_seq FROM outbox WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
            if row is None:
                return None
            cur = self._conn.execute(
                "UPDATE outbox SET state='acked' WHERE batch_id=?"
                " AND state='delivered' AND lease_owner=?",
                (batch_id, lease_owner),
            )
            if cur.rowcount == 0:
                return None
            self._conn.execute(
                "UPDATE cursors SET last_acked_seq=?, updated_at=?"
                " WHERE subscription_id=? AND last_acked_seq<?",
                (row["to_seq"], now, row["subscription_id"], row["to_seq"]),
            )
            return int(row["to_seq"])

    def expire_leases(self, now: float) -> list[str]:
        """租约超时回收。返回被回收的 batch_id。"""
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT batch_id FROM outbox WHERE state='delivered'"
                " AND lease_expires_at IS NOT NULL AND lease_expires_at<?",
                (now,),
            ).fetchall()
            ids = [r["batch_id"] for r in rows]
            if ids:
                self._conn.execute(
                    "UPDATE outbox SET state='expired', lease_owner=NULL,"
                    " leased_at=NULL, lease_expires_at=NULL WHERE batch_id IN (%s)"
                    % ",".join("?" * len(ids)),
                    ids,
                )
        return ids

    def supersede_unacked(self, subscription_id: str) -> int:
        """回放：将未确认批次标记为 superseded，不再投递。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE outbox SET state='superseded', lease_owner=NULL,"
                " leased_at=NULL, lease_expires_at=NULL"
                " WHERE subscription_id=? AND state IN ('pending','delivered','expired')",
                (subscription_id,),
            )
            return cur.rowcount

    def revoke_live_batches(self, partner_id: str) -> int:
        """授权撤销：该合作方所有存活批次立即停止投递。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE outbox SET state='revoked', lease_owner=NULL,"
                " leased_at=NULL, lease_expires_at=NULL"
                " WHERE state IN ('pending','delivered','expired')"
                " AND subscription_id IN"
                " (SELECT subscription_id FROM subscriptions WHERE partner_id=?)",
                (partner_id,),
            )
            return cur.rowcount

    def redact_outbox_for_partner(self, partner_id: str) -> int:
        """授权撤销：抹掉该合作方全部历史批次中的敏感字段（含已确认批次）。"""
        updated = 0
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT bi.batch_id, bi.position, bi.projection_json"
                " FROM batch_items bi"
                " JOIN outbox o ON o.batch_id = bi.batch_id"
                " JOIN subscriptions s ON s.subscription_id = o.subscription_id"
                " WHERE s.partner_id=?",
                (partner_id,),
            ).fetchall()
            for r in rows:
                proj = json.loads(r["projection_json"])
                changed = False
                for name in m.SENSITIVE_FIELDS:
                    if name in proj and proj[name] is not None:
                        proj[name] = None
                        changed = True
                if changed:
                    self._conn.execute(
                        "UPDATE batch_items SET projection_json=? WHERE batch_id=? AND position=?",
                        (json.dumps(proj, ensure_ascii=False), r["batch_id"], r["position"]),
                    )
                    updated += 1
            self._conn.execute(
                "UPDATE outbox SET redacted=1 WHERE subscription_id IN"
                " (SELECT subscription_id FROM subscriptions WHERE partner_id=?)",
                (partner_id,),
            )
        return updated

    def list_batches(
        self, subscription_id: str | None = None, state: m.LeaseState | None = None
    ) -> list[m.DeliveryBatch]:
        with self._lock:
            sql = "SELECT * FROM outbox"
            conds: list[str] = []
            params: list[Any] = []
            if subscription_id is not None:
                conds.append("subscription_id=?")
                params.append(subscription_id)
            if state is not None:
                conds.append("state=?")
                params.append(state.value)
            if conds:
                sql += " WHERE " + " AND ".join(conds)
            sql += " ORDER BY created_at, batch_id"
            rows = self._conn.execute(sql, params).fetchall()
            return [self._load_batch(r) for r in rows]

    def delivery_queue(self) -> list[m.DeliveryBatch]:
        """待投递队列：高拥堵优先，其次按生成时间（FIFO）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM outbox WHERE state IN ('pending','expired')"
                " ORDER BY priority DESC, created_at ASC"
            ).fetchall()
            return [self._load_batch(r) for r in rows]

    # -- 计费 ---------------------------------------------------------------

    def record_billing(
        self,
        entries: Iterable[tuple[str, str, str, str, float]],
    ) -> int:
        """记账 (fingerprint, subscription_id, event_id, batch_id, at)。

        依赖 fingerprint 主键去重，返回新增条数；重复投递/回放不重复计费。
        """
        inserted = 0
        with self._lock, self._conn:
            for fingerprint, sub_id, event_id, batch_id, at in entries:
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO billing(fingerprint, subscription_id,"
                    " event_id, batch_id, charged_at) VALUES(?,?,?,?,?)",
                    (fingerprint, sub_id, event_id, batch_id, at),
                )
                inserted += cur.rowcount
        return inserted

    def list_billing(self, subscription_id: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if subscription_id is None:
                rows = self._conn.execute(
                    "SELECT * FROM billing ORDER BY charged_at, fingerprint"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM billing WHERE subscription_id=? ORDER BY charged_at",
                    (subscription_id,),
                ).fetchall()
        return [dict(r) for r in rows]

    def billing_count(self, subscription_id: str | None = None) -> int:
        with self._lock:
            if subscription_id is None:
                row = self._conn.execute("SELECT COUNT(*) AS c FROM billing").fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM billing WHERE subscription_id=?",
                    (subscription_id,),
                ).fetchone()
        return int(row["c"])

    # -- 审计 ---------------------------------------------------------------

    def append_audit(self, entry: m.AuditEntry) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO audit_log(at, action, partner_id, subscription_id, actor,"
                " detail_json) VALUES(?,?,?,?,?,?)",
                (
                    entry.at,
                    entry.action.value,
                    entry.partner_id,
                    entry.subscription_id,
                    entry.actor,
                    json.dumps(dict(entry.detail), ensure_ascii=False),
                ),
            )
            return int(cur.lastrowid)

    def list_audit(
        self,
        *,
        subscription_id: str | None = None,
        partner_id: str | None = None,
        action: m.AuditAction | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            sql = "SELECT * FROM audit_log"
            conds: list[str] = []
            params: list[Any] = []
            if subscription_id is not None:
                conds.append("subscription_id=?")
                params.append(subscription_id)
            if partner_id is not None:
                conds.append("partner_id=?")
                params.append(partner_id)
            if action is not None:
                conds.append("action=?")
                params.append(action.value)
            if conds:
                sql += " WHERE " + " AND ".join(conds)
            sql += " ORDER BY audit_id ASC"
            if limit is not None:
                sql += f" LIMIT {int(limit)}"
            rows = self._conn.execute(sql, params).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "audit_id": r["audit_id"],
                    "at": r["at"],
                    "action": r["action"],
                    "partner_id": r["partner_id"],
                    "subscription_id": r["subscription_id"],
                    "actor": r["actor"],
                    "detail": json.loads(r["detail_json"]),
                }
            )
        return out
